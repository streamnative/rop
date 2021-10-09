/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streamnative.pulsar.handlers.rocketmq.inner.proxy;

import static org.apache.bookkeeper.util.ZkUtils.createFullPathOptimistic;
import static org.apache.bookkeeper.util.ZkUtils.deleteFullPathOptimistic;
import static org.apache.pulsar.broker.web.PulsarWebResource.joinPath;
import static org.apache.rocketmq.common.protocol.RequestCode.CONSUMER_SEND_MSG_BACK;
import static org.apache.rocketmq.common.protocol.RequestCode.PULL_MESSAGE;
import static org.apache.rocketmq.common.protocol.RequestCode.QUERY_MESSAGE;
import static org.apache.rocketmq.common.protocol.RequestCode.SEND_BATCH_MESSAGE;
import static org.apache.rocketmq.common.protocol.RequestCode.SEND_MESSAGE;
import static org.apache.rocketmq.common.protocol.RequestCode.SEND_MESSAGE_V2;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.BROKER_CLUSTER_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.COLO_CHAR;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.PULSAR_REAL_PARTITION_ID_TAG;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_CACHE_EXPIRE_TIME_MS;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_CACHE_INITIAL_SIZE;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_CACHE_MAX_SIZE;
import static org.streamnative.pulsar.handlers.rocketmq.utils.PulsarUtil.autoExpanseBrokerGroupData;
import static org.streamnative.pulsar.handlers.rocketmq.utils.PulsarUtil.genBrokerGroupData;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQServiceConfiguration;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQRemoteServer;
import org.streamnative.pulsar.handlers.rocketmq.inner.coordinator.RopCoordinator;
import org.streamnative.pulsar.handlers.rocketmq.inner.namesvr.MQTopicManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.namesvr.NameserverProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.AdminBrokerProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.ClientManageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.ConsumerManageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.EndTransactionProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.PullMessageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.QueryMessageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.SendMessageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopClusterContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Rop broker proxy is a rocketmq request simulator
 * find the real broker that topicPartition is stored on
 * and transfer the request to the owner broke.
 */
@Slf4j
public class RopBrokerProxy extends RocketMQRemoteServer implements AutoCloseable {

    private static final int ROP_SERVICE_PORT = 9876;
    private static final int INTERNAL_REDIRECT_TIMEOUT_MS = 3000;

    private final RocketMQBrokerController brokerController;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    @Getter
    private RopZookeeperCacheService zkService;
    private RopCoordinator coordinator;
    private PulsarService pulsarService;
    private final OrderedExecutor orderedExecutor;
    private final List<ProcessorProxyRegister> processorProxyRegisters = new ArrayList<>();
    private final BrokerNetworkAPI brokerNetworkClients = new BrokerNetworkAPI(this);
    private volatile String brokerTag = Strings.EMPTY;
    private final String clusterName;
    @Getter
    private final MQTopicManager mqTopicManager;
    private final ThreadLocal<RemotingCommand> sendResponseThreadLocal = ThreadLocal
            .withInitial(() -> RemotingCommand.createResponseCommand(SendMessageResponseHeader.class));
    private final ThreadLocal<RemotingCommand> consumeResponseThreadLocal = ThreadLocal
            .withInitial(() -> RemotingCommand.createResponseCommand(PullMessageResponseHeader.class));
    private final ThreadLocal<PulsarClientImpl> pulsarClientThreadLocal = new ThreadLocal<>();
    private final Cache<TopicName, String> ownedBrokerCache = CacheBuilder.newBuilder()
            .initialCapacity(ROP_CACHE_INITIAL_SIZE).maximumSize(ROP_CACHE_MAX_SIZE)
            .expireAfterAccess(ROP_CACHE_EXPIRE_TIME_MS, TimeUnit.MILLISECONDS).build();
    @Getter
    private PullMessageProcessor pullMessageProcessor;

    public RopBrokerProxy(final RocketMQServiceConfiguration config, RocketMQBrokerController brokerController,
            final ChannelEventListener channelEventListener) {
        super(config, channelEventListener);
        this.clusterName = config.getClusterName();
        this.brokerController = brokerController;
        this.orderedExecutor = OrderedExecutor.newBuilder().numThreads(4).name("rop-ordered-executor").build();
        this.mqTopicManager = new MQTopicManager(brokerController);
    }

    private boolean checkTopicOwnerBroker(RemotingCommand cmd, TopicName pulsarTopicName, int queueId) {
        int pulsarTopicPartitionId = getPulsarTopicPartitionId(pulsarTopicName, queueId);
        boolean isOwner = mqTopicManager.isPartitionTopicOwner(pulsarTopicName, pulsarTopicPartitionId);
        cmd.addExtField(PULSAR_REAL_PARTITION_ID_TAG, String.valueOf(pulsarTopicPartitionId));
        return isOwner;
    }

    public int getPulsarTopicPartitionId(TopicName pulsarTopicName, int queueId) {
        Map<String, List<Integer>> pulsarTopicRoute = mqTopicManager
                .getPulsarTopicRoute(pulsarTopicName, Strings.EMPTY);
        Preconditions.checkArgument(pulsarTopicRoute != null && !pulsarTopicRoute.isEmpty());
        List<Integer> pulsarPartitionIdList = pulsarTopicRoute.get(this.brokerTag);
        Preconditions.checkArgument(pulsarPartitionIdList != null
                && !pulsarPartitionIdList.isEmpty()
                && queueId < pulsarPartitionIdList.size());
        return pulsarPartitionIdList.get(queueId);
    }

    @Override
    public void processRequestCommand(ChannelHandlerContext ctx, RemotingCommand cmd) throws RemotingCommandException {
        switch (cmd.getCode()) {
            case PULL_MESSAGE:
                RemotingCommand pullResponse = sendResponseThreadLocal.get();
                pullResponse.setCode(-1);
                if (pullResponse.getCode() != -1) {
                    //fail to msgCheck and flush error at once;
                    ctx.writeAndFlush(pullResponse);
                } else {
                    PullMessageRequestHeader pullMsgHeader =
                            (PullMessageRequestHeader) cmd.decodeCommandCustomHeader(PullMessageRequestHeader.class);
                    RocketMQTopic rmqTopic = new RocketMQTopic(pullMsgHeader.getTopic());
                    TopicName pulsarTopicName = rmqTopic.getPulsarTopicName();
                    boolean isOwnedBroker = checkTopicOwnerBroker(cmd, rmqTopic.getPulsarTopicName(),
                            pullMsgHeader.getQueueId());
                    if (isOwnedBroker) {
                        super.processRequestCommand(ctx, cmd);
                    } else {
                        processNonOwnedBrokerPullRequest(ctx, cmd, pulsarTopicName, INTERNAL_REDIRECT_TIMEOUT_MS);
                    }
                }
                break;
            case SEND_MESSAGE:
            case SEND_MESSAGE_V2:
            case SEND_BATCH_MESSAGE:
                RemotingCommand sendResponse = sendResponseThreadLocal.get();
                sendResponse.setCode(-1);
                if (sendResponse.getCode() != -1) {
                    //fail to msgCheck and flush error at once;
                    ctx.writeAndFlush(sendResponse);
                } else {
                    SendMessageRequestHeader sendHeader = SendMessageProcessor.parseRequestHeader(cmd);
                    SendMessageProcessor
                            .msgCheck(brokerController.getServerConfig(), mqTopicManager, ctx, sendHeader,
                                    sendResponse);
                    RocketMQTopic rmqTopic = new RocketMQTopic(sendHeader.getTopic());
                    TopicName pulsarTopicName = rmqTopic.getPulsarTopicName();
                    boolean isOwnedBroker = checkTopicOwnerBroker(cmd, pulsarTopicName,
                            sendHeader.getQueueId());
                    if (isOwnedBroker) {
                        super.processRequestCommand(ctx, cmd);
                    } else {
                        processNonOwnedBrokerSendRequest(ctx, cmd, pulsarTopicName, INTERNAL_REDIRECT_TIMEOUT_MS);
                    }
                }
                break;
            case QUERY_MESSAGE:
                // TODO: not to support in the version
            case CONSUMER_SEND_MSG_BACK: //TODO: CommitLogOffset 0
                break;
            default:
                super.processRequestCommand(ctx, cmd);
                break;
        }

    }

    private void processNonOwnedBrokerSendRequest(ChannelHandlerContext ctx, RemotingCommand cmd,
            TopicName pulsarTopicName, long timeout) {
        int pulsarPartitionId = Integer.parseInt(cmd.getExtFields().get(PULSAR_REAL_PARTITION_ID_TAG));
        TopicName partitionedTopicName = pulsarTopicName.getPartition(pulsarPartitionId);
        String address = lookupPulsarTopicBroker(partitionedTopicName);
        try {
            long timeoutTime = System.currentTimeMillis() + timeout;
            brokerNetworkClients.invokeAsync(address, cmd, timeout, (responseFuture) -> {
                RemotingCommand sendResponse = responseFuture.getResponseCommand();
                if (sendResponse != null) {
                    if (sendResponse.getCode() == ResponseCode.SUCCESS) {
                        ctx.writeAndFlush(sendResponse);
                    } else {
                        //maybe partitioned topic have transfer to other broker, invalidate cache at once.
                        ownedBrokerCache.invalidate(partitionedTopicName);
                        log.info("processNonOwnedBrokerSendRequest failed and retry, {} {}", sendResponse.getCode(),
                                sendResponse.getRemark());
                        long curTime = System.currentTimeMillis();
                        if (curTime < timeoutTime) {
                            processNonOwnedBrokerSendRequest(ctx, cmd, pulsarTopicName,
                                    timeoutTime - curTime);
                        } else {
                            ctx.writeAndFlush(sendResponse);
                        }
                    }
                } else {
                    log.warn("getSendResponseCommand return null");
                    sendResponse = sendResponseThreadLocal.get();
                    sendResponse.setCode(ResponseCode.SYSTEM_ERROR);
                    sendResponse.setRemark("getSendResponseCommand return null");
                    ctx.writeAndFlush(sendResponse);
                }
            });
        } catch (Exception e) {
            log.warn("BrokerNetworkAPI invokeAsync error.", e);
            RemotingCommand sendResponse = sendResponseThreadLocal.get();
            sendResponse.setCode(ResponseCode.SYSTEM_ERROR);
            sendResponse.setRemark("BrokerNetworkAPI invokeAsync error");
            ctx.writeAndFlush(sendResponse);
        }
    }

    private void processNonOwnedBrokerPullRequest(ChannelHandlerContext ctx, RemotingCommand cmd,
            TopicName pulsarTopicName, long timeout) {
        int pulsarPartitionId = Integer.parseInt(cmd.getExtFields().get(PULSAR_REAL_PARTITION_ID_TAG));
        TopicName partitionedTopicName = pulsarTopicName.getPartition(pulsarPartitionId);
        String address = lookupPulsarTopicBroker(partitionedTopicName);
        try {
            long timeoutTime = System.currentTimeMillis() + timeout;
            brokerNetworkClients.invokeAsync(address, cmd, timeout, (responseFuture) -> {
                RemotingCommand pullResponse = responseFuture.getResponseCommand();
                if (pullResponse != null) {
                    if (pullResponse.getCode() == ResponseCode.SUCCESS) {
                        ctx.writeAndFlush(pullResponse);
                    } else {
                        //maybe partitioned topic have transfer to other broker, invalidate cache at once.
                        ownedBrokerCache.invalidate(partitionedTopicName);
                        log.info("processNonOwnedBrokerPullRequest failed and retry, {} {}", pullResponse.getCode(),
                                pullResponse.getRemark());
                        long curTime = System.currentTimeMillis();
                        if (curTime < timeoutTime) {
                            processNonOwnedBrokerSendRequest(ctx, cmd, pulsarTopicName,
                                    timeoutTime - curTime);
                        } else {
                            ctx.writeAndFlush(pullResponse);
                        }
                    }
                } else {
                    log.warn("getPullResponseCommand return null");
                    pullResponse = consumeResponseThreadLocal.get();
                    pullResponse.setCode(ResponseCode.SYSTEM_ERROR);
                    pullResponse.setRemark("getPullResponseCommand return null");
                    ctx.writeAndFlush(pullResponse);
                }
            });
        } catch (Exception e) {
            log.warn("BrokerNetworkAPI invokeAsync error.", e);
            RemotingCommand consumeResponse = consumeResponseThreadLocal.get();
            consumeResponse.setCode(ResponseCode.SYSTEM_ERROR);
            consumeResponse.setRemark("BrokerNetworkAPI invokeAsync error");
            ctx.writeAndFlush(consumeResponse);
        }
    }

    @Override
    public void start() {
        super.start();
        try {
            this.pulsarService = brokerController.getBrokerService().pulsar();
            ServiceConfiguration config = this.pulsarService.getConfig();
            RopZookeeperCache ropZkCache = new RopZookeeperCache(pulsarService.getZkClientFactory(),
                    (int) config.getZooKeeperSessionTimeoutMillis(),
                    config.getZooKeeperOperationTimeoutSeconds(), config.getZookeeperServers(), orderedExecutor,
                    brokerController.getScheduledExecutorService(), config.getZooKeeperCacheExpirySeconds());
            this.zkService = new RopZookeeperCacheService(ropZkCache);
            this.zkService.start();

            initClusterMeta();
            setBrokerTagListener();

            this.coordinator = new RopCoordinator(brokerController, zkService);
            this.coordinator.start();

            this.mqTopicManager.start(zkService);
        } catch (Exception e) {
            log.error("RopBrokerProxy fail to start.", e);
            throw new RuntimeException("RopBrokerProxy not running.");
        }
    }

    private void initClusterMeta() throws Exception {
        RopClusterContent clusterContent = zkService.getClusterContent();
        List<String> activeBrokers = getActiveBrokers();
        int ropBrokerReplicationNum = getConfig().getRopBrokerReplicationNum();
        Preconditions.checkArgument(ropBrokerReplicationNum > 0);
        if (clusterContent == null) {
            //initialize RoP cluster metadata
            RopClusterContent defaultClusterContent = new RopClusterContent();
            defaultClusterContent.setClusterName(clusterName);
            log.info("RoP cluster[{}] broker list: {}.", defaultClusterContent.getClusterName(),
                    activeBrokers.toString());
            defaultClusterContent
                    .setBrokerCluster(
                            genBrokerGroupData(activeBrokers, ropBrokerReplicationNum));
            zkService.setJsonObjectForPath(BROKER_CLUSTER_PATH, defaultClusterContent);
            zkService.getClusterDataCache().reloadCache(BROKER_CLUSTER_PATH);
        } else if (getConfig().isAutoCreateRopClusterMeta()) {
            if (autoExpanseBrokerGroupData(clusterContent, activeBrokers, ropBrokerReplicationNum)) {
                zkService.setJsonObjectForPath(BROKER_CLUSTER_PATH, clusterContent);
                zkService.getClusterDataCache().reloadCache(BROKER_CLUSTER_PATH);
            }
        }
        log.info("RoP cluster metadata is: [{}].", zkService.getClusterContent());
    }

    public RopClusterContent getRopClusterContent() {
        try {
            return zkService.getClusterContent();
        } catch (Exception e) {
            log.error("RoP cluster metadata is missing, service can't run correctly.");
            return null;
        }
    }

    private String setBrokerTagListener() {
        String brokerHost = brokerController.getBrokerHost();
        RopClusterContent clusterContent = zkService.getClusterContent();
        for (Entry<String, List<String>> entry : clusterContent.getBrokerCluster().entrySet()) {
            if (entry.getValue().contains(brokerHost)) {
                this.brokerTag = entry.getKey();
            }
        }
        zkService.getClusterDataCache().registerListener((path, data, stat) -> {
            if (BROKER_CLUSTER_PATH.equals(path)) {
                log.info("the cluster[{}] configure have changed, new configure: [{}].",
                        clusterContent.getClusterName(), data);
                String host = brokerController.getBrokerHost();
                for (Entry<String, List<String>> entry : data.getBrokerCluster().entrySet()) {
                    if (entry.getValue().contains(host)) {
                        brokerTag = entry.getKey();
                    }
                }
            }
        });
        if (this.brokerTag.equals(Strings.EMPTY)) {
            log.warn("host[{}] isn't belong to current cluster[{}].", brokerHost, getConfig().getClusterName());
        }
        return brokerTag;
    }

    @Override
    public void close() throws Exception {
        this.coordinator.close();
        this.zkService.close();
        this.brokerNetworkClients.close();
        this.mqTopicManager.shutdown();
    }

    // such as: /rop/brokers/{xxx,}
    private void registerBrokerZNode() {
        String hostName = this.brokerController.getBrokerHost();
        String brokerPathRoot = RopZkUtils.BROKERS_PATH;
        String localAddressPath = joinPath(brokerPathRoot, hostName);
        this.zkService.getBrokerCache()
                .getAsync(localAddressPath)
                .thenApply(brokerInfo -> {
                    try {
                        if (brokerInfo.isPresent()) {
                            log.info("broker[{}] is already exists, delete it first.",
                                    hostName);
                            deleteFullPathOptimistic(zkService.getCache().getZooKeeper(), localAddressPath,
                                    -1);
                        }

                        createFullPathOptimistic(zkService.getCache().getZooKeeper(),
                                localAddressPath,
                                hostName.getBytes(StandardCharsets.UTF_8),
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.EPHEMERAL);
                        zkService.getBrokerCache().reloadCache(localAddressPath);
                        log.info("broker address ===========>[{}].",
                                zkService.getBrokerCache().getDataIfPresent(localAddressPath));
                    } catch (KeeperException | InterruptedException e) {
                        log.warn("broker[{}] is already exists.", hostName, e);
                    }
                    return null;
                });
    }

    public void registerProcessor() {
        // SendMessageProcessor
        processorProxyRegisters.add(new SendMessageProcessorProxy(brokerController.getSendMessageExecutor()));

        // PullMessageProcessor
        processorProxyRegisters.add(new PullMessageProcessorProxy(brokerController.getPullMessageExecutor()));

        // QueryMessageProcessor
        processorProxyRegisters.add(new QueryMessageProcessorProxy(brokerController.getQueryMessageExecutor()));

        // ClientManageProcessor
        processorProxyRegisters.add(new ClientManageProcessorProxy(brokerController.getHeartbeatExecutor()));

        // ConsumerManageProcessor
        processorProxyRegisters
                .add(new ConsumerManageProcessorProxy(brokerController.getConsumerManageExecutor()));

        // EndTransactionProcessor
        processorProxyRegisters
                .add(new EndTransactionProcessorProxy(brokerController.getEndTransactionExecutor()));

        // NameserverProcessor
        processorProxyRegisters.add(new NameserverProcessorProxy(brokerController.getAdminBrokerExecutor()));

        // Default
        processorProxyRegisters.add(new AdminBrokerProcessorProxy(brokerController.getAdminBrokerExecutor()));

        //register all processors to remoteServer
        processorProxyRegisters.forEach(ProcessorProxyRegister::registerProxyProcessor);
    }

    /**
     * Processor Proxy Register.
     */
    protected interface ProcessorProxyRegister {

        /**
         * register Proxy Processor.
         *
         * @return boolean
         */
        boolean registerProxyProcessor();
    }

    /**
     * Admin Broker Processor Proxy.
     */
    protected class AdminBrokerProcessorProxy extends AdminBrokerProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public AdminBrokerProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this.registerDefaultProcessor(this, processorExecutor);
            return true;
        }

        /**
         * process Request.
         */
        @Override
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
                throws RemotingCommandException {
            //TODO
            return super.processRequest(ctx, request);
        }
    }

    /**
     * Nameserver Processor Proxy.
     */
    protected class NameserverProcessorProxy extends NameserverProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public NameserverProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this.registerProcessor(RequestCode.PUT_KV_CONFIG, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_KV_CONFIG, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.DELETE_KV_CONFIG, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.QUERY_DATA_VERSION, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.REGISTER_BROKER, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.UNREGISTER_BROKER, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_ROUTEINTO_BY_TOPIC, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_BROKER_CLUSTER_INFO, this, processorExecutor);
            RopBrokerProxy.this
                    .registerProcessor(RequestCode.WIPE_WRITE_PERM_OF_BROKER, this, processorExecutor);
            RopBrokerProxy.this
                    .registerProcessor(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.DELETE_TOPIC_IN_NAMESRV, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_KVLIST_BY_NAMESPACE, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_TOPICS_BY_CLUSTER, this, processorExecutor);
            RopBrokerProxy.this
                    .registerProcessor(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_UNIT_TOPIC_LIST, this, processorExecutor);
            RopBrokerProxy.this
                    .registerProcessor(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, this, processorExecutor);
            RopBrokerProxy.this
                    .registerProcessor(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.UPDATE_NAMESRV_CONFIG, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_NAMESRV_CONFIG, this, processorExecutor);
            return true;
        }
    }

    /**
     * Consumer Manage Processor Proxy.
     */
    protected class ConsumerManageProcessorProxy extends ConsumerManageProcessor implements
            ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public ConsumerManageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this
                    .registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, this, processorExecutor);
            return true;
        }
    }

    /**
     * Client Manage Processor Proxy.
     */
    protected class ClientManageProcessorProxy extends ClientManageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public ClientManageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this.registerProcessor(RequestCode.HEART_BEAT, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.UNREGISTER_CLIENT, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, this, processorExecutor);
            return true;
        }
    }

    /**
     * Query Message Processor Proxy.
     */
    protected class QueryMessageProcessorProxy extends QueryMessageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public QueryMessageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this.registerProcessor(RequestCode.QUERY_MESSAGE, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, this, processorExecutor);
            return true;
        }
    }

    /**
     * Pull Message Processor Proxy.
     */
    protected class PullMessageProcessorProxy extends PullMessageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public PullMessageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
                throws RemotingCommandException {
            return super.processRequest(ctx, request);
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this.registerProcessor(PULL_MESSAGE, this, processorExecutor);
            RopBrokerProxy.this.pullMessageProcessor = this;
            return true;
        }
    }

    /**
     * Send Message Processor Proxy.
     */
    protected class SendMessageProcessorProxy extends SendMessageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public SendMessageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
            registerSendMessageHook(sendMessageHookList);
            registerConsumeMessageHook(consumeMessageHookList);
        }

        @Override
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
                throws RemotingCommandException {
            return super.processRequest(ctx, request);
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this.registerProcessor(SEND_MESSAGE, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(SEND_MESSAGE_V2, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(SEND_BATCH_MESSAGE, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(CONSUMER_SEND_MSG_BACK, this, processorExecutor);
            return true;
        }
    }

    /**
     * End Transaction Processor Proxy.
     */
    protected class EndTransactionProcessorProxy extends EndTransactionProcessor implements
            ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public EndTransactionProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this.registerProcessor(RequestCode.END_TRANSACTION, this, processorExecutor);
            return true;
        }
    }

    public List<String> getActiveBrokers() {
        ModularLoadManagerImpl loadManager = (ModularLoadManagerImpl) ((ModularLoadManagerWrapper) pulsarService
                .getLoadManager().get()).getLoadManager();
        return loadManager.getAvailableBrokers().stream()
                .map(broker -> Splitter.on(COLO_CHAR).splitToList(broker).get(0)).collect(
                        Collectors.toList());
    }

    private PulsarClientImpl initPulsarClient(String listenerName) {
        try {
            ClientBuilder builder =
                    PulsarClient.builder().serviceUrl(pulsarService.getBrokerServiceUrl());
            if (StringUtils.isNotBlank(getConfig().getBrokerClientAuthenticationPlugin())) {
                builder.authentication(
                        getConfig().getBrokerClientAuthenticationPlugin(),
                        getConfig().getBrokerClientAuthenticationParameters()
                );
            }
            if (StringUtils.isNotBlank(listenerName)) {
                builder.listenerName(listenerName);
            }
            return (PulsarClientImpl) builder.build();
        } catch (Exception e) {
            log.error("listenerName [{}] getClient error", listenerName, e);
            return null;
        }
    }

    public PulsarClientImpl getPulsarClient() {
        PulsarClientImpl pulsarClient = pulsarClientThreadLocal.get();
        if (pulsarClient == null || pulsarClient.isClosed()) {
            pulsarClientThreadLocal.remove();
            pulsarClient = initPulsarClient(null);
            pulsarClientThreadLocal.set(pulsarClient);
        }
        return pulsarClient;
    }

    public String lookupPulsarTopicBroker(TopicName pulsarTopicName) {
        try {
            String ropBrokerAddr = ownedBrokerCache.getIfPresent(pulsarTopicName);
            if (Strings.isBlank(ropBrokerAddr)) {
                InetSocketAddress pulsarBrokerAddr = getPulsarClient().getLookup()
                        .getBroker(pulsarTopicName)
                        .get()
                        .getLeft();
                ropBrokerAddr = Joiner.on(COLO_CHAR).join(pulsarBrokerAddr.getHostName(), ROP_SERVICE_PORT);
                ownedBrokerCache.put(pulsarTopicName, ropBrokerAddr);
            }
            return ropBrokerAddr;
        } catch (Exception e) {
            log.error("LookupTopics pulsar topic=[{}] error.", pulsarTopicName, e);
        }
        return null;
    }

}
