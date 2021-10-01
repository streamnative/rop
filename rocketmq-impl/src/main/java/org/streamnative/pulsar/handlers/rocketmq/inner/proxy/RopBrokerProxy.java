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

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.zookeeper.CreateMode;
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

    private final RocketMQBrokerController brokerController;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    @Getter
    private RopZookeeperCacheService zkService;
    private RopCoordinator coordinator;
    private PulsarService pulsarService;
    private final OrderedExecutor orderedExecutor;
    private List<ProcessorProxyRegister> processorProxyRegisters = new ArrayList<>();
    private final BrokerNetworkAPI brokerNetworkClients = new BrokerNetworkAPI(this);
    private final String brokerPathRoot = RopZkUtils.BROKERS_PATH;
    private volatile String brokerTag = Strings.EMPTY;
    @Getter
    private final MQTopicManager mqTopicManager;
    private final ThreadLocal<RemotingCommand> sendResponseThreadLocal = ThreadLocal
            .withInitial(() -> RemotingCommand.createResponseCommand(SendMessageResponseHeader.class));

    public RopBrokerProxy(final RocketMQServiceConfiguration config, RocketMQBrokerController brokerController,
            final ChannelEventListener channelEventListener) {
        super(config, channelEventListener);
        this.brokerController = brokerController;
        this.orderedExecutor = OrderedExecutor.newBuilder().numThreads(4).name("rop-ordered-executor").build();
        this.mqTopicManager = new MQTopicManager(brokerController);
    }

    private boolean checkTopicOwnerBroker(String topic, int queueId) {
        RocketMQTopic rmqTopic = new RocketMQTopic(topic);
        Map<String, List<Integer>> pulsarTopicRoute = mqTopicManager
                .getPulsarTopicRoute(rmqTopic.getPulsarTopicName(), Strings.EMPTY);
        Preconditions.checkArgument(pulsarTopicRoute != null && !pulsarTopicRoute.isEmpty());
        List<Integer> queueList = pulsarTopicRoute.get(this.brokerTag);
        Preconditions.checkArgument(queueList != null && !queueList.isEmpty() && queueId < queueList.size());
        return mqTopicManager.isPartitionTopicOwner(rmqTopic.getPulsarTopicName(), queueList.get(queueId));
    }

    @Override
    public void processRequestCommand(ChannelHandlerContext ctx, RemotingCommand cmd) throws RemotingCommandException {
        switch (cmd.getCode()) {
            case PULL_MESSAGE:
                PullMessageRequestHeader pullMsgHeader =
                        (PullMessageRequestHeader) cmd.decodeCommandCustomHeader(PullMessageRequestHeader.class);
                boolean isOwnedBroker = checkTopicOwnerBroker(pullMsgHeader.getTopic(), pullMsgHeader.getQueueId());

            case SEND_MESSAGE:
            case SEND_MESSAGE_V2:
            case SEND_BATCH_MESSAGE:
                RemotingCommand response = sendResponseThreadLocal.get();
                SendMessageRequestHeader sendHeader = SendMessageProcessor.parseRequestHeader(cmd);
                response.setCode(-1);
                SendMessageProcessor
                        .msgCheck(brokerController.getServerConfig(), mqTopicManager, ctx, sendHeader, response);
                if (response.getCode() != -1) {
                    //fail to msgCheck and flush error at once;
                    ctx.writeAndFlush(response);
                } else {
                    boolean isOwnerBroker = checkTopicOwnerBroker(sendHeader.getTopic(), sendHeader.getQueueId());

                    if (isOwnerBroker) {
                        super.processRequestCommand(ctx, cmd);
                    } else {

                    }
                }
                break;
            case QUERY_MESSAGE:
            case CONSUMER_SEND_MSG_BACK: //TODO: CommitLogOffset 0
                break;
            default:
                super.processRequestCommand(ctx, cmd);
                break;
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
            ropZkCache.start();

            this.zkService = new RopZookeeperCacheService(ropZkCache);
            registerBrokerZNode();
            setBrokerTagListener();

            this.coordinator = new RopCoordinator(brokerController, zkService);
            this.coordinator.start();

            this.mqTopicManager.start(zkService);
        } catch (Exception e) {
            log.error("RopBrokerProxy fail to start.", e);
        }
    }

    private String setBrokerTagListener() throws Exception {
        String brokerHost = brokerController.getBrokerHost();
        RopClusterContent clusterContent = zkService.getClusterContent();
        for (Entry<String, List<String>> entry : clusterContent.getBrokerCluster().entrySet()) {
            if (entry.getValue().contains(brokerHost)) {
                this.brokerTag = entry.getKey();
            }
        }
        zkService.getClusterDataCache().registerListener((path, data, stat) -> {
            if (BROKER_CLUSTER_PATH.equals(path)) {
                log.info("the cluster[{}] configure have changed, new configure: [{}].", data);
                String host = brokerController.getBrokerHost();
                for (Entry<String, List<String>> entry : data.getBrokerCluster().entrySet()) {
                    if (entry.getValue().contains(host)) {
                        brokerTag = entry.getKey();
                    }
                }
            }
        });
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
                    } catch (Exception e) {
                        log.warn("broker[{}] is already exists.", hostName);
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
        processorProxyRegisters.add(new ConsumerManageProcessorProxy(brokerController.getConsumerManageExecutor()));

        // EndTransactionProcessor
        processorProxyRegisters.add(new EndTransactionProcessorProxy(brokerController.getEndTransactionExecutor()));

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
            RopBrokerProxy.this.registerProcessor(RequestCode.WIPE_WRITE_PERM_OF_BROKER, this, processorExecutor);
            RopBrokerProxy.this
                    .registerProcessor(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.DELETE_TOPIC_IN_NAMESRV, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_KVLIST_BY_NAMESPACE, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_TOPICS_BY_CLUSTER, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_UNIT_TOPIC_LIST, this, processorExecutor);
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, this, processorExecutor);
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
    protected class ConsumerManageProcessorProxy extends ConsumerManageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public ConsumerManageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            RopBrokerProxy.this.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, this, processorExecutor);
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
    protected class EndTransactionProcessorProxy extends EndTransactionProcessor implements ProcessorProxyRegister {

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

    public List<String> getAllBrokers() {
        return zkService.getAllBrokers();
    }

}
