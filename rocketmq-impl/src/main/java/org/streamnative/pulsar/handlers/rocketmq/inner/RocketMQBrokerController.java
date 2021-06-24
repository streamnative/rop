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

package org.streamnative.pulsar.handlers.rocketmq.inner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.util.ServiceProvider;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQServiceConfiguration;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.ConsumerManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.ConsumerOffsetManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.SubscriptionGroupManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.listener.AbstractTransactionalMessageCheckListener;
import org.streamnative.pulsar.handlers.rocketmq.inner.listener.DefaultConsumerIdsChangeListener;
import org.streamnative.pulsar.handlers.rocketmq.inner.listener.DefaultTransactionalMessageCheckListener;
import org.streamnative.pulsar.handlers.rocketmq.inner.listener.NotifyMessageArrivingListener;
import org.streamnative.pulsar.handlers.rocketmq.inner.namesvr.MQTopicManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.namesvr.NameserverProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.AdminBrokerProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.ClientManageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.ConsumerManageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.EndTransactionProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.PullMessageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.QueryMessageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.SendMessageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ProducerManager;

/**
 * RocketMQ broker controller.
 */
@Data
@Slf4j
public class RocketMQBrokerController {

    private final RocketMQServiceConfiguration serverConfig;
    private final ConsumerOffsetManager consumerOffsetManager;
    private final ConsumerManager consumerManager;
    private final ProducerManager producerManager;
    private final ClientHousekeepingService clientHousekeepingService;
    private final PullMessageProcessor pullMessageProcessor;
    private final PullRequestHoldService pullRequestHoldService;
    private final MessageArrivingListener messageArrivingListener;
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private final RebalancedLockManager rebalancedLockManager = new RebalancedLockManager();
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
                    "BrokerControllerScheduledThread"));

    private final BlockingQueue<Runnable> sendThreadPoolQueue;
    private final BlockingQueue<Runnable> pullThreadPoolQueue;
    private final BlockingQueue<Runnable> replyThreadPoolQueue;
    private final BlockingQueue<Runnable> queryThreadPoolQueue;
    private final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
    private final BlockingQueue<Runnable> heartbeatThreadPoolQueue;
    private final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
    private final BlockingQueue<Runnable> endTransactionThreadPoolQueue;
    private final BrokerStatsManager brokerStatsManager;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    private final RocketMQRemoteServer remotingServer;
    private final Broker2Client broker2Client = new Broker2Client(this);

    private MQTopicManager topicConfigManager;
    private ExecutorService sendMessageExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService replyMessageExecutor;
    private ExecutorService queryMessageExecutor;
    private ExecutorService adminBrokerExecutor;
    private ExecutorService clientManageExecutor;
    private ExecutorService heartbeatExecutor;
    private ExecutorService consumerManageExecutor;
    private ExecutorService endTransactionExecutor;
    private BrokerStats brokerStats;
    private String brokerHost;
    private TransactionalMessageCheckService transactionalMessageCheckService;
    private TransactionalMessageService transactionalMessageService;
    private AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;
    private volatile BrokerService brokerService;
    private ScheduleMessageService delayedMessageService;

    public static String stringToken = Strings.EMPTY;

    public RocketMQBrokerController(final RocketMQServiceConfiguration serverConfig) throws PulsarServerException {
        this.serverConfig = serverConfig;
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.topicConfigManager = new MQTopicManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);

        this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
                this.serverConfig.getSendThreadPoolQueueCapacity());
        this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
                this.serverConfig.getPullThreadPoolQueueCapacity());
        this.replyThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
                this.serverConfig.getReplyThreadPoolQueueCapacity());
        this.queryThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
                this.serverConfig.getQueryThreadPoolQueueCapacity());
        this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
                this.serverConfig.getClientManagerThreadPoolQueueCapacity());
        this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
                this.serverConfig.getConsumerManagerThreadPoolQueueCapacity());
        this.heartbeatThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
                this.serverConfig.getHeartbeatThreadPoolQueueCapacity());
        this.endTransactionThreadPoolQueue = new LinkedBlockingQueue<Runnable>(
                this.serverConfig.getEndTransactionPoolQueueCapacity());

        this.brokerStatsManager = new BrokerStatsManager(serverConfig.getBrokerName());
        this.remotingServer = new RocketMQRemoteServer(this.serverConfig, this.clientHousekeepingService);
        this.delayedMessageService = new ScheduleMessageService(this, serverConfig);
    }

    public void initialize() throws Exception {
        this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.serverConfig.getSendMessageThreadPoolNums(),
                this.serverConfig.getSendMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.sendThreadPoolQueue,
                new ThreadFactoryImpl("SendMessageThread_"));

        this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.serverConfig.getPullMessageThreadPoolNums(),
                this.serverConfig.getPullMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.pullThreadPoolQueue,
                new ThreadFactoryImpl("PullMessageThread_"));

        this.queryMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.serverConfig.getQueryMessageThreadPoolNums(),
                this.serverConfig.getQueryMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.queryThreadPoolQueue,
                new ThreadFactoryImpl("QueryMessageThread_"));

        this.adminBrokerExecutor =
                Executors
                        .newFixedThreadPool(this.serverConfig.getAdminBrokerThreadPoolNums(), new ThreadFactoryImpl(
                                "AdminBrokerThread_"));

        this.clientManageExecutor = new ThreadPoolExecutor(
                this.serverConfig.getClientManageThreadPoolNums(),
                this.serverConfig.getClientManageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.clientManagerThreadPoolQueue,
                new ThreadFactoryImpl("ClientManageThread_"));

        this.heartbeatExecutor = new BrokerFixedThreadPoolExecutor(
                this.serverConfig.getHeartbeatThreadPoolNums(),
                this.serverConfig.getHeartbeatThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.heartbeatThreadPoolQueue,
                new ThreadFactoryImpl("HeartbeatThread_", true));

        this.endTransactionExecutor = new BrokerFixedThreadPoolExecutor(
                this.serverConfig.getEndTransactionThreadPoolNums(),
                this.serverConfig.getEndTransactionThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.endTransactionThreadPoolQueue,
                new ThreadFactoryImpl("EndTransactionThread_"));

        this.consumerManageExecutor =
                Executors.newFixedThreadPool(this.serverConfig.getConsumerManageThreadPoolNums(),
                        new ThreadFactoryImpl(
                                "ConsumerManageThread_"));

        this.registerProcessor();

        final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
        final long period = 1000 * 60 * 60 * 24;
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    RocketMQBrokerController.this.getBrokerStats().record();
                } catch (Throwable e) {
                    log.error("schedule record error.", e);
                }
            }
        }, initialDelay, period, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    RocketMQBrokerController.this.consumerOffsetManager.persist();
                } catch (Throwable e) {
                    log.error("schedule persist consumerOffset error.", e);
                }
            }
        }, 1000 * 10, this.serverConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    RocketMQBrokerController.this.printWaterMark();
                } catch (Throwable e) {
                    log.error("printWaterMark error.", e);
                }
            }
        }, 60, 30, TimeUnit.SECONDS);

        if (this.serverConfig.isRopAclEnable()) {
            initialAcl();
            initialRpcHooks();
        }

        if (this.serverConfig.isRopTransactionEnable()) {
            initialTransaction();
        }
    }


    private void initialAcl() {
        if (!this.serverConfig.isRopAclEnable()) {
            log.info("The broker dose not enable acl");
            return;
        }

        String originalAuthToken = this.serverConfig.getBrokerClientAuthenticationParameters();
        if (Strings.EMPTY.equals(originalAuthToken)) {
            log.error("Get the broker client auth token is null, please check.");
            throw new AclException("Get the broker client auth token is null, please check.");
        }

        String[] parts = StringUtils.split(originalAuthToken, ":");
        String authToken = parts[1];

        getRemotingServer().registerRPCHook(new RPCHook() {
            @Override
            public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                if (request.getExtFields() == null) {
                    // If request's extFields is null,then return "".
                    return;
                }

                // token authorization logic
                String token = request.getExtFields().get(SessionCredentials.ACCESS_KEY);
                if (Strings.EMPTY.equals(token)) {
                    log.error("The access key is null, please check.");
                    throw new AclException("No accessKey is configured");
                }
                AuthenticationService authService = brokerService.getAuthenticationService();
                AuthenticationDataCommand authCommand = new AuthenticationDataCommand(token);

                log.info("The user upload token is: {} and the superuser token is: {}", token, authToken);
                if (RequestCode.SEND_MESSAGE == request.getCode()
                        || RequestCode.SEND_MESSAGE_V2 == request.getCode()
                        || RequestCode.CONSUMER_SEND_MSG_BACK == request.getCode()
                        || RequestCode.SEND_BATCH_MESSAGE == request.getCode()) {

                    try {
                        SendMessageRequestHeader requestHeader = SendMessageProcessor
                                .parseRequestHeader(request);
                        if (requestHeader == null) {
                            log.warn("Parse send message request header.");
                            return;
                        }

                        log.info("The use topic is: {}", requestHeader.getTopic());
                        String roleSubject = authService.authenticate(authCommand, "token");
                        if (Strings.EMPTY.equals(roleSubject)) {
                            log.error("The upload token:{} is wrong.", token);
                            throw new AclException("[PRODUCE] The uploaded token is wrong");
                        }

                        ClientTopicName clientTopicName = new ClientTopicName(requestHeader.getTopic());
                        String topicName = clientTopicName.getPulsarTopicName();

                        Boolean authOK = brokerService.getAuthorizationService()
                                .allowTopicOperationAsync(TopicName.get(topicName), TopicOperation.PRODUCE,
                                        roleSubject,
                                        authCommand).get();
                        if (!authOK) {
                            log.error("[PRODUCE] Token authentication failed, please check");
                            throw new AclException("[PRODUCE] Token authentication failed, please check");
                        }

                        log.info("Successfully for send auth: {}", authOK);
                    } catch (Exception e) {
                        log.error("[PRODUCE] Throws exception:{}", e.getMessage());
                        throw new RuntimeException(e);
                    }
                } else if (RequestCode.PULL_MESSAGE == request.getCode()) {
                    try {
                        final PullMessageRequestHeader requestHeader =
                                (PullMessageRequestHeader) request
                                        .decodeCommandCustomHeader(PullMessageRequestHeader.class);

                        String roleSubject = authService.authenticate(authCommand, "token");
                        if (Strings.EMPTY.equals(roleSubject)) {
                            log.error("The upload token:{} is wrong.", token);
                            throw new AclException("[CONSUME] The uploaded token is wrong");
                        }

                        ClientTopicName clientTopicName = new ClientTopicName(requestHeader.getTopic());
                        String topicName = clientTopicName.getPulsarTopicName();
                        Boolean authOK = brokerService.getAuthorizationService()
                                .allowTopicOperationAsync(TopicName.get(topicName), TopicOperation.PRODUCE,
                                        roleSubject,
                                        authCommand).get();
                        if (!authOK) {
                            log.error("[CONSUME] Token authentication failed, please check");
                            throw new AclException("[CONSUME] Token authentication failed, please check");
                        }
                        log.info("Successfully for receive auth");
                    } catch (Exception e) {
                        log.error("[CONSUME] Throws exception:{}", e.getMessage());
                        throw new RuntimeException(e);
                    }
                } else if (RequestCode.UPDATE_AND_CREATE_TOPIC == request.getCode()
                        || RequestCode.DELETE_TOPIC_IN_BROKER == request.getCode()
                        || RequestCode.UPDATE_BROKER_CONFIG == request.getCode()
                        || RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP == request.getCode()
                        || RequestCode.DELETE_SUBSCRIPTIONGROUP == request.getCode()
                        || RequestCode.INVOKE_BROKER_TO_RESET_OFFSET == request.getCode()) {

                    log.info("Into admin auth logic and the check is: {}", authToken.equals(token));
                    if (!authToken.equals(token)) {
                        log.error("[ADMIN] Token authentication failed, please check");
                        throw new AclException("[ADMIN] Token authentication failed, please check");
                    }
                    log.info("Successfully for admin auth");
                } else {
                    log.info("No auth check.");
                }

            }

            @Override
            public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
            }
        });

    }

    private void initialRpcHooks() {

        List<RPCHook> rpcHooks = ServiceProvider.load(ServiceProvider.RPC_HOOK_ID, RPCHook.class);
        if (rpcHooks == null || rpcHooks.isEmpty()) {
            return;
        }
        for (RPCHook rpcHook : rpcHooks) {
            this.registerServerRPCHook(rpcHook);
        }
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
    }

    private void initialTransaction() {
        this.transactionalMessageService = ServiceProvider
                .loadClass(ServiceProvider.TRANSACTION_SERVICE_ID, TransactionalMessageService.class);
        if (null == this.transactionalMessageService) {
            this.transactionalMessageService = new TransactionalMessageServiceImpl(
                    new TransactionalMessageBridge(this, null));
            log.warn("Load default transaction message hook service: {}",
                    TransactionalMessageServiceImpl.class.getSimpleName());
        }
        this.transactionalMessageCheckListener = ServiceProvider
                .loadClass(ServiceProvider.TRANSACTION_LISTENER_ID, AbstractTransactionalMessageCheckListener.class);
        if (null == this.transactionalMessageCheckListener) {
            this.transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
            log.warn("Load default discard message hook service: {}",
                    DefaultTransactionalMessageCheckListener.class.getSimpleName());
        }
        this.transactionalMessageCheckListener.setBrokerController(this);
        this.transactionalMessageCheckService = new TransactionalMessageCheckService(this);
    }

    public void registerProcessor() throws PulsarServerException {

        // SendMessageProcessor
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);
        sendProcessor.registerConsumeMessageHook(consumeMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);

        // PullMessageProcessor
        this.remotingServer
                .registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        // QueryMessageProcessor
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

        // ClientManageProcessor
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.heartbeatExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, this.clientManageExecutor);

        // ConsumerManageProcessor
        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor,
                this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor,
                this.consumerManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor,
                this.consumerManageExecutor);

        // EndTransactionProcessor
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this),
                this.endTransactionExecutor);

        // NameserverProcessor
        NameserverProcessor namesvrProcessor = new NameserverProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.PUT_KV_CONFIG, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_KV_CONFIG, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(RequestCode.DELETE_KV_CONFIG, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.QUERY_DATA_VERSION, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(RequestCode.REGISTER_BROKER, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.UNREGISTER_BROKER, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.GET_ROUTEINTO_BY_TOPIC, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.GET_BROKER_CLUSTER_INFO, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.WIPE_WRITE_PERM_OF_BROKER, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, namesvrProcessor,
                this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.DELETE_TOPIC_IN_NAMESRV, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.GET_KVLIST_BY_NAMESPACE, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.GET_TOPICS_BY_CLUSTER, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, namesvrProcessor,
                this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.GET_UNIT_TOPIC_LIST, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, namesvrProcessor,
                this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.UPDATE_NAMESRV_CONFIG, namesvrProcessor, this.adminBrokerExecutor);
        this.remotingServer
                .registerProcessor(RequestCode.GET_NAMESRV_CONFIG, namesvrProcessor, this.adminBrokerExecutor);

        // Default
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }

    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
/*  TODO:          RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : this.messageStore.now() - rt.getCreateTimestamp();*/
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return this.headSlowTimeMills(this.queryThreadPoolQueue);
    }

    public long headSlowTimeMills4EndTransactionThreadPoolQueue() {
        return this.headSlowTimeMills(this.endTransactionThreadPoolQueue);
    }

    public void printWaterMark() {
        log.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(),
                headSlowTimeMills4SendThreadPoolQueue());
        log.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(),
                headSlowTimeMills4PullThreadPoolQueue());
        log.info("[WATERMARK] Query Queue Size: {} SlowTimeMills: {}", this.queryThreadPoolQueue.size(),
                headSlowTimeMills4QueryThreadPoolQueue());
        log.info("[WATERMARK] Transaction Queue Size: {} SlowTimeMills: {}",
                this.endTransactionThreadPoolQueue.size(), headSlowTimeMills4EndTransactionThreadPoolQueue());
    }

    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.replyMessageExecutor != null) {
            this.replyMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        //TODO: this.consumerOffsetManager.persist();

        if (this.clientManageExecutor != null) {
            this.clientManageExecutor.shutdown();
        }

        if (this.queryMessageExecutor != null) {
            this.queryMessageExecutor.shutdown();
        }

        if (this.consumerManageExecutor != null) {
            this.consumerManageExecutor.shutdown();
        }

        if (this.transactionalMessageCheckService != null) {
            this.transactionalMessageCheckService.shutdown(false);
        }

        if (this.endTransactionExecutor != null) {
            this.endTransactionExecutor.shutdown();
        }

        if (this.topicConfigManager != null) {
            this.topicConfigManager.shutdown();
        }
    }

    public void start() throws Exception {

        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        if (this.topicConfigManager != null) {
            this.topicConfigManager.start();
        }

        if (this.delayedMessageService != null) {
            this.delayedMessageService.start();
        }

        if (this.subscriptionGroupManager != null) {
            this.subscriptionGroupManager.start();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    public RocketMQRemoteServer getRemotingServer() {
        return remotingServer;
    }
}