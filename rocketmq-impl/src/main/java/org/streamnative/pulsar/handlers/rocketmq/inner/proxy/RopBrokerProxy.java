package org.streamnative.pulsar.handlers.rocketmq.inner.proxy;

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQRemoteServer;
import org.streamnative.pulsar.handlers.rocketmq.inner.coordinator.RopCoordinator;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopServerException;
import org.streamnative.pulsar.handlers.rocketmq.inner.namesvr.NameserverProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.namesvr.TopicConfigManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.AdminBrokerProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.ClientManageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.ConsumerManageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.EndTransactionProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.PullMessageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.QueryMessageProcessor;
import org.streamnative.pulsar.handlers.rocketmq.inner.processor.SendMessageProcessor;

/**
 * Rop broker proxy is a rocketmq request simulator
 * find the real broker that topicPartition is stored on
 * and transfer the request to the owner broker
 */
@Slf4j
public class RopBrokerProxy implements NettyRequestProcessor, AutoCloseable {

    private final RocketMQBrokerController brokerController;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
    private RopZookeeperCacheService ropZookeeperCacheService;
    private final OrderedExecutor orderedExecutor;
    private RopCoordinator coordinator;
    private PulsarService pulsarService;
    private TopicConfigManager topicConfigManager;
    private final RocketMQRemoteServer remotingServer;
    private List<ProcessorProxyRegister> processorProxyRegisters = new ArrayList<>();

    public RopBrokerProxy(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.remotingServer = brokerController.getRemotingServer();
        this.orderedExecutor = OrderedExecutor.newBuilder().numThreads(4).name("rop-ordered-executor").build();
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext channelHandlerContext, RemotingCommand remotingCommand)
            throws Exception {

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public void start() throws RopServerException {
        Preconditions.checkNotNull(brokerController);
        this.pulsarService = brokerController.getBrokerService().pulsar();
        ServiceConfiguration config = this.pulsarService.getConfig();
        RopZookeeperCache ropZkCache = new RopZookeeperCache(pulsarService.getZkClientFactory(),
                (int) config.getZooKeeperSessionTimeoutMillis(),
                config.getZooKeeperOperationTimeoutSeconds(), config.getZookeeperServers(), orderedExecutor,
                brokerController.getScheduledExecutorService(), config.getZooKeeperCacheExpirySeconds());
        ropZkCache.start();
        this.ropZookeeperCacheService = new RopZookeeperCacheService(ropZkCache);
        this.coordinator = new RopCoordinator(brokerController, ropZookeeperCacheService);
        this.coordinator.start();
    }

    @Override
    public void close() throws Exception {
        this.coordinator.close();
        this.ropZookeeperCacheService.close();
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
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

    protected interface ProcessorProxyRegister {

        boolean registerProxyProcessor();
    }

    protected class AdminBrokerProcessorProxy extends AdminBrokerProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public AdminBrokerProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            remotingServer.registerDefaultProcessor(this, processorExecutor);
            return true;
        }

        @Override
        public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
                throws RemotingCommandException {
            //TODO
            return super.processRequest(ctx, request);
        }
    }

    protected class NameserverProcessorProxy extends NameserverProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public NameserverProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            remotingServer.registerProcessor(RequestCode.PUT_KV_CONFIG, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_KV_CONFIG, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.DELETE_KV_CONFIG, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.QUERY_DATA_VERSION, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.REGISTER_BROKER, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.UNREGISTER_BROKER, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_ROUTEINTO_BY_TOPIC, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_BROKER_CLUSTER_INFO, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.WIPE_WRITE_PERM_OF_BROKER, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.DELETE_TOPIC_IN_NAMESRV, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_KVLIST_BY_NAMESPACE, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_TOPICS_BY_CLUSTER, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_UNIT_TOPIC_LIST, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.UPDATE_NAMESRV_CONFIG, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.GET_NAMESRV_CONFIG, this, processorExecutor);
            return true;
        }
    }

    protected class ConsumerManageProcessorProxy extends ConsumerManageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public ConsumerManageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, this, processorExecutor);
            return true;
        }
    }

    protected class ClientManageProcessorProxy extends ClientManageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public ClientManageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            remotingServer.registerProcessor(RequestCode.HEART_BEAT, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, this, processorExecutor);
            return true;
        }
    }

    protected class QueryMessageProcessorProxy extends QueryMessageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public QueryMessageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, this, processorExecutor);
            return true;
        }
    }

    protected class PullMessageProcessorProxy extends PullMessageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public PullMessageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this, processorExecutor);
            return true;
        }
    }

    protected class SendMessageProcessorProxy extends SendMessageProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public SendMessageProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
            registerSendMessageHook(sendMessageHookList);
            registerConsumeMessageHook(consumeMessageHookList);
        }

        @Override
        public boolean registerProxyProcessor() {
            remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, this, processorExecutor);
            remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, this, processorExecutor);
            return true;
        }
    }

    protected class EndTransactionProcessorProxy extends EndTransactionProcessor implements ProcessorProxyRegister {

        private final ExecutorService processorExecutor;

        public EndTransactionProcessorProxy(ExecutorService processorExecutor) {
            super(RopBrokerProxy.this.brokerController);
            this.processorExecutor = processorExecutor;
        }

        @Override
        public boolean registerProxyProcessor() {
            remotingServer.registerProcessor(RequestCode.END_TRANSACTION, this, processorExecutor);
            return true;
        }
    }

}
