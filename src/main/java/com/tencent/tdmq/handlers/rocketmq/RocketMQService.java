package com.tencent.tdmq.handlers.rocketmq;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.pulsar.ZookeeperSessionExpiredHandlers;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.stats.MetricsGenerator;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsServlet;
import org.apache.pulsar.broker.web.WebService;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.zookeeper.LocalZooKeeperConnectionService;
import org.apache.pulsar.zookeeper.ZookeeperSessionExpiredHandler;
import org.eclipse.jetty.servlet.ServletHolder;

@Slf4j
public class RocketMQService extends PulsarService {

    @Getter
    private final RocketMQServiceConfiguration config;


    public RocketMQService(RocketMQServiceConfiguration config) {
        super(config);
        this.config = config;
    }

    @Override
    public Map<String, String> getProtocolDataToAdvertise() {
        return ImmutableMap.<String, String>builder()
                .put("kafka", config.getRocketmqListeners())
                .build();
    }

    @Override
    public void start() throws PulsarServerException {
        ReentrantLock lock = getMutex();

        lock.lock();

        try {
            // TODO: add Kafka on Pulsar Version support -- https://github.com/streamnative/kop/issues/3
            log.info("Starting Pulsar Broker service powered by Pulsar version: '{}'",
                    (getBrokerVersion() != null ? getBrokerVersion() : "unknown"));

            if (getState() != State.Init) {
                throw new PulsarServerException("Cannot start the service once it was stopped");
            }

            if (config.getRocketmqListeners() == null || config.getRocketmqListeners().isEmpty()) {
                throw new IllegalArgumentException("Kafka Listeners should be provided through brokerConf.listeners");
            }

            if (config.getAdvertisedAddress() != null
                    && !config.getRocketmqListeners().contains(config.getAdvertisedAddress())) {
                String err = "Error config: advertisedAddress - " + config.getAdvertisedAddress()
                        + " and listeners - " + config.getRocketmqListeners() + " not match.";
                log.error(err);
                throw new IllegalArgumentException(err);
            }

            setOrderedExecutor(OrderedExecutor.newBuilder().numThreads(8).name("pulsar-ordered")
                    .build());

            // init KafkaProtocolHandler
            RocketMQProtocolHandler rocketmqHandler = new RocketMQProtocolHandler();
            rocketmqHandler.initialize(config);

            // Now we are ready to start services
            setLocalZooKeeperConnectionProvider(new LocalZooKeeperConnectionService(getZooKeeperClientFactory(),
                    config.getZookeeperServers(), config.getZooKeeperSessionTimeoutMillis()));
            ZookeeperSessionExpiredHandler expiredHandler =
                    ZookeeperSessionExpiredHandlers.shutdownWhenZookeeperSessionExpired(getShutdownService());
            getLocalZooKeeperConnectionProvider().start(expiredHandler);

            // Initialize and start service to access configuration repository.
            startZkCacheService();

            BookKeeperClientFactory bkClientFactory = newBookKeeperClientFactory();
            setBkClientFactory(bkClientFactory);
            setManagedLedgerClientFactory(
                    new ManagedLedgerClientFactory(config, getZkClient(), bkClientFactory));
            setBrokerService(new BrokerService(this));

            // Start load management service (even if load balancing is disabled)
            getLoadManager().set(LoadManager.create(this));

            setDefaultOffloader(createManagedLedgerOffloader(OffloadPolicies.create(config.getProperties())));

            getBrokerService().start();

            WebService webService = new WebService(this);
            setWebService(webService);
            Map<String, Object> attributeMap = Maps.newHashMap();
            attributeMap.put(WebService.ATTRIBUTE_PULSAR_NAME, this);
            Map<String, Object> vipAttributeMap = Maps.newHashMap();
            vipAttributeMap.put(VipStatus.ATTRIBUTE_STATUS_FILE_PATH, config.getStatusFilePath());
            vipAttributeMap.put(VipStatus.ATTRIBUTE_IS_READY_PROBE, new Supplier<Boolean>() {
                @Override
                public Boolean get() {
                    // Ensure the VIP status is only visible when the broker is fully initialized
                    return getState() == State.Started;
                }
            });
            webService.addRestResources("/",
                    VipStatus.class.getPackage().getName(), false, vipAttributeMap);
            webService.addRestResources("/",
                    "org.apache.pulsar.broker.web", false, attributeMap);
            webService.addRestResources("/admin",
                    "org.apache.pulsar.broker.admin.v1", true, attributeMap);
            webService.addRestResources("/admin/v2",
                    "org.apache.pulsar.broker.admin.v2", true, attributeMap);
            webService.addRestResources("/admin/v3",
                    "org.apache.pulsar.broker.admin.v3", true, attributeMap);
            webService.addRestResources("/lookup",
                    "org.apache.pulsar.broker.lookup", true, attributeMap);

            webService.addServlet("/metrics",
                    new ServletHolder(
                            new PrometheusMetricsServlet(
                                    this,
                                    config.isExposeTopicLevelMetricsInPrometheus(),
                                    config.isExposeConsumerLevelMetricsInPrometheus())),
                    false, attributeMap);

            if (log.isDebugEnabled()) {
                log.debug("Attempting to add static directory");
            }
            webService.addStaticResources("/static", "/static");

            setSchemaRegistryService(SchemaRegistryService.create(null, new HashSet<>()));

            webService.start();

            // Refresh addresses, since the port might have been dynamically assigned
            setWebServiceAddress(webAddress(config));
            setWebServiceAddressTls(webAddressTls(config));
            setBrokerServiceUrl(config.getBrokerServicePort().isPresent()
                    ? brokerUrl(advertisedAddress(config), getBrokerListenPort().get())
                    : null);
            setBrokerServiceUrlTls(brokerUrlTls(config));

            // needs load management service
            this.startNamespaceService();

            // Start the leader election service
            startLeaderElectionService();

            // Register heartbeat and bootstrap namespaces.
            getNsService().registerBootstrapNamespaces();

            setMetricsGenerator(new MetricsGenerator(this));

            // By starting the Load manager service, the broker will also become visible
            // to the rest of the broker by creating the registration z-node. This needs
            // to be done only when the broker is fully operative.
            startLoadManagementService();

            acquireSLANamespace();

            final String bootstrapMessage = "bootstrap service "
                    + (config.getWebServicePort().isPresent()
                    ? "port = " + config.getWebServicePort().get() : "")
                    + (config.getWebServicePortTls().isPresent()
                    ? "tls-port = " + config.getWebServicePortTls() : "")
                    + ("kafka listener url= " + config.getRocketmqListeners());

            // start Kafka protocol handler.
            // put after load manager for the use of existing broker service to create internal topics.
            rocketmqHandler.start(this.getBrokerService());

            Map<InetSocketAddress, ChannelInitializer<SocketChannel>> channelInitializer =
                    rocketmqHandler.newChannelInitializers();
            Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> protocolHandlers = ImmutableMap
                    .<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>>builder()
                    .put("kafka", channelInitializer)
                    .build();
            getBrokerService().startProtocolHandlers(protocolHandlers);

            setState(State.Started);

            log.info("Kafka messaging service is ready, {}, cluster={}, configs={}",
                    bootstrapMessage, config.getClusterName(),
                    ReflectionToStringBuilder.toString(config));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws PulsarServerException {
        super.close();
    }

}
