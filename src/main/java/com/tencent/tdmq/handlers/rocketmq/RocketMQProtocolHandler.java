package com.tencent.tdmq.handlers.rocketmq;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.inner.namesvr.MQTopicManager;
import com.tencent.tdmq.handlers.rocketmq.utils.ConfigurationUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 12:17 下午
 */

/**
 * 这个类主要用来实现 pulsar 中 ProtocolHandler 的接口
 * 可以通过 pulsar service 加载以及运行
 */
@Slf4j
@Data
public class RocketMQProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "rocketmq";
    public static final String SSL_PREFIX = "SSL://";
    public static final String PLAINTEXT_PREFIX = "rocketmq://";
    public static final String LISTENER_DEL = ",";
    public static final String LISTENER_PATTEN = "^(rocketmq?|SSL)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*:([0-9]+)";

    private RocketMQServiceConfiguration rocketmqConfig;
    private BrokerService brokerService;
    private RocketMQBrokerController rocketMQBroker;
    private String bindAddress;

    public static int getListenerPort(String listener) {
        checkState(listener.matches(LISTENER_PATTEN), "listener not match patten");

        int lastIndex = listener.lastIndexOf(':');
        return Integer.parseInt(listener.substring(lastIndex + 1));
    }

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equals(protocol.toLowerCase(Locale.ROOT));
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // init config
        if (conf instanceof RocketMQServiceConfiguration) {
            // in unit test, passed in conf will be AmqpServiceConfiguration
            rocketmqConfig = (RocketMQServiceConfiguration) conf;
        } else {
            // when loaded with PulsarService as NAR, `conf` will be type of ServiceConfiguration
            rocketmqConfig = ConfigurationUtils.create(conf.getProperties(), RocketMQServiceConfiguration.class);
        }
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(rocketmqConfig.getBindAddress());
        this.rocketMQBroker = new RocketMQBrokerController(rocketmqConfig);
        this.rocketMQBroker.initialize();
        RocketMQTopic.initialize(rocketmqConfig.getRocketmqTenant() + "/" + rocketmqConfig.getRocketmqNamespace());
    }

    @Override
    public String getProtocolDataToAdvertise() {
        if (log.isDebugEnabled()) {
            log.debug("Get configured listeners", rocketmqConfig.getRocketmqListeners());
        }
        return rocketmqConfig.getRocketmqListeners();
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;
        rocketMQBroker.setBrokerService(brokerService);
        log.info("Starting RocketmqProtocolHandler, listener: {}, rop version is: '{}'",
                rocketmqConfig.getRocketmqListeners(), RopVersion.getVersion());
        log.info("Git Revision {}", RopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
                RopVersion.getBuildUser(),
                RopVersion.getBuildHost(),
                RopVersion.getBuildTime());
        try {
            String cluster = rocketmqConfig.getClusterName();
            String metaTanant = rocketmqConfig.getRocketmqMetadataTenant();
            String metaNs = rocketmqConfig.getRocketmqMetadataNamespace();
            String defaultTanant = rocketmqConfig.getRocketmqTenant();
            String defaultNs = rocketmqConfig.getRocketmqNamespace();

            createSysNamespaceIfNeeded(service, cluster, metaTanant, metaNs);
            createSysNamespaceIfNeeded(service, cluster, defaultTanant, defaultNs);
            createSystemTopic(service, defaultTanant, defaultNs,
                    rocketmqConfig.getRmqScheduleTopic(), rocketmqConfig.getDefaultNumPartitions());
            createSystemTopic(service, defaultTanant, defaultNs,
                    rocketmqConfig.getRmqSysTransHalfTopic(), rocketmqConfig.getDefaultNumPartitions());
            createSystemTopic(service, defaultTanant, defaultNs,
                    rocketmqConfig.getRmqSysTransOpHalfTopic(), rocketmqConfig.getDefaultNumPartitions());
            createSystemTopic(service, defaultTanant, defaultNs,
                    rocketmqConfig.getRmqTransCheckMaxTimeTopic(), 1);

            loadSysTopics(service, defaultTanant, defaultNs, rocketmqConfig.getRmqScheduleTopic());
            loadSysTopics(service, defaultTanant, defaultNs, rocketmqConfig.getRmqSysTransHalfTopic());
            loadSysTopics(service, defaultTanant, defaultNs, rocketmqConfig.getRmqSysTransOpHalfTopic());
            loadSysTopics(service, defaultTanant, defaultNs, rocketmqConfig.getRmqTransCheckMaxTimeTopic());

            rocketMQBroker.start();
        } catch (Exception e) {
            log.error("start rop error.", e);
        }

    }
    private String loadSysTopics(BrokerService brokerService, String tenant, String ns, String topicName) {
        String fullTopicName = Joiner.on('/').join(tenant, ns, topicName);
        String broker = null;
        try {
            broker = brokerService.pulsar().getAdminClient().lookups().lookupTopic(fullTopicName);
        } catch (Exception e) {
            log.warn("load system topic [{}] error.", fullTopicName, e);
        }
        return broker;
    }

    private String createSystemTopic(BrokerService service, String tenant, String ns, String topicName, int pNum)
            throws PulsarServerException, PulsarAdminException {
        String fullTopicName = Joiner.on('/').join(tenant, ns, topicName);
        PartitionedTopicMetadata topicMetadata =
                service.pulsar().getAdminClient().topics().getPartitionedTopicMetadata(fullTopicName);
        if (topicMetadata.partitions <= 0) {
            log.info("RocketMQ metadata topic {} doesn't exist. Creating it ...", fullTopicName);
            try {
                service.pulsar().getAdminClient().topics().createPartitionedTopic(
                        fullTopicName,
                        pNum);

                for (int i = 0; i < pNum; i++) {
                    service.pulsar().getAdminClient().topics()
                            .createNonPartitionedTopic(fullTopicName + PARTITIONED_TOPIC_SUFFIX + i);
                }
            } catch (ConflictException e) {
                log.info("Topic {} concurrent creating and cause e: ", fullTopicName, e);
                return fullTopicName;
            }
        }
        return fullTopicName;
    }

    private void createSysNamespaceIfNeeded(BrokerService service, String cluster, String tanant, String ns)
            throws PulsarServerException, PulsarAdminException {
        PulsarAdmin pulsarAdmin = service.pulsar().getAdminClient();
        String fullNs = Joiner.on('/').join(tanant, ns);
        try {
            ClusterData clusterData = new ClusterData(service.pulsar().getWebServiceAddress(),
                    null /* serviceUrlTls */,
                    service.pulsar().getBrokerServiceUrl(),
                    null /* brokerServiceUrlTls */);
            if (!pulsarAdmin.clusters().getClusters().contains(cluster)) {
                pulsarAdmin.clusters().createCluster(cluster, clusterData);
            } else {
                pulsarAdmin.clusters().updateCluster(cluster, clusterData);
            }

            if (!pulsarAdmin.tenants().getTenants().contains(tanant)) {
                pulsarAdmin.tenants().createTenant(tanant,
                        new TenantInfo(Sets.newHashSet(rocketmqConfig.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }
            if (!pulsarAdmin.namespaces().getNamespaces(tanant).contains(fullNs)) {
                Set<String> clusters = Sets.newHashSet(rocketmqConfig.getClusterName());
                pulsarAdmin.namespaces().createNamespace(fullNs, clusters);
                pulsarAdmin.namespaces().setNamespaceReplicationClusters(fullNs, clusters);
                pulsarAdmin.namespaces().setRetention(fullNs,
                        new RetentionPolicies(-1, -1));
            }
        } catch (PulsarAdminException e) {
            if (e instanceof ConflictException) {
                log.info("Resources concurrent creating and cause e: ", e);
                return;
            }
            log.error("Failed to get retention policy for kafka metadata namespace {}", fullNs, e);
            throw e;
        }
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(rocketmqConfig != null);
        checkState(rocketmqConfig.getRocketmqListeners() != null);
        checkState(brokerService != null);
        checkState(rocketMQBroker != null);

        String listeners = rocketmqConfig.getRocketmqListeners();
        String[] parts = listeners.split(LISTENER_DEL);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.builder();

            for (String listener : parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new RocketMQChannelInitializer(rocketmqConfig, rocketMQBroker,
                                    brokerService, false));
                } else {
                    log.error("Rocketmq listener {} not supported. supports {} and {}",
                            listener, PLAINTEXT_PREFIX, SSL_PREFIX);
                }
            }

            return builder.build();
        } catch (Exception e) {
            log.error("RocketmqProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {
        // no-on
    }
}
