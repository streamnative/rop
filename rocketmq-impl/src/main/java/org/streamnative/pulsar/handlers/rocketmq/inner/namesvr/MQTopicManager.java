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

package org.streamnative.pulsar.handlers.rocketmq.inner.namesvr;

import static org.apache.pulsar.broker.web.PulsarWebResource.joinPath;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.streamnative.pulsar.handlers.rocketmq.inner.InternalProducer;
import org.streamnative.pulsar.handlers.rocketmq.inner.InternalServerCnx;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RopServerCnx;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.proxy.RopZookeeperCacheService;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopClusterContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopTopicContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.PulsarUtil;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;
import org.testng.collections.Maps;

/**
 * MQ topic manager.
 */
@Slf4j
public class MQTopicManager extends TopicConfigManager implements NamespaceBundleOwnershipListener {

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private PulsarService pulsarService;
    private BrokerService brokerService;
    private PulsarAdmin adminClient;
    private RopZookeeperCacheService zkService;

    public MQTopicManager(RocketMQBrokerController brokerController) {
        super(brokerController);
    }

    @Override
    protected void createPulsarPartitionedTopic(TopicConfig tc) {
        createOrUpdateTopic(tc);
    }

    public void start(RopZookeeperCacheService zkService) throws Exception {
        this.pulsarService = brokerController.getBrokerService().pulsar();
        this.brokerService = pulsarService.getBrokerService();
        this.adminClient = pulsarService.getAdminClient();
        this.zkService = zkService;
        this.createSysResource();
        this.pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(this);
        log.info("MQTopicManager started successfully.");
    }

    @Override
    public TopicConfig selectTopicConfig(String rmqTopicName) {
        //rmqTopicName: tenants|ns%topicName
        TopicConfig topicConfig = super.selectTopicConfig(rmqTopicName);
        try {
            if (topicConfig == null) {
                ClientTopicName clientTopicName = new ClientTopicName(rmqTopicName);
                RopTopicContent ropTopicContent = zkService.getTopicContent(clientTopicName.toPulsarTopicName());
                if (ropTopicContent != null) {
                    topicConfig = ropTopicContent.getConfig();
                    this.topicConfigTable.put(clientTopicName.getPulsarTopicName(), topicConfig);
                }
            }
        } catch (Exception e) {
            log.warn("zkService getTopicContent for topic[{}] error.", rmqTopicName, e);
        }
        return topicConfig;
    }

    public void shutdown() {
        shuttingDown.set(true);
    }

    public Map<String, List<Integer>> getPulsarTopicRoute(TopicName topicName, String listenerName) {
        try {
            String topicConfigKey = joinPath(topicName.getNamespace(), topicName.getLocalName());
            if (isTBW12Topic(topicName)) {
                if (this.topicConfigTable.containsKey(topicConfigKey)) {
                    RopClusterContent clusterContent = zkService.getClusterContent();
                    TopicConfig topicConfig = topicConfigTable.get(topicConfigKey);
                    int writeQueueNums = topicConfig.getWriteQueueNums();
                    return clusterContent.createTopicRouteMap(writeQueueNums);
                }
            } else {
                RopTopicContent ropTopicContent = zkService.getTopicContent(topicName);
                Preconditions
                        .checkNotNull(ropTopicContent, "RopTopicContent[" + topicName.toString() + "] can't be null.");
                brokerController.getRopBrokerProxy().getPulsarClient().getLookup().getBroker(topicName);
                this.topicConfigTable.put(topicConfigKey, ropTopicContent.getConfig());
                return ropTopicContent.getRouteMap();
            }
        } catch (Exception e) {
            log.warn("[{}] Get topic route error.", topicName.toString(), e);
        }
        return Maps.newHashMap();
    }

    /**
     * If current broker is this partition topic owner return true else return false.
     *
     * @param topicName topic name
     * @param partitionId real partition ID
     * @return if current broker is this partition topic owner return true else return false.
     */
    public boolean isPartitionTopicOwner(TopicName topicName, int partitionId) {
        return this.brokerController.getBrokerService().isTopicNsOwnedByBroker(topicName.getPartition(partitionId));
    }

    @Override
    public void onLoad(NamespaceBundle bundle) {
        // get new partitions owned by this pulsar service.
        pulsarService.getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                .whenComplete((topics, ex) -> {
                    if (ex == null) {
                        log.info("Bundle onLoad topic and size=[{}].", topics.size());
                        //load topic to cache
                        loadPersistentTopic(topics);
                    } else {
                        log.error("Failed to get owned topic list for "
                                        + "OffsetAndTopicListener when triggering on-loading bundle {}.",
                                bundle, ex);
                    }
                });
    }

    public void loadPersistentTopic(List<String> topics) {
        topics.forEach(topic -> {
            try {
                this.brokerService.getTopic(topic, false).whenComplete((t2, throwable) -> {
                    if (throwable == null && t2.isPresent()) {
                        PersistentTopic persistentTopic = (PersistentTopic) t2.get();
                        TopicName topicName = TopicName.get(topic);
                        ClientTopicName clientTopicName = new ClientTopicName(topicName);
                        this.brokerController.getConsumerOffsetManager()
                                .putPulsarTopic(clientTopicName, topicName.getPartitionIndex(), persistentTopic);
                    } else {
                        log.warn("getTopicIfExists error, topic=[{}].", topic);
                    }
                });
            } catch (Exception e) {
                log.warn("getTopicIfExists error, topic=[{}].", topic, e);
            }
        });
    }

    @Override
    public void unLoad(NamespaceBundle bundle) {
        pulsarService.getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                .whenComplete((topics, ex) -> {
                    if (ex == null) {
                        log.info("[MQTopicManager] unLoad bundle [{}], topic size [{}] ", bundle,
                                topics.size());
                        for (String topic : topics) {
                            log.info("[MQTopicManager] unload topic[{}] from current node.", topic);
                            TopicName partitionedTopic = TopicName.get(topic);

                            //remove cache from consumer offset manager
                            ClientTopicName clientTopicName = new ClientTopicName(
                                    partitionedTopic.getPartitionedTopicName());
                            this.brokerController.getConsumerOffsetManager()
                                    .removePulsarTopic(clientTopicName, partitionedTopic.getPartitionIndex());

                            removeReferenceProducer(topic);
                        }
                    } else {
                        log.error("Failed to get owned topic list for "
                                + "OffsetAndTopicListener when triggering un-loading bundle {}.", bundle, ex);
                    }
                });
    }

    @Override
    public boolean test(NamespaceBundle namespaceBundle) {
        NamespaceName namespaceObject = namespaceBundle.getNamespaceObject();
        String tenant = namespaceObject.getTenant();
        if (null == tenant || Strings.EMPTY.equals(tenant)) {
            return false;
        }
        return tenant.toLowerCase(Locale.ROOT).startsWith(config.getRocketmqTenant());
    }

    private void createSysResource() throws Exception {
        String cluster = config.getClusterName();
        String metaTenant = config.getRocketmqMetadataTenant();
        String metaNs = config.getRocketmqMetadataNamespace();
        String defaultTenant = config.getRocketmqTenant();
        String defaultNs = config.getRocketmqNamespace();

        //create system namespace & default namespace
        createPulsarNamespaceIfNeeded(brokerService, cluster, metaTenant, metaNs);
        createPulsarNamespaceIfNeeded(brokerService, cluster, defaultTenant, defaultNs);

        //for test
        createPulsarNamespaceIfNeeded(brokerService, cluster, "test1", "InstanceTest");
        for (TopicConfig topicConfig : this.topicConfigTable.values()) {
            //createOrUpdateTopic(topicConfig);
            createOrUpdateInnerTopic(topicConfig);
        }
    }

    private void createPulsarNamespaceIfNeeded(BrokerService service, String cluster, String tenant, String ns)
            throws Exception {
        String fullNs = Joiner.on('/').join(tenant, ns);
        try {

            ClusterData clusterData = ClusterData.builder()
                    .serviceUrl(service.getPulsar().getWebServiceAddress())
                    .serviceUrlTls(null)
                    .brokerServiceUrl(service.getPulsar().getBrokerServiceUrl())
                    .brokerServiceUrlTls(null)
                    .build();
            if (!adminClient.clusters().getClusters().contains(cluster)) {
                adminClient.clusters().createCluster(cluster, clusterData);
            } else {
                adminClient.clusters().updateCluster(cluster, clusterData);
            }

            if (!adminClient.tenants().getTenants().contains(tenant)) {
                adminClient.tenants().createTenant(tenant,
                        TenantInfo.builder()
                                .adminRoles(this.config.getSuperUserRoles())
                                .allowedClusters(Sets.newHashSet(cluster))
                                .build());
            }
            if (!adminClient.namespaces().getNamespaces(tenant).contains(fullNs)) {
                Set<String> clusters = Sets.newHashSet(this.config.getClusterName());
                adminClient.namespaces().createNamespace(fullNs, clusters);
                adminClient.namespaces().setNamespaceReplicationClusters(fullNs, clusters);
                adminClient.namespaces().setRetention(fullNs,
                        new RetentionPolicies(
                                this.brokerController.getServerConfig().getDefaultRetentionTimeInMinutes(), -1));
                adminClient.namespaces().setNamespaceMessageTTL(fullNs,
                        this.brokerController.getServerConfig().getDefaultRetentionTimeInMinutes() * 60);
            }
        } catch (Exception e) {
            if (e instanceof ConflictException) {
                log.info("Resources concurrent creating and cause e: ", e);
                return;
            }
            log.error("Failed to create sysName metadata namespace {}", fullNs, e);
            throw e;
        }
    }

    /**
     *
     */
    protected void createOrUpdateInnerTopic(final TopicConfig tc) throws Exception {
        log.info("Create or update inner topic config: [{}].", tc);
        String fullTopicName = tc.getTopicName();
        PartitionedTopicMetadata topicMetadata = null;

        try {
            topicMetadata = adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
        } catch (Exception e) {
            log.info("getPartitionedTopicMetadata[topic:{}] not exists.", fullTopicName);
        }

        try {
            if (topicMetadata == null || tc.getWriteQueueNums() > topicMetadata.partitions) {
                adminClient.topics().createPartitionedTopic(fullTopicName, tc.getWriteQueueNums());
            }
        } catch (Exception e) {
            log.warn("createOrUpdateInnerTopic error", e);
            throw e;
        }
    }

    /**
     * if pulsar topic not exist, create pulsar topic,
     * else update pulsar topic partition.
     * meanwhile create rop topic partition mapping table
     *
     * @param tc topic config
     */
    public void createOrUpdateTopic(final TopicConfig tc) {
        String fullTopicName = tc.getTopicName();
        log.info("Create or update topic [{}], config: [{}].", fullTopicName, tc);

        TopicName topicName = TopicName.get(fullTopicName);
        String tenant = topicName.getTenant();
        String ns = topicName.getNamespacePortion();
        tenant = Strings.isBlank(tenant) ? config.getRocketmqTenant() : tenant;
        ns = Strings.isBlank(ns) ? config.getRocketmqNamespace() : ns;

        try {
            if (brokerController.getServerConfig().isAutoCreateTopicEnable()) {
                createPulsarNamespaceIfNeeded(brokerService, config.getClusterName(), tenant, ns);
            }
        } catch (Exception e) {
            log.warn("CreatePulsarPartitionedTopic tenant=[{}] and namespace=[{}] error.", tenant, ns);
            throw new RuntimeException("Create topic error.");
        }

        RopClusterContent clusterBrokers;
        PartitionedTopicMetadata topicMetadata = null;
        int currentPartitionNum = 0;

        try {
            topicMetadata = adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
        } catch (Exception e) {
            log.warn("get partitioned topic metadata[{}] error:", fullTopicName, e);
        }

        currentPartitionNum = topicMetadata != null ? topicMetadata.partitions : currentPartitionNum;

        try {
            // get cluster brokers list
            clusterBrokers = zkService.getClusterContent();
        } catch (Exception e) {
            log.warn("clusterBrokers haven't been created, error: ", e);
            throw new RuntimeException("cluster config haven't been created.");
        }
        Preconditions.checkNotNull(clusterBrokers, "clusterBrokers can't be null.");

        // If the partition < 0, the topic does not exist, and create topic.
        if (currentPartitionNum <= 0) {
            log.info("RocketMQ topic {} doesn't exist. Creating it ...", fullTopicName);
            try {
                adminClient.topics().createPartitionedTopic(fullTopicName, tc.getWriteQueueNums());
                adminClient.lookups().lookupPartitionedTopic(fullTopicName);
                currentPartitionNum = tc.getWriteQueueNums();
            } catch (PulsarAdminException e) {
                log.warn("create or get partitioned topic metadata [{}] error: ", fullTopicName, e);
                throw new RuntimeException("Create topic error.");
            }

            Map<String, List<Integer>> topicRouteMap = clusterBrokers.createTopicRouteMap(currentPartitionNum);
            // Create or update zk topic node
            RopTopicContent topicContent;
            try {
                topicContent = zkService.getTopicContent(topicName);
                topicContent.setConfig(tc);
                topicContent.setRouteMap(topicRouteMap);
                zkService.setTopicContent(topicName, topicContent);
            } catch (Exception e) {
                topicContent = new RopTopicContent();
                topicContent.setConfig(tc);
                topicContent.setRouteMap(topicRouteMap);
                try {
                    zkService.createTopicContent(topicName, topicContent);
                } catch (Exception exception) {
                    log.error("createTopicContent [{}] error. caused by: {}", topicName, exception);
                    throw new RuntimeException("createTopicContent error.");
                }
            }
        } else {
            log.info("RocketMQ topic {} has exist. Updating it ...", fullTopicName);

            // expand partition
            try {
                if (currentPartitionNum < tc.getWriteQueueNums()) {
                    log.info("Expend partition for topic [{}], from [{}] to [{}].", fullTopicName, currentPartitionNum,
                            tc.getWriteQueueNums());
                    adminClient.topics().updatePartitionedTopic(fullTopicName, tc.getWriteQueueNums());
                    currentPartitionNum = tc.getWriteQueueNums();
                }
            } catch (PulsarAdminException e) {
                log.warn("Expend partitioned for topic [{}] error.", fullTopicName, e);
                throw new RuntimeException("Update topic error.");
            }

            // partition can not be shrink
            if (currentPartitionNum > tc.getWriteQueueNums()) {
                log.info("Rop not allow shrink queue number, current: [{}] target: [{}].", currentPartitionNum,
                        tc.getWriteQueueNums());
                tc.setWriteQueueNums(currentPartitionNum);
                tc.setReadQueueNums(currentPartitionNum);
            }
            Map<String, List<Integer>> topicRouteMap = clusterBrokers.createTopicRouteMap(currentPartitionNum);
            // Create or update zk topic node
            RopTopicContent topicContent;
            try {
                topicContent = zkService.getTopicContent(topicName);
                topicContent.setConfig(tc);
                topicContent.setRouteMap(topicRouteMap);
                zkService.setTopicContent(topicName, topicContent);
            } catch (Exception e) {
                topicContent = new RopTopicContent();
                topicContent.setConfig(tc);
                topicContent.setRouteMap(topicRouteMap);
                try {
                    zkService.createTopicContent(topicName, topicContent);
                } catch (Exception exception) {
                    log.error("createTopicContent [{}] error. caused by: {}", topicName, exception);
                    throw new RuntimeException("createTopicContent error.");
                }
            }
        }

        updateTopicConfig(tc);
    }

    /**
     * delete pulsar topic.
     *
     * @param topic rop topic name
     */
    public void deleteTopic(final String topic) {
        String fullTopicName = RocketMQTopic.getPulsarOrigNoDomainTopic(topic);
        log.info("Delete topic [{}].", fullTopicName);

        try {
            //delete metadata from pulsar system
            PartitionedTopicMetadata topicMetadata = adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
            if (topicMetadata.partitions > 0) {
                adminClient.topics().deletePartitionedTopic(fullTopicName, true);
            }

            //delete metadata from zookeeper
            String topicNodePath = String
                    .format(RopZkUtils.TOPIC_BASE_PATH_MATCH,
                            PulsarUtil.getNoDomainTopic(TopicName.get(fullTopicName)));
            zkService.deleteFullPath(topicNodePath);
        } catch (Exception e) {
            log.warn("[DELETE] Topic {} create or update partition failed", fullTopicName, e);
        }

    }

    @Getter
    private final ConcurrentHashMap<String, Producer> references = new ConcurrentHashMap<>();

    public Producer getReferenceProducer(String topicName, PersistentTopic persistentTopic,
            RopServerCnx ropServerCnx) {

        return references.computeIfAbsent(topicName, new Function<String, Producer>() {
            @Nullable
            @Override
            public Producer apply(@Nullable String s) {
                try {
                    return registerInPersistentTopic(persistentTopic, ropServerCnx);
                } catch (Exception e) {
                    log.warn("Register producer in persistentTopic [{}] failed", topicName, e);
                }
                return null;
            }
        });
    }

    private Producer registerInPersistentTopic(PersistentTopic persistentTopic, RopServerCnx ropServerCnx)
            throws Exception {
        Producer producer = new InternalProducer(persistentTopic, new InternalServerCnx(ropServerCnx),
                ((PulsarClientImpl) (pulsarService.getClient())).newRequestId(),
                brokerService.generateUniqueProducerName(), Collections.singletonMap("Type", "ROP"));

        // this will register and add USAGE_COUNT_UPDATER.
        persistentTopic.addProducer(producer, new CompletableFuture<>());
        return producer;
    }

    private void removeReferenceProducer(String topicName) {
        Producer producer = references.remove(topicName);
        if (producer != null) {
            try {
                producer.close(true);
            } catch (Exception e) {
                log.warn("The [{}] producer close failed", topicName, e);
            }
        }
    }

    private boolean isTBW12Topic(TopicName topicName) {
        return MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC.equals(topicName.getLocalName());
    }

    //create RoP cluster broker list
    public CompletableFuture<Optional<RopClusterContent>> createOrUpdateClusterConfig(
            RopClusterContent clusterContent) {
        CompletableFuture<Optional<RopClusterContent>> future = new CompletableFuture<>();
        if (clusterContent == null) {
            future.completeExceptionally(
                    new IllegalArgumentException("Invalid parameters for createOrUpdateClusterConfig"));
            return future;
        }
        String clusterZNodePath = RopZkUtils.getClusterZNodePath();
        zkService.getClusterDataCache().getAsync(clusterZNodePath).thenAccept(clusterInfo -> {
            if (clusterInfo.isPresent()) {
                RopClusterContent oldClusterContent = clusterInfo.get();
                if (clusterContent.equals(oldClusterContent)) {
                    future.complete(clusterInfo);
                } else {
                    try {
                        zkService.setJsonObjectForPath(clusterZNodePath, clusterContent);
                        zkService.getClusterDataCache().reloadCache(clusterZNodePath);
                        future.complete(clusterInfo);
                    } catch (Exception e) {
                        log.warn("fail to createOrUpdateClusterConfig. caused by: ", e);
                        throw new RuntimeException("setJsonObjectForPath error, path: " + clusterZNodePath);
                    }
                }
            } else {
                try {
                    zkService.createFullPathWithJsonObject(clusterZNodePath, clusterContent);
                } catch (Exception e) {
                    log.warn("fail to createOrUpdateClusterConfig. caused by: ", e);
                    throw new RuntimeException("createFullPathWithJsonObject error, path: " + clusterZNodePath);
                }
            }
            future.complete(Optional.of(clusterContent));
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });
        return future;
    }
}
