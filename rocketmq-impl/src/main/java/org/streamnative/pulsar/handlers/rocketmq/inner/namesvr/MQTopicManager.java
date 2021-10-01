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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.rocketmq.broker.topic.TopicValidator;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
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

    private final int topicOwnedBrokerCacheInitSz = 1024;
    private final int topicOwnedBrokerCacheSz = 10240;
    private final int topicOwnedBrokerExpireMs = 60 * 1000;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final Map<String, PulsarClient> pulsarClientMap = Maps.newConcurrentMap();
    private final Cache<TopicName, String> ownedBrokerAddrCache = CacheBuilder.newBuilder()
            .initialCapacity(topicOwnedBrokerCacheInitSz)
            .maximumSize(topicOwnedBrokerCacheSz)
            .expireAfterWrite(topicOwnedBrokerExpireMs, TimeUnit.MILLISECONDS)
            .build();

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
        TopicConfig topicConfig = super.selectTopicConfig(rmqTopicName);
        try {
            if (topicConfig == null) {
                RopTopicContent ropTopicContent = zkService.getTopicContent(TopicName.get(rmqTopicName));
                if (ropTopicContent != null) {
                    topicConfig = ropTopicContent.getConfig();
                    this.topicConfigTable.put(rmqTopicName, topicConfig);
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

    public void getTopicBrokerAddr(TopicName topicName) {
        //TODO:getTopicRoute(topicName, Strings.EMPTY);
    }

    public Map<String, List<Integer>> getPulsarTopicRoute(TopicName topicName, String listenerName) {
        try {
            String topicConfigKey = joinPath(topicName.getNamespace(), topicName.getLocalName());
            if (isTBW12Topic(topicName) && this.topicConfigTable.containsKey(topicConfigKey)) {
                RopClusterContent clusterContent = zkService.getClusterContent();
                TopicConfig topicConfig = topicConfigTable.get(topicConfigKey);
                int writeQueueNums = topicConfig.getWriteQueueNums();
                return clusterContent.createTopicRouteMap(writeQueueNums);
            } else {
                RopTopicContent ropTopicContent = zkService.getTopicContent(topicName);
                Preconditions
                        .checkNotNull(ropTopicContent, "RopTopicContent[" + topicName.toString() + "] can't be null.");

                this.topicConfigTable.put(topicConfigKey, ropTopicContent.getConfig());
                return ropTopicContent.getRouteMap();
            }
        } catch (Exception e) {
            log.warn("[{}] Get topic route error.", topicName.toString(), e);
        }
        return Maps.newHashMap();
    }

    // call pulsarClient.lookup.getBroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public void getTopicBrokerAddr(TopicName topicName, String listenerName) {
        try {
            PartitionedTopicMetadata pTopicMeta = brokerService.fetchPartitionedTopicMetadataAsync(topicName).get();
            if (pTopicMeta.partitions > 0) {
                IntStream.range(0, pTopicMeta.partitions).forEach((i) -> {
                    try {
                        Backoff backoff = new Backoff(
                                100, TimeUnit.MILLISECONDS,
                                15, TimeUnit.SECONDS,
                                15, TimeUnit.SECONDS
                        );
                        CompletableFuture<InetSocketAddress> resultFuture = new CompletableFuture<>();
                        lookupBroker(topicName.getPartition(i), backoff, listenerName, resultFuture);
                    } catch (Exception e) {
                        log.warn("getTopicBrokerAddr error.", e);
                    }
                });
//                putPulsarTopic2Config(topicName, pTopicMeta.partitions);
            }
        } catch (Exception e) {
            log.warn("getTopicBroker info error for the topic[{}].", topicName, e);
        }
    }

    /**
     * If current broker is this partition topic owner return true else return false.
     *
     * @param topicName topic name
     * @param partitionId queue id
     * @return if current broker is this partition topic owner return true else return false.
     */
    public boolean isPartitionTopicOwner(TopicName topicName, int partitionId) {
        return this.brokerController.getBrokerService().isTopicNsOwnedByBroker(topicName.getPartition(partitionId));
    }

    /**
     * Get pulsar persistent topic.
     *
     * @param topicName topic name
     * @return persistent topic
     */
    public PersistentTopic getPulsarPersistentTopic(String topicName) {
        PulsarService pulsarService = this.brokerController.getBrokerService().pulsar();
        Optional<Topic> topic = pulsarService.getBrokerService().getTopicIfExists(topicName).join();
        if (topic.isPresent()) {
            return (PersistentTopic) topic.get();
        } else {
            log.warn("Not found pulsar persistentTopic [{}]", topicName);
        }
        return null;
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    private void lookupBroker(TopicName topicName, Backoff backoff, String listenerName,
            CompletableFuture<InetSocketAddress> retFuture) {
        try {
            PulsarClient pulsarClient =
                    StringUtils.isBlank(listenerName) ? pulsarService.getClient() : getClient(listenerName);
            ((PulsarClientImpl) pulsarClient).getLookup()
                    .getBroker(topicName)
                    .thenAccept(pair -> retFuture.complete(pair.getLeft())).exceptionally(th -> {
                long waitTimeMs = backoff.next();
                if (backoff.isMandatoryStopMade()) {
                    log.warn("getBroker for topic {} failed, retried too many times {}, return null.",
                            topicName, waitTimeMs, th);
                    retFuture.complete(null);
                } else {
                    log.warn("getBroker for topic [{}] failed, will retry in [{}] ms.",
                            topicName, waitTimeMs, th);
                    pulsarService.getExecutor()
                            .schedule(() -> lookupBroker(topicName, backoff, listenerName, retFuture),
                                    waitTimeMs,
                                    TimeUnit.MILLISECONDS);
                }
                return null;
            });
        } catch (Exception e) {
            log.error("getTopicBroker for topic {} failed get pulsar client, return null. throwable: ",
                    topicName, e);
            retFuture.complete(null);
        }
    }

    public String lookupBroker(TopicName topicName, String listenerName) throws Exception {
        String addr = ownedBrokerAddrCache.getIfPresent(topicName);
        if (Strings.isBlank(addr)) {
            PulsarClient pulsarClient =
                    StringUtils.isBlank(listenerName) ? pulsarService.getClient() : getClient(listenerName);
            Pair<InetSocketAddress, InetSocketAddress> lookupResult = ((PulsarClientImpl) pulsarClient).getLookup()
                    .getBroker(topicName).join();
            ownedBrokerAddrCache.put(topicName, lookupResult.getLeft().getHostString());
        }
        return ownedBrokerAddrCache.getIfPresent(topicName);
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
                        new RetentionPolicies(-1, -1));
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
        try {
            log.info("Create or update inner topic config: [{}].", tc);
            String fullTopicName = tc.getTopicName();
            PartitionedTopicMetadata topicMetadata = adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
            if (tc.getWriteQueueNums() > topicMetadata.partitions) {
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

        PartitionedTopicMetadata topicMetadata;
        try {
            topicMetadata = adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
        } catch (PulsarAdminException e) {
            log.warn("get partitioned topic metadata error", e);
            throw new NullPointerException(String.format("Get partitioned topic[%s] metadata error.", fullTopicName));
        }

        int currentPartitionNum = topicMetadata.partitions;

        // get cluster brokers list
        RopClusterContent clusterBrokers;
        try {
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
                tc.setWriteQueueNums(currentPartitionNum);
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

    private InetSocketAddress lookupTopic(String pulsarTopicName) {
        try {
            return ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                    .getBroker(TopicName.get(pulsarTopicName))
                    .get()
                    .getLeft();
        } catch (Exception e) {
            log.error("LookupTopics pulsar topic=[{}] error.", pulsarTopicName, e);
        }
        return null;
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

    private synchronized PulsarClient getClient(String listenerName) {
        if (pulsarClientMap.get(listenerName) == null || pulsarClientMap.get(listenerName).isClosed()) {
            try {
                ClientBuilder builder =
                        PulsarClient.builder().serviceUrl(this.brokerService.getPulsar().getBrokerServiceUrl());
                if (StringUtils.isNotBlank(this.config.getBrokerClientAuthenticationPlugin())) {
                    builder.authentication(
                            this.config.getBrokerClientAuthenticationPlugin(),
                            this.config.getBrokerClientAuthenticationParameters()
                    );
                }
                if (StringUtils.isNotBlank(listenerName)) {
                    builder.listenerName(listenerName);
                }
                pulsarClientMap.put(listenerName, builder.build());
            } catch (Exception e) {
                log.error("listenerName [{}] getClient error", listenerName, e);
            }
        }
        return pulsarClientMap.get(listenerName);
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

    protected void msgCheck(final ChannelHandlerContext ctx,
            final SendMessageRequestHeader requestHeader, final RemotingCommand response) {
        if (!PermName.isWriteable(this.brokerController.getServerConfig().getBrokerPermission())
                && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" //+ this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] sending message is forbidden");
            return;
        }

        if (!TopicValidator.validateTopic(requestHeader.getTopic(), response)) {
            return;
        }

        TopicConfig topicConfig = selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                } else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            log.warn("the topic {} not exist, producer: {}", requestHeader.getTopic(), ctx.channel().remoteAddress());
            topicConfig = createTopicInSendMessageMethod(
                    requestHeader.getTopic(),
                    requestHeader.getDefaultTopic(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    requestHeader.getDefaultTopicQueueNums(), topicSysFlag);

            if (null == topicConfig) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig = createTopicInSendMessageBackMethod(
                            requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ,
                            topicSysFlag);
                }
            }

            if (null == topicConfig) {
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                        + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return;
            }
        }

        int queueIdInt = requestHeader.getQueueId();
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        if (queueIdInt >= idValid) {
            String errorInfo = String.format("request queueId[%d] is illegal, %s Producer: %s",
                    queueIdInt,
                    topicConfig.toString(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);

        }
    }
}
