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

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.SLASH_CHAR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.Backoff;
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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.streamnative.pulsar.handlers.rocketmq.inner.InternalProducer;
import org.streamnative.pulsar.handlers.rocketmq.inner.InternalServerCnx;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RopServerCnx;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopTopicContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath;
import org.streamnative.pulsar.handlers.rocketmq.utils.PulsarUtil;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;
import org.streamnative.pulsar.handlers.rocketmq.utils.ZookeeperUtils;
import org.testng.collections.Lists;
import org.testng.collections.Maps;

/**
 * MQ topic manager.
 */
@Slf4j
public class MQTopicManager extends TopicConfigManager implements NamespaceBundleOwnershipListener {

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final Map<String, PulsarClient> pulsarClientMap = Maps.newConcurrentMap();
    private PulsarService pulsarService;
    private BrokerService brokerService;
    private PulsarAdmin adminClient;

    /**
     * group offset reader executor.
     */
    private final ExecutorService routeChangeReaderExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("Rop-routeChange-reader");
        t.setDaemon(true);
        return t;
    });
    private Reader<String> routeChangeReader;

    public MQTopicManager(RocketMQBrokerController brokerController) {
        super(brokerController);
    }

    @Override
    protected void createPulsarPartitionedTopic(TopicConfig tc) {
        createOrUpdateTopic(tc);
    }

    public void start() throws Exception {
        this.pulsarService = brokerController.getBrokerService().pulsar();
        this.brokerService = pulsarService.getBrokerService();
        this.adminClient = pulsarService.getAdminClient();
        this.zkClient = pulsarService.getZkClient();
        this.createSysResource();
        this.pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(this);
        log.info("MQTopicManager started successfully.");

        this.routeChangeReader = brokerController.getBrokerService().getPulsar().getClient()
                .newReader(Schema.STRING)
                .topic(RocketMQTopic.getTopicRouteChangeTopic().getPulsarFullName())
                .startMessageId(MessageId.latest)
                .create();
        routeChangeReaderExecutor.execute(this::loadRouteChange);
    }

    private void loadRouteChange() {
        log.info("Start load route change ...");
        while (!shuttingDown.get()) {
            try {
                Message<String> message = routeChangeReader.readNext(1, TimeUnit.SECONDS);
                if (message == null) {
                    continue;
                }

                TopicName topicName = TopicName.get(message.getValue());
                topicTableCache.invalidate(topicName);
            } catch (Exception e) {
                log.warn("Rop load route change failed.", e);
            }
        }
    }

    @Override
    public TopicConfig selectTopicConfig(String rmqTopicName) {
        TopicConfig topicConfig = super.selectTopicConfig(rmqTopicName);
        if (topicConfig == null) {
            //load from pulsar server
            String lookupTopic = RocketMQTopic.getPulsarOrigNoDomainTopic(rmqTopicName);
            getTopicBrokerAddr(TopicName.get(lookupTopic));
        }
        return super.selectTopicConfig(rmqTopicName);
    }

    public void shutdown() {
        shuttingDown.set(true);
        routeChangeReaderExecutor.shutdownNow();
        cleanUpExecutor.shutdownNow();
    }

    public void getTopicBrokerAddr(TopicName topicName) {
        getTopicRoute(topicName, Strings.EMPTY);
    }

    public int getPulsarPartition(TopicName topicName, int queueId) {
        Map<String, List<Integer>> topicRoute = getTopicRoute(topicName, Strings.EMPTY);
        List<Integer> partitions = topicRoute.get(brokerController.getBrokerHost());
        if (partitions == null || queueId >= partitions.size()) {
            throw new RuntimeException("Not found queueId.");
        }
        return partitions.get(queueId);
    }


    public Map<String, List<Integer>> getTopicRoute(TopicName topicName, String listenerName) {
        if (isTBW12Topic(topicName)) {
            if (brokerController.getServerConfig().isAutoCreateTopicEnable()) {
                List<Integer> partitions = Lists.newArrayList();
                for (int i = 0; i < brokerController.getServerConfig().getDefaultTopicQueueNums(); i++) {
                    partitions.add(i);
                }
                return Collections.singletonMap(brokerController.getBrokerHost(), partitions);
            } else {
                return Maps.newHashMap();
            }
        }

        RopTopicContent ropTopicContent = topicTableCache.getIfPresent(topicName);
        if (ropTopicContent != null) {
            return ropTopicContent.getRouteMap();
        }

        // trigger topic lookup
        getTopicBrokerAddr(topicName, listenerName);

        try {
            String topicNodePath = String.format(RopZkPath.TOPIC_BASE_PATH_MATCH,
                    PulsarUtil.getNoDomainTopic(topicName));

            byte[] content = zkClient.getData(topicNodePath, null, null);
            ropTopicContent = jsonMapper.readValue(content, RopTopicContent.class);
            topicTableCache.put(topicName, ropTopicContent);

            String pulsarTopicName = Joiner.on(SLASH_CHAR).join(topicName.getNamespace(), topicName.getLocalName());
            this.topicConfigTable.put(pulsarTopicName, ropTopicContent.getConfig());

            return ropTopicContent.getRouteMap();
        } catch (KeeperException.NoNodeException e) {
            log.info("[{}] Not found topic from zk.", topicName.toString());
        } catch (Exception e) {
            log.error("[{}] Get topic route error.", topicName.toString(), e);
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
     * @param queueId queue id
     * @return if current broker is this partition topic owner return true else return false.
     */
    public boolean isPartitionTopicOwner(TopicName topicName, int queueId) {
        return this.brokerController.getBrokerService().isTopicNsOwnedByBroker(topicName.getPartition(queueId));
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
        this.topicConfigTable.values().forEach(this::createOrUpdateTopic);
    }

    private void createPulsarNamespaceIfNeeded(BrokerService service, String cluster, String tenant, String ns)
            throws Exception {
        String fullNs = Joiner.on('/').join(tenant, ns);
        try {
            ClusterData clusterData = new ClusterData(service.pulsar().getWebServiceAddress(),
                    null /* serviceUrlTls */,
                    service.pulsar().getBrokerServiceUrl(),
                    null /* brokerServiceUrlTls */);
            if (!adminClient.clusters().getClusters().contains(cluster)) {
                adminClient.clusters().createCluster(cluster, clusterData);
            } else {
                adminClient.clusters().updateCluster(cluster, clusterData);
            }

            if (!adminClient.tenants().getTenants().contains(tenant)) {
                adminClient.tenants().createTenant(tenant,
                        new TenantInfo(Sets.newHashSet(this.config.getSuperUserRoles()), Sets.newHashSet(cluster)));
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
     * if pulsar topic not exist, create pulsar topic, else update pulsar topic partition.
     *
     * @param tc topic config
     */
    public void createOrUpdateTopic(final TopicConfig tc) {
        String fullTopicName = tc.getTopicName();
        log.info("Create or update topic [{}], config: [{}].", tc.getTopicName(), tc);

        TopicName topicName = TopicName.get(fullTopicName);
        String tenant = topicName.getTenant();
        String ns = topicName.getNamespacePortion();
        tenant = Strings.isBlank(tenant) ? config.getRocketmqTenant() : tenant;

        try {
            if (brokerController.getServerConfig().isAutoCreateTopicEnable()) {
                createPulsarNamespaceIfNeeded(brokerService, config.getClusterName(), tenant, ns);
            }
        } catch (Exception e) {
            log.warn("CreatePulsarPartitionedTopic tenant=[{}] and namespace=[{}] error.", tenant, ns);
            throw new RuntimeException("Create topic error.");
        }

        PartitionedTopicMetadata topicMetadata = null;
        try {
            topicMetadata = adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
        } catch (PulsarAdminException e) {
            log.warn("get partitioned topic metadata error", e);
        }
        if (topicMetadata == null) {
            throw new RuntimeException("Get partitioned topic metadata error");
        }

        int currentPartitionNum = topicMetadata.partitions;

        // If the partition < 0, the topic does not exist, let us to create topic.
        if (currentPartitionNum <= 0) {
            log.info("RocketMQ topic {} doesn't exist. Creating it ...", fullTopicName);
            try {
                adminClient.topics().createPartitionedTopic(fullTopicName, tc.getWriteQueueNums());
                currentPartitionNum = adminClient.topics().getPartitionedTopicMetadata(fullTopicName).partitions;
            } catch (PulsarAdminException e) {
                log.warn("create or get partitioned topic metadata [{}] error: ", fullTopicName, e);
                throw new RuntimeException("Create topic error.");
            }

            // Build the routing table
            Map<String, List<Integer>> routeMap = Maps.newHashMap();
            for (int i = 0; i < currentPartitionNum; i++) {
                InetSocketAddress brokerAddress = lookupTopic(fullTopicName + PARTITIONED_TOPIC_SUFFIX + i);
                if (brokerAddress == null) {
                    throw new RuntimeException("Create topic error.");
                }
                String brokerIp = brokerAddress.getHostName();
                List<Integer> partitions = routeMap
                        .computeIfAbsent(brokerIp, (Function<String, List<Integer>>) s -> Lists.newLinkedList());
                partitions.add(i);
            }

            // Create or update zk topic node
            String topicNodePath = String
                    .format(RopZkPath.TOPIC_BASE_PATH_MATCH, PulsarUtil.getNoDomainTopic(topicName));

            try {
                byte[] content = zkClient.getData(topicNodePath, null, null);
                RopTopicContent ropTopicContent = jsonMapper.readValue(content, RopTopicContent.class);
                ropTopicContent.setConfig(tc);
                ropTopicContent.setRouteMap(routeMap);
                content = jsonMapper.writeValueAsBytes(ropTopicContent);
                zkClient.setData(topicNodePath, content, -1);
            } catch (KeeperException.NoNodeException e) {
                // Create tenant node if not exist
                String tenantNodePath = String.format(RopZkPath.TOPIC_BASE_PATH_MATCH, tenant);
                ZookeeperUtils.createPersistentNodeIfNotExist(zkClient, tenantNodePath);

                // Create namespaces node if not exist
                String nsNodePath = String.format(RopZkPath.TOPIC_BASE_PATH_MATCH, topicName.getNamespace());
                ZookeeperUtils.createPersistentNodeIfNotExist(zkClient, nsNodePath);

                RopTopicContent newRopTopicContent = new RopTopicContent(tc, routeMap);
                try {
                    byte[] newContent = jsonMapper.writeValueAsBytes(newRopTopicContent);
                    zkClient.create(topicNodePath, newContent, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException | InterruptedException | JsonProcessingException ex) {
                    log.warn("create topic path [{}] error", topicNodePath, ex);
                    throw new RuntimeException("Create topic error.");
                }
            } catch (IOException | KeeperException | InterruptedException e) {
                log.warn("Create or update zk topic node error: ", e);
                throw new RuntimeException("Create or update zk topic node error.");
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

            String topicNodePath = String
                    .format(RopZkPath.TOPIC_BASE_PATH_MATCH, PulsarUtil.getNoDomainTopic(topicName));

            try {
                byte[] content = zkClient.getData(topicNodePath, null, null);
                RopTopicContent ropTopicContent = jsonMapper.readValue(content, RopTopicContent.class);
                Map<String, List<Integer>> routeMap = ropTopicContent.getRouteMap();
                Set<Integer> currentPartition = Sets.newHashSet();
                routeMap.forEach((k, v) -> currentPartition.addAll(v));

                for (int i = 0; i < currentPartitionNum; i++) {
                    if (currentPartition.contains(i)) {
                        continue;
                    }

                    InetSocketAddress brokerAddress = lookupTopic(fullTopicName + PARTITIONED_TOPIC_SUFFIX + i);
                    if (brokerAddress == null) {
                        throw new RuntimeException("Update topic error.");
                    }
                    String brokerIp = brokerAddress.getHostName();
                    List<Integer> partitions = routeMap
                            .computeIfAbsent(brokerIp, (Function<String, List<Integer>>) s -> Lists.newLinkedList());
                    partitions.add(i);
                }

                ropTopicContent.setConfig(tc);
                ropTopicContent.setRouteMap(routeMap);

                content = jsonMapper.writeValueAsBytes(ropTopicContent);
                zkClient.setData(topicNodePath, content, -1);
            } catch (KeeperException | InterruptedException | IOException e) {
                log.warn("get data from path [{}] error", topicNodePath, e);
                throw new RuntimeException("Update topic error.");
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
            PartitionedTopicMetadata topicMetadata = adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
            if (topicMetadata.partitions > 0) {
                adminClient.topics().deletePartitionedTopic(fullTopicName, true);
            }

            String topicNodePath = String
                    .format(RopZkPath.TOPIC_BASE_PATH_MATCH, PulsarUtil.getNoDomainTopic(TopicName.get(fullTopicName)));
            try {
                zkClient.delete(topicNodePath, -1);
            } catch (KeeperException.NoNodeException ignore) {
                log.info("Topic [{}] has deleted.", topic);
            }
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
        persistentTopic.addProducer(producer);
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
        return TopicName.get(RocketMQTopic.getPulsarMetaNoDomainTopic(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC))
                .equals(topicName);
    }

}
