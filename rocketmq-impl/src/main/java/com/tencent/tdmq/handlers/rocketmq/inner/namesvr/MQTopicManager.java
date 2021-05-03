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

package com.tencent.tdmq.handlers.rocketmq.inner.namesvr;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.tencent.tdmq.handlers.rocketmq.RocketMQProtocolHandler;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.inner.producer.ClientTopicName;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.rocketmq.common.TopicConfig;

/**
 * MQ topic manager.
 */
@Slf4j
public class MQTopicManager extends TopicConfigManager implements NamespaceBundleOwnershipListener {

    private final int maxCacheSize = 1024;
    private final int maxCacheTimeInSec = 120;
    //cache-key TopicName = {tenant/ns/topic}, Map key={partition id} nonPartitionedTopic, only one record in map.
    @Getter
    private final Cache<TopicName, Map<Integer, InetSocketAddress>> lookupCache = CacheBuilder
            .newBuilder()
            .initialCapacity(maxCacheSize)
            .expireAfterWrite(maxCacheTimeInSec, TimeUnit.SECONDS)
            .build();
    private final int servicePort;
    private PulsarService pulsarService;
    private BrokerService brokerService;
    private PulsarAdmin adminClient;

    public MQTopicManager(RocketMQBrokerController brokerController) {
        super(brokerController);
        this.servicePort = RocketMQProtocolHandler.getListenerPort(config.getRocketmqListeners());
    }

    @Override
    protected void createPulsarPartitionedTopic(TopicConfig tc) {
        String cluster = config.getClusterName();
        TopicName rmqTopic = TopicName.get(tc.getTopicName());
        String tenant = rmqTopic.getTenant();
        String ns = rmqTopic.getNamespacePortion();
        tenant = Strings.isBlank(tenant) ? config.getRocketmqTenant() : tenant;
        ns = Strings.isBlank(ns) ? config.getRocketmqNamespace() : ns;

        try {
            createPulsarNamespaceIfNeeded(brokerService, cluster, tenant, ns);
            createPulsarTopic(tc);
        } catch (Exception e) {
            log.warn("createPulsarPartitionedTopic tenant=[{}] and namespace=[{}] error.", tenant, ns);
        }
    }

    public void start() throws Exception {
        this.pulsarService = brokerController.getBrokerService().pulsar();
        this.brokerService = pulsarService.getBrokerService();
        this.adminClient = this.pulsarService.getAdminClient();
        this.createSysResource();
        this.pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(this);
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
    }

    // call pulsarClient.lookup.getBroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public Map<Integer, InetSocketAddress> getTopicBrokerAddr(TopicName topicName) {
        if (lookupCache.getIfPresent(topicName) != null) {
            return lookupCache.getIfPresent(topicName);
        }

        Map<Integer, InetSocketAddress> partitionedTopicAddr = new HashMap<>();
        try {
            //String lookupName = TopicNameUtils.getNoDomainTopicName(topicName);
            //adminClient.lookups().lookupTopic(lookupName);
            PartitionedTopicMetadata pTopicMeta = adminClient.topics()
                    .getPartitionedTopicMetadata(topicName.toString());
            if (pTopicMeta.partitions > 0) {
                IntStream.range(0, pTopicMeta.partitions).forEach((i) -> {
                    try {
                        Backoff backoff = new Backoff(
                                100, TimeUnit.MILLISECONDS,
                                15, TimeUnit.SECONDS,
                                15, TimeUnit.SECONDS
                        );
                        CompletableFuture<InetSocketAddress> resultFuture = new CompletableFuture<>();
                        lookupBroker(topicName.getPartition(i), backoff, resultFuture);
                        partitionedTopicAddr.put(i, resultFuture.get());
                    } catch (Exception e) {
                        log.warn("getTopicBrokerAddr error.", e);
                    }
                });
            }
            if (!partitionedTopicAddr.isEmpty()) {
                lookupCache.put(topicName, partitionedTopicAddr);
                putPulsarTopic2Config(topicName, partitionedTopicAddr.size());
            }
        } catch (Exception e) {
            log.warn("getTopicBroker info error for the topic[{}].", topicName);
        }
        return partitionedTopicAddr;
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    private void lookupBroker(TopicName topicName, Backoff backoff,
            CompletableFuture<InetSocketAddress> retFuture) {
        try {
            ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                    .getBroker(topicName)
                    .thenAccept(pair -> {
                        checkState(pair.getLeft().equals(pair.getRight()));
                        retFuture.complete(pair.getLeft());
                    }).exceptionally(th -> {
                long waitTimeMs = backoff.next();
                if (backoff.isMandatoryStopMade()) {
                    log.warn("getBroker for topic {} failed, retried too many times {}, return null."
                            + " throwable: ", topicName, waitTimeMs, th);
                    retFuture.complete(null);
                } else {
                    log.warn("getBroker for topic [{}] failed, will retry in [{}] ms. throwable: ",
                            topicName, waitTimeMs, th);
                    pulsarService.getExecutor().schedule(() -> lookupBroker(topicName, backoff, retFuture),
                            waitTimeMs,
                            TimeUnit.MILLISECONDS);
                }
                return null;
            });
        } catch (PulsarServerException e) {
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
                        log.info("get owned topic list when onLoad bundle {}, topic size {} ", bundle, topics.size());
                        try {
                            for (String topic : topics) {
                                log.info("NamespaceBundle onLoad topic = [{}].", topic);
                                TopicName name = TopicName.get(TopicName.get(topic).getPartitionedTopicName());
                                getTopicBrokerAddr(name);
                            }
                        } catch (Exception e) {
                            log.warn("NamespaceBundle loading broker address error. topics=[{}].", topics);
                        }
                        try {
                            loadPersistentTopic(topics);
                        } catch (Exception e) {
                            log.warn("NamespaceBundle loading persistent topic error. topics=[{}].", topics);
                        }
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
                    if (throwable != null) {
                        log.warn("getTopicIfExists error, topic=[{}].", topic);
                        return;
                    }
                    if (t2.isPresent()) {
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
                        log.info("get owned topic list when unLoad bundle {}, topic size {} ", bundle,
                                topics.size());
                        for (String topic : topics) {
                            TopicName partitionedTopic = TopicName.get(topic);
                            ClientTopicName clientTopicName = new ClientTopicName(
                                    partitionedTopic.getPartitionedTopicName());
                            this.brokerController.getConsumerOffsetManager()
                                    .removePulsarTopic(clientTopicName, partitionedTopic.getPartitionIndex());
                        }
                    } else {
                        log.error("Failed to get owned topic list for "
                                        + "OffsetAndTopicListener when triggering un-loading bundle {}.",
                                bundle, ex);
                    }
                });
    }

    @Override
    public boolean test(NamespaceBundle namespaceBundle) {
        return true;
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
        this.topicConfigTable.values().forEach(this::createPulsarTopic);
    }

    private void lookupTopics(TopicConfig tc) {
        lookupTopics(tc.getTopicName());
    }

    // tenant/ns/topicName
    public void lookupTopics(String pulsarTopicName) {
        try {
            adminClient.lookups().lookupTopicAsync(pulsarTopicName)
                    .whenComplete((serviceUrl, throwable1) -> {
                        if (throwable1 != null) {
                            log.warn("lookupTopicAsync topic[{}] exception.", pulsarTopicName);
                            return;
                        }
                        log.info("lookupTopics topic[{}] successfully.", pulsarTopicName);
                    }).get();
        } catch (Exception e) {
            log.error("lookupTopics pulsar topic=[{}] error.", pulsarTopicName, e);
        }
    }

    private void createPulsarTopic(TopicConfig tc) {
        String fullTopicName = tc.getTopicName();
        try {
            PartitionedTopicMetadata pTopicMeta = adminClient.topics()
                    .getPartitionedTopicMetadata(fullTopicName);
            if (pTopicMeta.partitions <= 0) {
                log.info("RocketMQ metadata topic {} doesn't exist. Creating it ...", fullTopicName);
                adminClient.topics().createPartitionedTopic(
                        fullTopicName,
                        tc.getWriteQueueNums());
                for (int i = 0; i < tc.getWriteQueueNums(); i++) {
                    adminClient.topics()
                            .createNonPartitionedTopic(
                                    fullTopicName + PARTITIONED_TOPIC_SUFFIX + i);
                }
            }
            lookupTopics(tc);
        } catch (Exception e) {
            log.error("createPulsarTopic topic=[{}] error.", tc, e);
        }
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
        String cluster = config.getClusterName();
        String fullTopicName = RocketMQTopic.getPulsarOrigNoDomainTopic(tc.getTopicName());
        TopicName rmqTopic = TopicName.get(fullTopicName);
        String tenant = rmqTopic.getTenant();
        String ns = rmqTopic.getNamespacePortion();
        tenant = Strings.isBlank(tenant) ? config.getRocketmqTenant() : tenant;

        try {
            createPulsarNamespaceIfNeeded(brokerService, cluster, tenant, ns);
        } catch (Exception e) {
            log.warn("createPulsarPartitionedTopic tenant=[{}] and namespace=[{}] error.", tenant, ns);
        }

        try {
            PartitionedTopicMetadata topicMetadata = adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
            // 如果分区小于0，主题不存在，创建主题
            if (topicMetadata.partitions <= 0) {
                log.info("RocketMQ metadata topic {} doesn't exist. Creating it ...", fullTopicName);
                adminClient.topics().createPartitionedTopic(
                        fullTopicName,
                        tc.getWriteQueueNums());
//                for (int i = 0; i < tc.getWriteQueueNums(); i++) {
//                    adminClient.topics().createNonPartitionedTopic(fullTopicName + PARTITIONED_TOPIC_SUFFIX + i);
//                }
                lookupTopics(tc);
            } else if (topicMetadata.partitions < tc.getWriteQueueNums()) {
                adminClient.topics().updatePartitionedTopic(fullTopicName, tc.getWriteQueueNums());
//                for (int i = topicMetadata.partitions; i < tc.getWriteQueueNums(); i++) {
//                    adminClient.topics().createNonPartitionedTopic(fullTopicName + PARTITIONED_TOPIC_SUFFIX + i);
//                }
                lookupTopics(tc);
            }
        } catch (Exception e) {
            log.warn("Topic {} create or update partition failed", fullTopicName, e);
        }
    }

    /**
     * delete pulsar topic.
     *
     * @param topic rop topic name
     */
    public void deleteTopic(final String topic) {
        String fullTopicName = RocketMQTopic.getPulsarOrigNoDomainTopic(topic);
        try {
            // 删除分区主题源主题，否者再次创建的主题的时候会查询到分区主题的一些元数据
            adminClient.topics().deletePartitionedTopic(fullTopicName, true);
        } catch (Exception e) {
            log.warn("Topic {} create or update partition failed", fullTopicName, e);
        }

    }

}
