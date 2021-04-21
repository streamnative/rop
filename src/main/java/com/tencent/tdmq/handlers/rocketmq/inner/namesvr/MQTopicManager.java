/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.tencent.tdmq.handlers.rocketmq.utils.TopicNameUtils;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.rocketmq.common.TopicConfig;

@Slf4j
public class MQTopicManager extends TopicConfigManager implements NamespaceBundleOwnershipListener {

    private final int MAX_CACHE_SIZE = 10000;
    private final int MAX_CACHE_TIME_IN_SEC = 120;
    //cache-key TopicName = {tenant/ns/topic}, Map key={partition id} nonPartitionedTopic, only one record in map.
    public final Cache<TopicName, Map<Integer, InetSocketAddress>> lookupCache = CacheBuilder
            .newBuilder()
            .initialCapacity(MAX_CACHE_SIZE).maximumSize(MAX_CACHE_SIZE)
            .expireAfterWrite(MAX_CACHE_TIME_IN_SEC, TimeUnit.SECONDS)
            .build();
    private final int servicePort;
    private PulsarService pulsarService;
    private BrokerService brokerService;
    private PulsarAdmin adminClient;
    private NamespaceName rocketmqMetaNs;
    private NamespaceName rocketmqTopicNs;

    public MQTopicManager(RocketMQBrokerController brokerController) {
        super(brokerController);
        this.servicePort = RocketMQProtocolHandler.getListenerPort(config.getRocketmqListeners());
        this.rocketmqMetaNs = NamespaceName
                .get(config.getRocketmqMetadataTenant(), config.getRocketmqMetadataNamespace());
        this.rocketmqTopicNs = NamespaceName.get(config.getRocketmqTenant(), config.getRocketmqNamespace());
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
        } catch (Exception e) {
            log.warn("createPulsarPartitionedTopic tenant=[{}] and namespace=[{}] error.", tenant, ns);
        }
        createPulsarTopic(tc);
    }

    public void start() throws Exception {
        this.pulsarService = brokerController.getBrokerService().pulsar();
        this.brokerService = pulsarService.getBrokerService();
        this.adminClient = this.pulsarService.getAdminClient();

        // 每次启动的时候，移除 global cluster，避免 global cluster 集群创建失败
        adminClient.clusters().deleteCluster("global");
        createSysResource();
        this.pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(this);
    }

    public void shutdown() {
        this.lookupCache.cleanUp();
    }

    // call pulsarClient.lookup.getBroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public Map<Integer, InetSocketAddress> getTopicBrokerAddr(TopicName topicName) {
        if (lookupCache.getIfPresent(topicName) != null) {
            return lookupCache.getIfPresent(topicName);
        }
        Map<Integer, InetSocketAddress> partitionedTopicAddr = new HashMap<>();
        try {
            String noDomainTopicName = TopicNameUtils.getNoDomainTopicName(topicName);
            PartitionedTopicMetadata partitionedMetadata = adminClient.topics()
                    .getPartitionedTopicMetadata(noDomainTopicName);

            CompletableFuture<InetSocketAddress> resultFuture = new CompletableFuture<>();

            //non-partitioned topic
            if (partitionedMetadata.partitions <= 0) {
                Backoff backoff = new Backoff(
                        100, TimeUnit.MILLISECONDS,
                        15, TimeUnit.SECONDS,
                        15, TimeUnit.SECONDS
                );
                lookupBroker(topicName, backoff, resultFuture);
                partitionedTopicAddr.put(0, resultFuture.get());
            } else {
                IntStream.range(0, partitionedMetadata.partitions).forEach(i -> {
                    try {
                        Backoff backoff = new Backoff(
                                100, TimeUnit.MILLISECONDS,
                                15, TimeUnit.SECONDS,
                                15, TimeUnit.SECONDS
                        );
                        lookupBroker(topicName.getPartition(i), backoff, resultFuture);
                        partitionedTopicAddr.put(i, resultFuture.get());
                    } catch (Exception e) {
                        log.warn("getTopicBrokerAddr error.", e);
                    }
                });
            }
            lookupCache.put(topicName, partitionedTopicAddr);
        } catch (Exception e) {
            log.error("getTopicBroker info error for the topic[{}].", topicName, e);
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
                    log.warn("getBroker for topic failed, will retry in {} ms. throwable: ",
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
                        for (String topic : topics) {
                            TopicName name = TopicName.get(TopicName.get(topic).getPartitionedTopicName());
                            getTopicBrokerAddr(name);
                        }
                    } else {
                        log.error("Failed to get owned topic list for "
                                        + "OffsetAndTopicListener when triggering on-loading bundle {}.",
                                bundle, ex);
                    }
                });
    }

    @Override
    public void unLoad(NamespaceBundle bundle) {
        pulsarService.getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                .whenComplete((topics, ex) -> {
                    if (ex == null) {
                        log.info("get owned topic list when unLoad bundle {}, topic size {} ", bundle, topics.size());
                        for (String topic : topics) {
                            TopicName name = TopicName.get(topic);
                            lookupCache.invalidate(name);
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
        return namespaceBundle.getNamespaceObject().equals(rocketmqMetaNs)
                || namespaceBundle.getNamespaceObject().equals(rocketmqTopicNs);
    }

    private void createSysResource() throws Exception {
        String cluster = config.getClusterName();
        String metaTanant = config.getRocketmqMetadataTenant();
        String metaNs = config.getRocketmqMetadataNamespace();
        String defaultTanant = config.getRocketmqTenant();
        String defaultNs = config.getRocketmqNamespace();

        //create system namespace & default namespace
        createPulsarNamespaceIfNeeded(brokerService, cluster, metaTanant, metaNs);
        createPulsarNamespaceIfNeeded(brokerService, cluster, defaultTanant, defaultNs);

        //for test
        createPulsarNamespaceIfNeeded(brokerService, cluster, "test1", "InstanceTest");

        this.topicConfigTable.values().stream().forEach(tc -> {
            loadSysTopics(tc);
            createPulsarTopic(tc);
        });
    }

    private void loadSysTopics(TopicConfig tc) {
        String fullTopicName = tc.getTopicName();
        try {
            adminClient.lookups().lookupTopicAsync(fullTopicName);
        } catch (Exception e) {
            log.warn("load system topic [{}] error.", fullTopicName, e);
        }
    }

    private String createPulsarTopic(TopicConfig tc) {
        String fullTopicName = tc.getTopicName();
        try {
            PartitionedTopicMetadata topicMetadata =
                    adminClient.topics().getPartitionedTopicMetadata(fullTopicName);
            if (topicMetadata.partitions <= 0) {
                log.info("RocketMQ metadata topic {} doesn't exist. Creating it ...", fullTopicName);
                adminClient.topics().createPartitionedTopic(
                        fullTopicName,
                        tc.getWriteQueueNums());
                for (int i = 0; i < tc.getWriteQueueNums(); i++) {
                    adminClient.topics()
                            .createNonPartitionedTopic(fullTopicName + PARTITIONED_TOPIC_SUFFIX + i);
                }
            }
        } catch (Exception e) {
            log.warn("Topic {} concurrent creating and cause e: ", fullTopicName, e);
            return fullTopicName;
        }
        return fullTopicName;
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

}
