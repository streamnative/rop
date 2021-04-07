package com.tencent.tdmq.handlers.rocketmq.inner.namesvr;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tencent.tdmq.handlers.rocketmq.RocketMQServiceConfiguration;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

@Slf4j
public class MQTopicManager implements NamespaceBundleOwnershipListener {

    private final int MAX_CACHE_SIZE = 2000;
    public final Cache<String, InetSocketAddress> lookupBrokerCache = CacheBuilder.newBuilder()
            .initialCapacity(MAX_CACHE_SIZE).maximumSize(MAX_CACHE_SIZE)
            .build();
    private final RocketMQBrokerController brokerController;
    private final Cache<String, PersistentTopic> topics = CacheBuilder.newBuilder()
            .initialCapacity(MAX_CACHE_SIZE).maximumSize(MAX_CACHE_SIZE)
            .build();
    private final Cache<String, PartitionedTopicMetadata> partitionedMetaCache = CacheBuilder.newBuilder()
            .initialCapacity(MAX_CACHE_SIZE).maximumSize(MAX_CACHE_SIZE)
            .build();
    private PulsarService pulsarService;
    private BrokerService brokerService;
    private PulsarAdmin adminClient;
    private NamespaceName rocketmqMetaNs;
    private NamespaceName rocketmqTopicNs;

    public MQTopicManager(RocketMQBrokerController brokerController) throws PulsarServerException {
        this.brokerController = brokerController;
        RocketMQServiceConfiguration config = brokerController.getServerConfig();
        RocketMQTopic.initialize(config.getRocketmqTenant() + "/" + config.getRocketmqNamespace());
        this.rocketmqMetaNs = NamespaceName
                .get(config.getRocketmqMetadataTenant(), config.getRocketmqMetadataNamespace());
        this.rocketmqTopicNs = NamespaceName.get(config.getRocketmqTenant(), config.getRocketmqNamespace());
    }

    public void start() throws PulsarServerException {
        this.pulsarService = brokerController.getBrokerService().pulsar();
        this.brokerService = pulsarService.getBrokerService();
        this.adminClient = this.pulsarService.getAdminClient();
        this.pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(this);
    }

    public void shutdown() {
        this.topics.cleanUp();
        this.partitionedMetaCache.cleanUp();
        this.lookupBrokerCache.cleanUp();
    }

    // call pulsarclient.lookup.getbroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public InetSocketAddress getTopicBroker(String topicName) {
        InetSocketAddress address = null;
        if (lookupBrokerCache.getIfPresent(topicName) == null) {
            log.debug("topic {} not in Lookup_cache, call lookupBroker", topicName);
            CompletableFuture<InetSocketAddress> resultFuture = new CompletableFuture<>();
            Backoff backoff = new Backoff(
                    100, TimeUnit.MILLISECONDS,
                    15, TimeUnit.SECONDS,
                    15, TimeUnit.SECONDS
            );
            lookupBroker(topicName, backoff, resultFuture);
            try {
                address = resultFuture.get();
                lookupBrokerCache.put(topicName, address);
            } catch (Exception e) {
                log.error("getTopicBroker info error for the topic[{}].", topicName, e);
            }
        }
        return lookupBrokerCache.getIfPresent(topicName);
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    private void lookupBroker(String topicName, Backoff backoff,
            CompletableFuture<InetSocketAddress> retFuture) {
        try {
            ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                    .getBroker(TopicName.get(topicName))
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

    public PartitionedTopicMetadata getPartitionedTopicMetadata(String fullTopicName) {
        if (partitionedMetaCache.getIfPresent(fullTopicName) == null) {
            try {
                PartitionedTopicMetadata partitionedMetadata = adminClient.topics()
                        .getPartitionedTopicMetadata(fullTopicName);
                partitionedMetaCache.put(fullTopicName, partitionedMetadata);
            } catch (PulsarAdminException e) {
                log.error("getPartitionedTopicMetadata info error for the topic[{}].", fullTopicName, e);
            }
        }
        return partitionedMetaCache.getIfPresent(fullTopicName);
    }

    // For Produce/Consume we need to lookup, to make sure topic served by brokerService,
    // or will meet error: "Service unit is not ready when loading the topic".
    // If getTopic is called after lookup, then no needLookup.
    // Returned Future wil complete with null when meet error.
    public PersistentTopic getTopic(String fullTopicName) {
        if (topics.getIfPresent(fullTopicName) == null) {
            try {
                Optional<Topic> optionalTopic = brokerService.getTopic(fullTopicName, true).get();
                if (optionalTopic.isPresent()) {
                    PersistentTopic persistentTopic = (PersistentTopic) optionalTopic.get();
                    topics.put(fullTopicName, persistentTopic);
                } else {
                    log.error("topic[{}] couldn't be found.", fullTopicName);
                }
                if (log.isDebugEnabled()) {
                    log.debug("create topic [{}] successful", fullTopicName);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to getTopic {}. exception:", fullTopicName, e);
            }
        }
        return topics.getIfPresent(fullTopicName);
    }

    @Override
    public void onLoad(NamespaceBundle bundle) {
        // get new partitions owned by this pulsar service.
        pulsarService.getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                .whenComplete((topics, ex) -> {
                    if (ex == null) {
                        log.info("get owned topic list when onLoad bundle {}, topic size {} ", bundle, topics.size());
                        for (String topic : topics) {
                            TopicName name = TopicName.get(topic);
                            topics.remove(name.toString());
                            // update lookup cache when onload
                            try {
                                CompletableFuture<InetSocketAddress> retFuture = new CompletableFuture<>();
                                ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                                        .getBroker(TopicName.get(topic))
                                        .whenComplete((pair, throwable) -> {
                                            if (throwable != null) {
                                                log.warn("cloud not get broker", throwable);
                                                retFuture.complete(null);
                                            }
                                            checkState(pair.getLeft().equals(pair.getRight()));
                                            retFuture.complete(pair.getLeft());
                                        });
                                this.lookupBrokerCache.put(topic, retFuture.get());
                            } catch (Exception e) {
                                log.error("onLoad topic routine Exception ", e);
                            }

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
                            lookupBrokerCache.invalidate(name.toString());
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

   /* private void getAllTopicsAsync(CompletableFuture<Map<String, List<TopicName>>> topicMapFuture) {
        admin.tenants().getTenantsAsync().thenApply(tenants -> {
            if (tenants.isEmpty()) {
                topicMapFuture.complete(Maps.newHashMap());
                return null;
            }
            Map<String, List<TopicName>> topicMap = Maps.newConcurrentMap();

            AtomicInteger numTenants = new AtomicInteger(tenants.size());
            for (String tenant : tenants) {
                admin.namespaces().getNamespacesAsync(tenant).thenApply(namespaces -> {
                    if (namespaces.isEmpty() && numTenants.decrementAndGet() == 0) {
                        topicMapFuture.complete(topicMap);
                        return null;
                    }
                    AtomicInteger numNamespaces = new AtomicInteger(namespaces.size());
                    for (String namespace : namespaces) {
                        pulsarService.getNamespaceService().getListOfPersistentTopics(NamespaceName.get(namespace))
                                .thenApply(topics -> {
                                    for (String topic : topics) {
                                        TopicName topicName = TopicName.get(topic);
                                        String key = topicName.getPartitionedTopicName();
                                        topicMap.computeIfAbsent(RocketMQTopic.removeDefaultNamespacePrefix(key),
                                                ignored ->
                                                        Collections.synchronizedList(new ArrayList<>())
                                        ).add(topicName);
                                    }
                                    if (numNamespaces.decrementAndGet() == 0 && numTenants.decrementAndGet() == 0) {
                                        topicMapFuture.complete(topicMap);
                                    }
                                    return null;
                                });
                    }
                    return null;
                });
            }
            return null;
        }).exceptionally(topicMapFuture::completeExceptionally);
    }*/

}
