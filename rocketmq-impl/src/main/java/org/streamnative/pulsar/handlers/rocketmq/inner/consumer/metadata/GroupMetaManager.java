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

package org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata;

import static org.streamnative.pulsar.handlers.rocketmq.utils.CoreUtils.inLock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQServiceConfiguration;
import org.streamnative.pulsar.handlers.rocketmq.inner.timer.Time;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

@Slf4j
public class GroupMetaManager {

    private final RocketMQServiceConfiguration rocketmqConfig;
    private final ReentrantLock partitionLock = new ReentrantLock();
    private final Set<Integer> loadingPartitions = new HashSet<>();
    private final Set<Integer> ownedPartitions = new HashSet<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final int groupMetadataTopicPartitionCount;
    private final ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupCache;

    private final ConcurrentMap<Integer, CompletableFuture<Producer<ByteBuffer>>> offsetsProducers =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, CompletableFuture<Reader<ByteBuffer>>> offsetsReaders =
            new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler;
    private final Map<Long, Set<String>> openGroupsForProducer = new HashMap<>();

    private final ProducerBuilder<ByteBuffer> metadataTopicProducerBuilder;
    private final ReaderBuilder<ByteBuffer> metadataTopicReaderBuilder;
    private final Time time;
    private final Function<String, Integer> partitioner;

    public GroupMetaManager(RocketMQServiceConfiguration rocketmqConfig,
            ProducerBuilder<ByteBuffer> metadataTopicProducerBuilder,
            ReaderBuilder<ByteBuffer> metadataTopicReaderBuilder,
            ScheduledExecutorService scheduler,
            Time time) {
        this(
                rocketmqConfig,
                metadataTopicProducerBuilder,
                metadataTopicReaderBuilder,
                scheduler,
                time,
                groupId -> MathUtils.signSafeMod(
                        groupId.hashCode(),
                        rocketmqConfig.getOffsetsTopicNumPartitions()
                )
        );
    }

    GroupMetaManager(RocketMQServiceConfiguration rocketmqConfig,
            ProducerBuilder<ByteBuffer> metadataTopicProducerBuilder,
            ReaderBuilder<ByteBuffer> metadataTopicConsumerBuilder,
            ScheduledExecutorService scheduler,
            Time time,
            Function<String, Integer> partitioner) {
        this.rocketmqConfig = rocketmqConfig;
        this.subscriptionGroupCache = new ConcurrentHashMap<>();
        this.groupMetadataTopicPartitionCount = rocketmqConfig.getOffsetsTopicNumPartitions();
        this.metadataTopicProducerBuilder = metadataTopicProducerBuilder;
        this.metadataTopicReaderBuilder = metadataTopicConsumerBuilder;
        this.scheduler = scheduler;
        this.time = time;
        this.partitioner = partitioner;
    }

    public void startup(boolean enableMetadataExpiration) {

    }

    public void shutdown() {
        shuttingDown.set(true);
        scheduler.shutdown();
        List<CompletableFuture<Void>> producerCloses = offsetsProducers.entrySet().stream()
                .map(v -> v.getValue()
                        .thenComposeAsync(producer -> producer.closeAsync(), scheduler))
                .collect(Collectors.toList());
        offsetsProducers.clear();
        List<CompletableFuture<Void>> readerCloses = offsetsReaders.entrySet().stream()
                .map(v -> v.getValue()
                        .thenComposeAsync(reader -> reader.closeAsync(), scheduler))
                .collect(Collectors.toList());
        offsetsReaders.clear();

        FutureUtil.waitForAll(producerCloses).whenCompleteAsync((ignore, t) -> {
            if (t != null) {
                log.error("Error when close all the {} offsetsProducers in GroupMetadataManager",
                        producerCloses.size(), t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Closed all the {} offsetsProducers in GroupMetadataManager", producerCloses.size());
            }
        }, scheduler);

        FutureUtil.waitForAll(readerCloses).whenCompleteAsync((ignore, t) -> {
            if (t != null) {
                log.error("Error when close all the {} offsetsReaders in GroupMetadataManager",
                        readerCloses.size(), t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Closed all the {} offsetsReaders in GroupMetadataManager.", readerCloses.size());
            }
        }, scheduler);
    }

    public boolean isPartitionOwned(int partition) {
        return inLock(
                partitionLock,
                () -> ownedPartitions.contains(partition));
    }

    public boolean isPartitionLoading(int partition) {
        return inLock(
                partitionLock,
                () -> loadingPartitions.contains(partition)
        );
    }

    public int partitionFor(String groupId) {
        return partitioner.apply(groupId);
    }

    CompletableFuture<Producer<ByteBuffer>> getOffsetsTopicProducer(String groupId) {
        return offsetsProducers.computeIfAbsent(partitionFor(groupId),
                partitionId -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Created Partitioned producer: {} for consumer group: {}",
                                RocketMQTopic.getGroupMetaOffsetTopic().getPartitionName(partitionId),
                                groupId);
                    }
                    return metadataTopicProducerBuilder.clone()
                            .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPartitionName(partitionId))
                            .createAsync();
                });
    }

    CompletableFuture<Producer<ByteBuffer>> getOffsetsTopicProducer(int partitionId) {
        return offsetsProducers.computeIfAbsent(partitionId,
                id -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Will create Partitioned producer: {}",
                                RocketMQTopic.getGroupMetaOffsetTopic().getPartitionName(id));
                    }
                    return metadataTopicProducerBuilder.clone()
                            .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPartitionName(id))
                            .createAsync();
                });
    }

    CompletableFuture<Reader<ByteBuffer>> getOffsetsTopicReader(int partitionId) {
        return offsetsReaders.computeIfAbsent(partitionId,
                id -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Will create Partitioned reader: {}",
                                RocketMQTopic.getGroupMetaOffsetTopic().getPartitionName(id));
                    }
                    return metadataTopicReaderBuilder.clone()
                            .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPartitionName(id))
                            .readCompacted(true)
                            .createAsync();
                });
    }
}
