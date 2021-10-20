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

import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_CACHE_INITIAL_SIZE;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.SLASH_CHAR;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.Uninterruptibles;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ReaderBuilderImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopServerException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Group meta manager.
 */
@Slf4j
public class GroupMetaManager {

    private volatile boolean isRunning = false;
    private final RocketMQBrokerController brokerController;
    private final long offsetsRetentionMs;
    private volatile PulsarService pulsarService;

    /**
     * group offset producer\reader.
     */
    private Producer<ByteBuffer> groupOffsetProducer;
    private Reader<ByteBuffer> groupOffsetReader;

    /**
     * group offset reader executor.
     */
    private final ExecutorService offsetReaderExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("Rop-group-offset-reader");
        t.setDaemon(true);
        return t;
    });

    private final ScheduledExecutorService persistOffsetExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("Rop-persist-offset");
        t.setDaemon(true);
        return t;
    });

    @Getter
    private final ConcurrentHashMap<ClientTopicName, ConcurrentMap<Integer, PersistentTopic>> pulsarTopicCache;

    @Getter
    private final Cache<GroupOffsetKey, GroupOffsetValue> offsetTable;

    public GroupMetaManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.offsetsRetentionMs = brokerController.getServerConfig().getOffsetsRetentionMinutes() * 60 * 1000;
        this.pulsarTopicCache = new ConcurrentHashMap<>(ROP_CACHE_INITIAL_SIZE);
        this.offsetTable = CacheBuilder.newBuilder()
                .initialCapacity(ROP_CACHE_INITIAL_SIZE)
                .expireAfterAccess(offsetsRetentionMs, TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<GroupOffsetKey, GroupOffsetValue>) listener -> {
                    GroupOffsetKey key = listener.getKey();
                    GroupOffsetValue value = listener.getValue();
                    if (System.currentTimeMillis() >= value.getExpireTimestamp()) {
                        log.info("Begin to remove GroupOffsetKey[{}] with GroupOffsetValue[{}].", key, value);
                        try {
                            Producer<ByteBuffer> producer = getGroupOffsetProducer();
                            if (producer != null && producer.isConnected()) {
                                producer.newMessage().keyBytes(key.encode().array())
                                        .value(null).sendAsync();
                            }
                            log.info("remove expired-group-offset-key[{}] successfully.", key);
                        } catch (Exception e) {
                            log.warn("remove expired-group-offset-key[{}] error.", key, e);
                        }
                    }
                }).build();
    }

    private Producer<ByteBuffer> getGroupOffsetProducer() {
        if (isRunning && groupOffsetProducer == null) {
            try {
                PulsarClient pulsarClient = brokerController.getBrokerService().getPulsar().getClient();
                ProducerBuilder<ByteBuffer> producer = pulsarClient
                        .newProducer(Schema.BYTEBUFFER)
                        .maxPendingMessages(100000);

                groupOffsetProducer = producer.clone()
                        .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPulsarFullName())
                        .compressionType(CompressionType.SNAPPY)
                        .enableBatching(true)
                        .blockIfQueueFull(false)
                        .create();
            } catch (Exception e) {
                log.warn("getGroupOffsetProducer error.", e);
                throw new RuntimeException("Get group offset producer exception");
            }
        }
        return groupOffsetProducer;
    }

    private Reader<ByteBuffer> getGroupOffsetReader() {
        if (isRunning && groupOffsetReader == null) {
            try {
                PulsarClient pulsarClient = brokerController.getBrokerService().getPulsar().getClient();
                ReaderBuilder<ByteBuffer> reader = new ReaderBuilderImpl<>((PulsarClientImpl) pulsarClient,
                        Schema.BYTEBUFFER);
                groupOffsetReader = reader.clone()
                        .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPulsarFullName())
                        .startMessageId(MessageId.earliest)
                        .readCompacted(true)
                        .create();
            } catch (Exception e) {
                log.warn("getGroupOffsetReader error.", e);
                throw new RuntimeException("Get group offset reader exception");
            }
        }
        return groupOffsetReader;
    }

    public void start() throws Exception {
        log.info("Starting GroupMetaManager service...");
        try {
            isRunning = true;
            pulsarService = this.brokerController.getBrokerService().pulsar();
            offsetReaderExecutor.execute(this::loadOffsets);
            Thread.sleep(5000);

            persistOffsetExecutor.scheduleAtFixedRate(() -> {
                try {
                    persistOffset();
                    showOffsetTable();
                } catch (Throwable e) {
                    log.error("Persist consumerOffset error.", e);
                }
            }, 1000 * 10, brokerController.getServerConfig().getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            isRunning = false;
            throw new RopServerException("GroupMetaManager failed to start.", e);
        }
        log.info("Start GroupMetaManager service successfully.");
    }

    private void showOffsetTable() {
        log.debug("Rop show offset table:");
        offsetTable.asMap().forEach((groupOffsetKey, groupOffsetValue) ->
                log.debug(String.format("GroupOffsetKey: [%s|%s|%s], GroupOffsetValue: %s",
                        groupOffsetKey.getGroupName(),
                        groupOffsetKey.getTopicName(),
                        groupOffsetKey.getPulsarPartitionId(),
                        groupOffsetValue.getOffset())));
    }

    /**
     * load offset.
     */
    private void loadOffsets() {
        log.info("Start load group offset.");
        while (isRunning) {
            try {
                Message<ByteBuffer> message = getGroupOffsetReader().readNext(1, TimeUnit.SECONDS);
                if (Objects.nonNull(message)) {
                    GroupOffsetKey groupOffsetKey = GroupMetaKey.decodeKey(ByteBuffer.wrap(message.getKeyBytes()));
                    GroupOffsetValue groupOffsetValue = GroupOffsetValue.decodeGroupOffset(message.getValue());
                    offsetTable.put(groupOffsetKey, groupOffsetValue);
                }
            } catch (Exception e) {
                log.warn("groupOffsetReader read offset failed.", e);
                Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * query offset.
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(group, topic);
        String pulsarGroupName = groupAndTopicName.getClientGroupName().getPulsarGroupName();
        String pulsarTopicName = groupAndTopicName.getClientTopicName().getPulsarTopicName();
        int pulsarPartitionId = brokerController.getRopBrokerProxy()
                .getPulsarTopicPartitionId(TopicName.get(pulsarTopicName), queueId);

        GroupOffsetKey groupOffsetKey = new GroupOffsetKey(pulsarGroupName, pulsarTopicName, pulsarPartitionId);
        GroupOffsetValue offsetValue = offsetTable.getIfPresent(groupOffsetKey);
        if (Objects.nonNull(offsetValue)) {
            return offsetValue.getOffset();
        } else {//query in pulsar, if exists store it into cache
            long groupOffset = getGroupOffsetFromPulsar(groupAndTopicName, pulsarPartitionId);
            if (groupOffset > -1) {
                commitOffset(group, topic, queueId, groupOffset);
            }
            return groupOffset;
        }
    }

    /**
     * query offset.
     */
    public long queryOffsetByPartitionId(final String group, final String topic, final int partitionId) {
        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(group, topic);
        String pulsarGroupName = groupAndTopicName.getClientGroupName().getPulsarGroupName();
        String pulsarTopicName = groupAndTopicName.getClientTopicName().getPulsarTopicName();

        GroupOffsetKey groupOffsetKey = new GroupOffsetKey(pulsarGroupName, pulsarTopicName, partitionId);
        GroupOffsetValue offsetValue = offsetTable.getIfPresent(groupOffsetKey);
        if (Objects.nonNull(offsetValue)) {
            return offsetValue.getOffset();
        } else {//query in pulsar, if exists store it into cache
            return getGroupOffsetFromPulsar(groupAndTopicName, partitionId);
        }
    }

    private long getGroupOffsetFromPulsar(ClientGroupAndTopicName groupAndTopic, int pulsarPartitionId) {
        try {
            PersistentTopic persistentTopic = getPulsarPersistentTopic(groupAndTopic.getClientTopicName(),
                    pulsarPartitionId);
            String pulsarGroup = groupAndTopic.getClientGroupName().getPulsarGroupName();
            PersistentSubscription subscription = persistentTopic.getSubscription(pulsarGroup);
            if (subscription != null) {
                PositionImpl markDeletedPosition = (PositionImpl) subscription.getCursor().getMarkDeletedPosition();
                return MessageIdUtils.getQueueOffsetByPosition(persistentTopic, markDeletedPosition);
            }
        } catch (Exception ex) {
            log.warn("GroupMetaManager getGroupOffsetFromPulsar error: {}", ex.getMessage());
        }
        return -1L;
    }

    public void commitOffset(final String group, final String topic, final int queueId, final long offset) {
        // skip commit offset request if this broker not owner for the request queueId topic
        log.debug("When commit group[{}] offset, the [topic@queueId] is [{}@{}] and the messageID is: {}",
                group, topic, queueId, offset);
        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(group, topic);
        String pulsarGroupName = groupAndTopicName.getClientGroupName().getPulsarGroupName();
        String pulsarTopicName = groupAndTopicName.getClientTopicName().getPulsarTopicName();
        int pulsarPartitionId = brokerController.getRopBrokerProxy()
                .getPulsarTopicPartitionId(TopicName.get(pulsarTopicName), queueId);

        GroupOffsetKey groupOffsetKey = new GroupOffsetKey(pulsarGroupName, pulsarTopicName, pulsarPartitionId);
        GroupOffsetValue oldGroupOffset = offsetTable.getIfPresent(groupOffsetKey);
        long commitTimestamp = System.currentTimeMillis();
        long expireTimestamp = System.currentTimeMillis() + offsetsRetentionMs;
        if (Objects.nonNull(oldGroupOffset)) {
            //refresh group offset value and expire time
            oldGroupOffset.refresh(offset, commitTimestamp, expireTimestamp);
        } else {
            // add new group offset
            offsetTable.put(groupOffsetKey, new GroupOffsetValue(offset, commitTimestamp, expireTimestamp));
        }
    }

    public void commitOffsetByPartitionId(final String group, final String topic, final int partitionId,
            final long offset) {
        // skip commit offset request if this broker not owner for the request queueId topic
        log.debug("When commit group[{}] offset, the [topic@partitionId] is [{}@{}] and the messageID is: {}",
                group, topic, partitionId, offset);
        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(group, topic);
        String pulsarGroupName = groupAndTopicName.getClientGroupName().getPulsarGroupName();
        String pulsarTopicName = groupAndTopicName.getClientTopicName().getPulsarTopicName();

        GroupOffsetKey groupOffsetKey = new GroupOffsetKey(pulsarGroupName, pulsarTopicName, partitionId);
        GroupOffsetValue oldGroupOffset = offsetTable.getIfPresent(groupOffsetKey);
        long commitTimestamp = System.currentTimeMillis();
        long expireTimestamp = System.currentTimeMillis() + offsetsRetentionMs;
        if (Objects.nonNull(oldGroupOffset)) {
            //refresh group offset value and expire time
            oldGroupOffset.refresh(offset, commitTimestamp, expireTimestamp);
        } else {
            // add new group offset
            offsetTable.put(groupOffsetKey, new GroupOffsetValue(offset, commitTimestamp, expireTimestamp));
        }
    }

    public void persistOffset() {
        for (Entry<GroupOffsetKey, GroupOffsetValue> entry : offsetTable.asMap().entrySet()) {
            GroupOffsetKey groupOffsetKey = entry.getKey();
            GroupOffsetValue groupOffsetValue = entry.getValue();
            String pulsarGroup = groupOffsetKey.getGroupName();
            if (groupOffsetValue.isValid() && !isSystemGroup(pulsarGroup)) {
                String pulsarTopicName = groupOffsetKey.getTopicName();
                int pulsarPartitionId = groupOffsetKey.getPulsarPartitionId();
                long offset = groupOffsetValue.getOffset();
                try {
                    //persist to compact topic[__consumer_offsets]
                    storeOffset(groupOffsetKey, groupOffsetValue);

                    if (!this.brokerController.getTopicConfigManager().isPartitionTopicOwner(
                            TopicName.get(pulsarTopicName), pulsarPartitionId)) {
                        continue;
                    }

                    PersistentTopic persistentTopic = getPulsarPersistentTopic(
                            new ClientTopicName(TopicName.get(pulsarTopicName)),
                            pulsarPartitionId);
                    if (persistentTopic != null) {
                        PersistentSubscription subscription = persistentTopic.getSubscription(pulsarGroup);
                        if (subscription == null) {
                            subscription = (PersistentSubscription) persistentTopic
                                    .createSubscription(pulsarGroup, InitialPosition.Earliest, false)
                                    .get();
                        }
                        ManagedCursor cursor = subscription.getCursor();
                        PositionImpl markDeletedPosition = (PositionImpl) cursor.getMarkDeletedPosition();

                        // get position by manage ledger and offset
                        PositionImpl commitPosition = MessageIdUtils
                                .getPositionForOffset(persistentTopic.getManagedLedger(), offset - 1);
                        PositionImpl lastPosition = (PositionImpl) persistentTopic.getLastPosition();

                        if (commitPosition.compareTo(markDeletedPosition) > 0
                                && commitPosition.compareTo(lastPosition) < 0) {
                            try {
                                cursor.markDelete(commitPosition);
                                log.debug("[{}] [{}] Mark delete [position = {}] successfully.",
                                        pulsarGroup, pulsarTopicName, commitPosition);
                            } catch (Exception e) {
                                log.info("[{}] [{}] Mark delete [position = {}] and deletedPosition[{}] error.",
                                        pulsarGroup, pulsarTopicName, commitPosition, markDeletedPosition, e);
                            }
                        } else {
                            log.debug(
                                    "[{}] [{}] Skip mark delete for [position = {}] less than [oldPosition = {}].",
                                    pulsarGroup, pulsarTopicName, commitPosition, markDeletedPosition);
                        }
                    }
                } catch (Exception ex) {
                    log.warn("persistOffset GroupOffsetKey[{}] and GroupOffsetValue[{}] error.", groupOffsetKey,
                            groupOffsetValue, ex);
                }
            }
        }
    }

    /**
     * store offset.
     */
    private void storeOffset(final GroupOffsetKey groupOffsetKey, final GroupOffsetValue groupOffsetValue) {
        try {
            if (Objects.isNull(groupOffsetKey) || Objects.isNull(groupOffsetValue)
                    || groupOffsetValue.getOffset() < 0L) {
                return;
            }
            if (groupOffsetValue.isUpdated()) {
                getGroupOffsetProducer().newMessage()
                        .keyBytes(groupOffsetKey.encode().array())
                        .value(groupOffsetValue.encode())
                        .sendAsync()
                        .whenCompleteAsync((msgId, e) -> {
                            if (e != null) {
                                log.info("[{}] [{}] Store group offset failed.", groupOffsetKey, groupOffsetValue, e);
                                return;
                            }
                            groupOffsetValue.setUpdated(false);
                        });
            }
        } catch (Exception e) {
            log.info("[{}] [{}] Store group offset error.", groupOffsetKey, groupOffsetValue, e);
        }
    }

    public void shutdown() {
        if (isRunning) {
            persistOffsetExecutor.shutdown();

            offsetReaderExecutor.shutdown();

            if (Objects.nonNull(groupOffsetProducer)) {
                groupOffsetProducer.closeAsync();
            }

            if (Objects.nonNull(groupOffsetReader)) {
                groupOffsetReader.closeAsync();
            }
            isRunning = false;
        }
    }


    public void putPulsarTopic(ClientTopicName clientTopicName, int pulsarPartitionId, PersistentTopic pulsarTopic) {
        if (Objects.isNull(clientTopicName) || Objects.isNull(pulsarTopic) || pulsarPartitionId < 0) {
            return;
        }
        pulsarTopicCache.putIfAbsent(clientTopicName, new ConcurrentHashMap<>());
        pulsarTopicCache.get(clientTopicName).put(pulsarPartitionId, pulsarTopic);
    }

    public void removePulsarTopic(ClientTopicName clientTopicName, int partitionId) {
        if (pulsarTopicCache.containsKey(clientTopicName)) {
            pulsarTopicCache.get(clientTopicName).remove(partitionId);
            if (pulsarTopicCache.get(clientTopicName).isEmpty()) {
                pulsarTopicCache.remove(clientTopicName);
            }
        }
    }

    public PersistentTopic getPulsarPersistentTopic(ClientTopicName topicName, int pulsarPartitionId)
            throws RopPersistentTopicException {
        if (isPulsarTopicCached(topicName, pulsarPartitionId)) {
            boolean isOwnedTopic = pulsarService.getNamespaceService()
                    .isServiceUnitActive(topicName.toPulsarTopicName().getPartition(pulsarPartitionId));
            if (isOwnedTopic) {
                return this.pulsarTopicCache.get(topicName).get(pulsarPartitionId);
            } else {
                pulsarTopicCache.get(topicName).remove(pulsarPartitionId);
                throw new RopPersistentTopicException("topic[{}] and partition[{}] isn't owned by current broker.");
            }
        }

        try {
            return (PersistentTopic) getPulsarPersistentTopicAsync(topicName, pulsarPartitionId)
                    .whenComplete((topic, ex) -> {
                        if (topic.isPresent()) {
                            PersistentTopic persistentTopic = (PersistentTopic) topic.get();
                            this.pulsarTopicCache.putIfAbsent(topicName, new ConcurrentHashMap<>());
                            pulsarTopicCache.get(topicName).putIfAbsent(pulsarPartitionId, persistentTopic);
                        } else {
                            log.warn("Not found PulsarPersistentTopic pulsarTopic: {}, partition: {}", topicName,
                                    pulsarPartitionId);
                        }
                    }).thenApply(Optional::get).join();
        } catch (Exception e) {
            throw new RopPersistentTopicException(
                    String.format("Get pulsarTopic[%s] and partition[%d] error.", topicName, pulsarPartitionId));
        }
    }

    private CompletableFuture<Optional<Topic>> getPulsarPersistentTopicAsync(ClientTopicName clientTopicName,
            int pulsarPartitionId) {
        // setup ownership of service unit to this broker
        TopicName pulsarTopicName = TopicName.get(clientTopicName.getPulsarTopicName());
        TopicName partitionTopicName = pulsarTopicName.getPartition(pulsarPartitionId);
        String topicName = partitionTopicName.toString();
        return brokerController.getBrokerService().getTopicIfExists(topicName);
    }

    public boolean isSystemGroup(String groupName) {
        return groupName.startsWith(brokerController.getServerConfig().getRocketmqMetadataTenant()
                + SLASH_CHAR
                + brokerController.getServerConfig().getRocketmqMetadataNamespace());
    }

    private boolean isPulsarTopicCached(ClientTopicName topicName, int pulsarPartitionId) {
        if (Objects.isNull(topicName) || pulsarPartitionId < 0) {
            return false;
        }

        return pulsarTopicCache.containsKey(topicName) && pulsarTopicCache.get(topicName)
                .containsKey(pulsarPartitionId);
    }

}
