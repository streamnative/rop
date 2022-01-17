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
import static org.streamnative.pulsar.handlers.rocketmq.utils.PulsarUtil.getTopicPartitionNum;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.Uninterruptibles;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ReaderBuilderImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopServerException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.trace.TraceContext;
import org.streamnative.pulsar.handlers.rocketmq.inner.trace.TraceManager;
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
    private volatile CountDownLatch offsetLoadingLatch;
    private volatile ProducerBuilder<ByteBuffer> offsetTopicProducerBuilder;
    private volatile ReaderBuilder<ByteBuffer> offsetTopicReaderBuilder;
    private static final int MAX_CHECKPOINT_TIMEOUT_MS = 30 * 1000;

    /**
     * group offset producer\reader.
     */
    private volatile Producer<ByteBuffer> groupOffsetProducer;
    private volatile Reader<ByteBuffer> groupOffsetReader;

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
                groupOffsetProducer = offsetTopicProducerBuilder.clone()
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
                groupOffsetReader = offsetTopicReaderBuilder.clone()
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
            offsetTopicProducerBuilder = pulsarService.getClient().newProducer(Schema.BYTEBUFFER)
                    .maxPendingMessages(100000);
            offsetTopicReaderBuilder = new ReaderBuilderImpl<>((PulsarClientImpl) pulsarService.getClient(),
                    Schema.BYTEBUFFER);
            //send checkpoint messages for every partition
            List<MessageId> messageIds = sendCheckPointMessages();
            offsetLoadingLatch = new CountDownLatch(messageIds.size());

            long now = System.currentTimeMillis();
            offsetReaderExecutor.execute(() -> loadOffsets(offsetLoadingLatch, messageIds));
            // wait for offset load finish
            offsetLoadingLatch.await();
            log.info("RoP load offset meta cost: [{}ms]", System.currentTimeMillis() - now);

            persistOffsetExecutor.scheduleAtFixedRate(() -> {
                try {
                    persistOffset();
                    //showOffsetTable();
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
    private void loadOffsets(CountDownLatch latch, List<MessageId> checkedMessageIds) {
        log.info("Start load group offset.");
        while (isRunning) {
            try {
                Message<ByteBuffer> message = getGroupOffsetReader().readNext(1, TimeUnit.SECONDS);
                if (Objects.isNull(message)) {
                    continue;
                }
                if (needCountDownOffsetLatch(latch, message, checkedMessageIds)) {
                    latch.countDown();
                    if (latch.getCount() == 0) {
                        log.info("loadOffsets successfully and CountDownOffsetLatch count == 0.");
                    }
                } else if (Objects.nonNull(message.getData()) && message.getData().length > 0) {
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

    private boolean needCountDownOffsetLatch(CountDownLatch latch, Message<ByteBuffer> checkedMessage,
            List<MessageId> checkedMessageIds) {
        if (latch.getCount() > 0) {
            MessageIdImpl checkedMessageId = (MessageIdImpl) ((TopicMessageImpl) checkedMessage)
                    .getInnerMessageId();
            return checkedMessageIds.stream().filter(msgId ->
                    ((MessageIdImpl) msgId).getPartitionIndex() == checkedMessageId.getPartitionIndex()
            ).anyMatch((msgId) -> {
                MessageIdImpl msgIdImpl = (MessageIdImpl) msgId;
                return checkedMessageId.getLedgerId() >= msgIdImpl.getLedgerId()
                        && checkedMessageId.getEntryId() >= msgIdImpl.getEntryId();
            });
        }
        return false;
    }

    /**
     * query offset.
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        checkOffsetTableOk();

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
        checkOffsetTableOk();

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
        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(group, topic);
        String pulsarGroupName = groupAndTopicName.getClientGroupName().getPulsarGroupName();
        String pulsarTopicName = groupAndTopicName.getClientTopicName().getPulsarTopicName();
        int pulsarPartitionId = brokerController.getRopBrokerProxy()
                .getPulsarTopicPartitionId(TopicName.get(pulsarTopicName), queueId);

        commitOffsetInternal(pulsarGroupName, pulsarTopicName, pulsarPartitionId, offset);
    }

    public void commitOffsetByPartitionId(final String group, final String topic, final int pulsarPartitionId,
            final long offset) {
        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(group, topic);
        String pulsarGroupName = groupAndTopicName.getClientGroupName().getPulsarGroupName();
        String pulsarTopicName = groupAndTopicName.getClientTopicName().getPulsarTopicName();

        commitOffsetInternal(pulsarGroupName, pulsarTopicName, pulsarPartitionId, offset);
    }

    public void commitOffsetInternal(String pulsarGroup, String pulsarTopic, final int pulsarPartitionId,
            final long offset) {
        // skip commit offset request if this broker not owner for the request queueId topic
        log.debug("When commit group[{}] offset, the [topic@partitionId] is [{}@{}] and the messageID is: {}",
                pulsarGroup, pulsarTopic, pulsarPartitionId, offset);

        GroupOffsetKey groupOffsetKey = new GroupOffsetKey(pulsarGroup, pulsarTopic, pulsarPartitionId);
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

        // Trace point:/rop/commit
        if (this.brokerController.isRopTraceEnable()) {
            oldGroupOffset = offsetTable.getIfPresent(groupOffsetKey);
            if (oldGroupOffset != null && oldGroupOffset.isValid()) {
                TraceContext traceContext = new TraceContext();
                traceContext.setTopic(pulsarTopic);
                traceContext.setGroup(pulsarGroup);
                traceContext.setPartitionId(pulsarPartitionId);
                traceContext.setOffset(offset);
                TraceManager.get().traceCommit(traceContext);
            }
        }
    }

    public void persistOffset() {
        checkOffsetTableOk();
        long now = System.currentTimeMillis();
        AtomicLong count = new AtomicLong(0);
        for (Entry<GroupOffsetKey, GroupOffsetValue> entry : offsetTable.asMap().entrySet()) {
            GroupOffsetKey groupOffsetKey = entry.getKey();
            GroupOffsetValue groupOffsetValue = entry.getValue();
            String pulsarGroup = groupOffsetKey.getGroupName();
            if (groupOffsetValue.isValid() && !isSystemGroup(pulsarGroup) && isGroupExist(pulsarGroup)) {
                count.incrementAndGet();
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
        log.info("RoP persist offset count: [{}], total count: [{}], cost: [{}ms]", count.get(), offsetTable.size(),
                System.currentTimeMillis() - now);
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

    public boolean isGroupExist(String pulsarGroup) {
        return this.brokerController.getRopBrokerProxy().getZkService().isGroupExist(pulsarGroup);
    }

    private boolean isPulsarTopicCached(ClientTopicName topicName, int pulsarPartitionId) {
        if (Objects.isNull(topicName) || pulsarPartitionId < 0) {
            return false;
        }

        return pulsarTopicCache.containsKey(topicName) && pulsarTopicCache.get(topicName)
                .containsKey(pulsarPartitionId);
    }

    private List<MessageId> sendCheckPointMessages() {
        TopicName offsetTopicName = RocketMQTopic.getGroupMetaOffsetTopic().getPulsarTopicName();
        int pNum = getTopicPartitionNum(brokerController.getBrokerService(), offsetTopicName.toString(),
                brokerController.getServerConfig().getOffsetsTopicNumPartitions());
        Preconditions.checkArgument(pNum > 0, "The partition num of offset topic must > 0.");
        List<MessageId> messageIds = IntStream.range(0, pNum).mapToObj(pId -> {
            Producer<ByteBuffer> partitionProducer = null;
            try {
                partitionProducer = offsetTopicProducerBuilder.clone()
                        .topic(offsetTopicName.getPartition(pId).toString())
                        .compressionType(CompressionType.SNAPPY)
                        .enableBatching(false)
                        .create();
                MessageIdImpl messageId = (MessageIdImpl) partitionProducer.newMessage().value(ByteBuffer.allocate(0))
                        .send();
                return new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), pId);
            } catch (PulsarClientException e) {
                log.warn("sendCheckPointMessages partitionId=[{}] error.", pId);
                return null;
            } finally {
                if (partitionProducer != null) {
                    partitionProducer.closeAsync();
                }
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        Preconditions.checkArgument(messageIds.size() == pNum, "sendCheckpointMessage num is wrong.");
        return messageIds;
    }

    private void checkOffsetTableOk() {
        try {
            offsetLoadingLatch.await(MAX_CHECKPOINT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("offsetTable is loading timeout [{}] ms.", MAX_CHECKPOINT_TIMEOUT_MS);
        }
    }
}
