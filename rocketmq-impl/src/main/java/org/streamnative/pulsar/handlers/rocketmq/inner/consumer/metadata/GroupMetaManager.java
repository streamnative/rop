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

import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.SLASH_CHAR;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Group meta manager.
 */
@Slf4j
public class GroupMetaManager {

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final RocketMQBrokerController brokerController;

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

    /**
     * group offset\meta callback executor.
     */
    private final ExecutorService groupMetaCallbackExecutor = Executors.newFixedThreadPool(10, r -> {
        Thread t = new Thread(r);
        t.setName("Rop-group-meta-callback");
        t.setDaemon(true);
        return t;
    });

    private final ScheduledExecutorService persistOffsetExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("Rop-persist-offset");
        t.setDaemon(true);
        return t;
    });

    private final ScheduledExecutorService clearOffsetExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("Rop-clear-offset");
        t.setDaemon(true);
        return t;
    });

    /**
     * group offset table
     * key   => topic@group.
     * topic => tenant/namespace/topicName.
     * group => tenant/namespace/groupName.
     * map   => [key => queueId] & [value => offset].
     **/
    @Getter
    private final ConcurrentHashMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<>(512);

    @Getter
    private final ConcurrentHashMap<ClientTopicName, ConcurrentMap<Integer, PersistentTopic>> pulsarTopicCache =
            new ConcurrentHashMap<>(512);

    private long offsetsRetentionMs;

    private final ConcurrentHashMap<GroupOffsetKey, Long> expireTimeTable =
            new ConcurrentHashMap<>(512);

    public GroupMetaManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() throws Exception {
        log.info("Starting GroupMetaManager service...");

        PulsarClient pulsarClient = brokerController.getBrokerService().getPulsar().getClient();
        this.offsetsRetentionMs = brokerController.getServerConfig().getOffsetsRetentionMinutes() * 60 * 1000;

        this.groupOffsetProducer = pulsarClient.newProducer(Schema.BYTEBUFFER)
                .maxPendingMessages(1000)
                .sendTimeout(5, TimeUnit.SECONDS)
                .enableBatching(false)
                .blockIfQueueFull(false)
                .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPulsarFullName())
                .create();

        this.groupOffsetReader = pulsarClient.newReader(Schema.BYTEBUFFER)
                .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPulsarFullName())
                .startMessageId(MessageId.earliest)
                .readCompacted(true)
                .create();

        offsetReaderExecutor.execute(this::loadOffsets);
//        Thread.sleep(10 * 1000);

        persistOffsetExecutor.scheduleAtFixedRate(() -> {
            try {
                persistOffset();
            } catch (Throwable e) {
                log.error("Persist consumerOffset error.", e);
            }
        }, 1000 * 10, brokerController.getServerConfig().getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        clearOffsetExecutor.scheduleAtFixedRate(() -> {
            try {
                clearOffset();
            } catch (Throwable e) {
                log.error("Clear consumerOffset error.", e);
            }
        }, 1000 * 60, brokerController.getServerConfig().getOffsetsRetentionCheckIntervalMs(), TimeUnit.MILLISECONDS);

        log.info("Start GroupMetaManager service finish.");
    }

    /**
     * load offset.
     */
    private void loadOffsets() {
        log.info("Start load group offset.");
        while (!shuttingDown.get()) {
            try {
                Message<ByteBuffer> message = groupOffsetReader.readNext(1, TimeUnit.SECONDS);
                if (message == null) {
                    continue;
                }

                GroupOffsetKey groupOffsetKey = (GroupOffsetKey) GroupMetaKey
                        .decodeKey(ByteBuffer.wrap(message.getKeyBytes()));

                String rmqGroupName = groupOffsetKey.getGroupName();
                String rmqTopicName = groupOffsetKey.getTopicName();
                int queueId = groupOffsetKey.getPartition();
                ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(rmqGroupName,
                        rmqTopicName);

                // clear expire group offset cache
                if (message.getValue() == null) {
                    offsetTable.remove(clientGroupAndTopicName);
                    expireTimeTable.remove(groupOffsetKey);
                    continue;
                }

                long expireTime = message.getEventTime() + offsetsRetentionMs;
                if (expireTime < System.currentTimeMillis()) {
                    continue;
                }

                expireTimeTable.put(groupOffsetKey, expireTime);

                offsetTable.putIfAbsent(clientGroupAndTopicName, new ConcurrentHashMap<>());

                GroupOffsetValue groupOffsetValue = new GroupOffsetValue();
                groupOffsetValue.decode(message.getValue());
                long offset = groupOffsetValue.getOffset();
                offsetTable.get(clientGroupAndTopicName).put(queueId, offset);
            } catch (Exception e) {
                log.warn("Rop load offset failed.", e);
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * query offset.
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(group, topic);
        ConcurrentMap<Integer, Long> partitionOffsets = offsetTable.get(clientGroupAndTopicName);
        if (partitionOffsets != null) {
            return partitionOffsets.get(queueId);
        }

        long groupOffset = getGroupOffsetFromPulsar(clientGroupAndTopicName, queueId);
        if (groupOffset != -1L) {
            this.offsetTable.putIfAbsent(clientGroupAndTopicName, new ConcurrentHashMap<>());
            this.offsetTable.get(clientGroupAndTopicName).put(queueId, groupOffset);
        }
        return groupOffset;
    }

    private long getGroupOffsetFromPulsar(ClientGroupAndTopicName groupAndTopic, int queueId) {
        try {
            PersistentTopic persistentTopic = getPulsarPersistentTopic(groupAndTopic.getClientTopicName(), queueId);
            String pulsarGroup = groupAndTopic.getClientGroupName().getPulsarGroupName();
            PersistentSubscription subscription = persistentTopic.getSubscription(pulsarGroup);
            if (subscription != null) {
                PositionImpl markDeletedPosition = (PositionImpl) subscription.getCursor().getMarkDeletedPosition();
                MessageIdImpl messageId = new MessageIdImpl(markDeletedPosition.getLedgerId(),
                        markDeletedPosition.getEntryId(), queueId);
                return MessageIdUtils.getOffset(messageId);
            }
        } catch (RopPersistentTopicException ignore) {
        }
        return -1L;
    }

    public void commitOffset(final String group, final String topic, final int queueId, final long offset) {
        // skip commit offset request if this broker not owner for the request queueId topic
        RocketMQTopic rmqTopic = new RocketMQTopic(topic);
        TopicName pulsarTopicName = rmqTopic.getPulsarTopicName();
        if (!this.brokerController.getTopicConfigManager().isPartitionTopicOwner(pulsarTopicName, queueId)) {
            log.debug("Skip this commit offset request because of this broker not owner for the partition topic, "
                    + "topic: {} queueId: {}", topic, queueId);
            return;
        }

        log.debug("When commit offset, the [topic@queueId] is [{}@{}] and the messageID is: {}", topic, queueId,
                MessageIdUtils.getMessageId(offset));
        this.commitOffset(new ClientGroupAndTopicName(group, topic), queueId, offset);
    }

    private void commitOffset(ClientGroupAndTopicName clientGroupAndTopicName, int queueId, long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.computeIfAbsent(clientGroupAndTopicName,
                groupAndTopicName -> new ConcurrentHashMap<>(32));
        map.put(queueId, offset);
    }

    public void persistOffset() {
        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> entry : offsetTable.entrySet()) {
            ClientGroupAndTopicName groupAndTopic = entry.getKey();
            ConcurrentMap<Integer, Long> offsetMap = entry.getValue();

            String pulsarGroup = groupAndTopic.getClientGroupName().getPulsarGroupName();
            if (!isSystemGroup(pulsarGroup)) {

                for (Entry<Integer, Long> entry1 : offsetMap.entrySet()) {
                    int partitionId = entry1.getKey();
                    long offset = entry1.getValue();
                    if (!this.brokerController.getTopicConfigManager().isPartitionTopicOwner(
                            TopicName.get(groupAndTopic.getClientTopicName().getPulsarTopicName()), partitionId)) {
                        continue;
                    }

                    PersistentTopic persistentTopic = null;
                    try {
                        persistentTopic = getPulsarPersistentTopic(groupAndTopic.getClientTopicName(),
                                partitionId);
                    } catch (Exception e) {
                        log.warn("[{}] getPulsarPersistentTopic [{}] error.", groupAndTopic, offset, e);
                    }
                    if (persistentTopic != null) {
                        PersistentSubscription subscription = persistentTopic.getSubscription(pulsarGroup);
                        if (subscription == null) {
                            try {
                                subscription = (PersistentSubscription) persistentTopic
                                        .createSubscription(pulsarGroup, InitialPosition.Earliest, false)
                                        .get();
                            } catch (Exception e) {
                                log.warn("[{}] createSubscription [{}] error.", groupAndTopic, offset, e);
                            }
                        }
                        assert subscription != null;
                        ManagedCursor cursor = subscription.getCursor();
                        PositionImpl markDeletedPosition = (PositionImpl) cursor.getMarkDeletedPosition();
                        PositionImpl commitPosition = MessageIdUtils.getPosition(offset);
                        PositionImpl lastPosition = (PositionImpl) persistentTopic.getLastPosition();

                        if (commitPosition.getEntryId() > 0) {
                            commitPosition = MessageIdUtils.getPosition(offset - 1);
                        }

                        if (commitPosition.compareTo(lastPosition) > 0) {
                            commitPosition = lastPosition;
                            offset = MessageIdUtils.getOffset(new MessageIdImpl(lastPosition.getLedgerId(),
                                    lastPosition.getEntryId() + 1,
                                    partitionId));
                        }

                        String rmqGroupName = groupAndTopic.getClientGroupName().getRmqGroupName();
                        String rmqTopicName = groupAndTopic.getClientTopicName().getRmqTopicName();
                        storeOffset(rmqGroupName, rmqTopicName, partitionId, offset);

                        if (commitPosition.compareTo(markDeletedPosition) > 0) {
                            try {
                                cursor.markDelete(commitPosition);
                                log.debug("[{}] [{}] Mark delete [position = {}] successfully.",
                                        rmqGroupName, rmqTopicName, commitPosition);
                            } catch (Exception e) {
                                log.info("[{}] [{}] Mark delete [position = {}] and deletedPosition[{}] error.",
                                        rmqGroupName, rmqTopicName, commitPosition, markDeletedPosition, e);
                            }
                        } else {
                            log.debug(
                                    "[{}] [{}] Skip mark delete for [position = {}] less than [oldPosition = {}].",
                                    rmqGroupName, rmqTopicName, commitPosition, markDeletedPosition);
                        }
                    }
                }
            }
        }
    }

    /**
     * Clear expire offset.
     */
    private void clearOffset() {
        Set<GroupOffsetKey> clearOffsets = Sets.newHashSet();
        for (Entry<GroupOffsetKey, Long> entry : expireTimeTable.entrySet()) {
            long expireTime = entry.getValue();
            if (System.currentTimeMillis() < expireTime) {
                continue;
            }

            GroupOffsetKey groupOffsetKey = entry.getKey();
            ClientTopicName clientTopicName = new ClientTopicName(groupOffsetKey.getTopicName());
            if (!this.brokerController.getTopicConfigManager().isPartitionTopicOwner(
                    TopicName.get(clientTopicName.getPulsarTopicName()), groupOffsetKey.getPartition())) {
                continue;
            }

            clearOffsets.add(entry.getKey());
        }

        for (GroupOffsetKey clearOffset : clearOffsets) {
            String rmqGroupName = clearOffset.getGroupName();
            String rmqTopicName = clearOffset.getTopicName();
            int queueId = clearOffset.getPartition();
            log.info("[{}] [{}] Clear offset.", rmqGroupName, rmqTopicName);
            storeOffset(rmqGroupName, rmqTopicName, queueId, -1L);
        }
    }

    /**
     * store offset.
     */
    private void storeOffset(final String rmqGroupName, final String rmqTopicName, final int queueId, Long offset) {
        try {
            GroupOffsetKey groupOffsetKey = new GroupOffsetKey();
            groupOffsetKey.setGroupName(rmqGroupName);
            groupOffsetKey.setTopicName(rmqTopicName);
            groupOffsetKey.setPartition(queueId);

            GroupOffsetValue groupOffsetValue = null;
            if (offset > -1) {
                groupOffsetValue = new GroupOffsetValue();
                groupOffsetValue.setOffset(offset);
                groupOffsetValue.setCommitTimestamp(System.currentTimeMillis());
                groupOffsetValue.setExpireTimestamp(System.currentTimeMillis());
            }

            groupOffsetProducer.newMessage()
                    .keyBytes(groupOffsetKey.encode().array())
                    .value(groupOffsetValue == null ? null : groupOffsetValue.encode())
                    .eventTime(System.currentTimeMillis()).sendAsync()
                    .whenCompleteAsync((msgId, e) -> {
                        if (e != null) {
                            log.info("[{}] [{}] Store group offset failed.", rmqGroupName, rmqTopicName, e);
                            return;
                        }
                        if (offset <= -1) {
                            log.info("[{}] [{}] Clear group offset successfully.", rmqGroupName, rmqTopicName);
                            return;
                        }

                        log.info("[{}] [{}] Store group offset [{}] [{}] successfully.",
                                rmqGroupName, rmqTopicName, MessageIdUtils.getMessageId(offset), offset);
                        ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(rmqGroupName,
                                rmqTopicName);
                        offsetTable.putIfAbsent(clientGroupAndTopicName, new ConcurrentHashMap<>());
                        offsetTable.get(clientGroupAndTopicName).put(queueId, offset);
                    }, groupMetaCallbackExecutor);
        } catch (Exception e) {
            log.info("[{}] [{}] Store group offset error.", rmqGroupName, rmqTopicName, e);
        }
    }

    public void shutdown() {
        shuttingDown.set(true);
        persistOffsetExecutor.shutdown();
        offsetReaderExecutor.shutdown();
        groupMetaCallbackExecutor.shutdown();

        groupOffsetProducer.closeAsync();
        groupOffsetReader.closeAsync();
    }


    public void putPulsarTopic(ClientTopicName clientTopicName, int partitionId, PersistentTopic pulsarTopic) {
        if (pulsarTopic == null) {
            return;
        }

        pulsarTopicCache.putIfAbsent(clientTopicName, new ConcurrentHashMap<>());
        pulsarTopicCache.get(clientTopicName).put(partitionId, pulsarTopic);
    }

    public void removePulsarTopic(ClientTopicName clientTopicName, int partitionId) {
        if (pulsarTopicCache.containsKey(clientTopicName)) {
            pulsarTopicCache.get(clientTopicName).remove(partitionId);
            if (pulsarTopicCache.get(clientTopicName).isEmpty()) {
                pulsarTopicCache.remove(clientTopicName);
            }
        }
    }

    public PersistentTopic getPulsarPersistentTopic(ClientTopicName topicName, int partitionId)
            throws RopPersistentTopicException {
        if (isPulsarTopicCached(topicName, partitionId)) {
            return this.pulsarTopicCache.get(topicName).get(partitionId);
        }

        Optional<Topic> topic;
        try {
            topic = getPulsarPersistentTopicAsync(topicName, partitionId).join();
        } catch (Exception e) {
            throw new RopPersistentTopicException(
                    String.format("Get pulsarTopic[%s] and partition[%d] error.", topicName, partitionId));
        }
        if (topic.isPresent()) {
            PersistentTopic persistentTopic = (PersistentTopic) topic.get();
            this.pulsarTopicCache.putIfAbsent(topicName, new ConcurrentHashMap<>());
            this.pulsarTopicCache.get(topicName).putIfAbsent(partitionId, persistentTopic);
            return this.pulsarTopicCache.get(topicName).get(partitionId);
        } else {
            log.warn("Not found PulsarPersistentTopic pulsarTopic: {}, partition: {}", topicName, partitionId);
            throw new RopPersistentTopicException("Not found PulsarPersistentTopic.");
        }
    }

    public CompletableFuture<Optional<Topic>> getPulsarPersistentTopicAsync(ClientTopicName clientTopicName,
            int partitionId) {
        // setup ownership of service unit to this broker
        TopicName pulsarTopicName = TopicName.get(clientTopicName.getPulsarTopicName());
        TopicName partitionTopicName = pulsarTopicName.getPartition(partitionId);
        String topicName = partitionTopicName.toString();
        PulsarService pulsarService = this.brokerController.getBrokerService().pulsar();
        return pulsarService.getBrokerService().getTopic(topicName, true);
    }

    public boolean isSystemGroup(String groupName) {
        return groupName.startsWith(RocketMQTopic.getMetaTenant() + SLASH_CHAR + RocketMQTopic.getMetaNamespace())
                || groupName
                .startsWith(RocketMQTopic.getDefaultTenant() + SLASH_CHAR + RocketMQTopic.getDefaultNamespace());
    }

    private boolean isPulsarTopicCached(ClientTopicName topicName, int partitionId) {
        if (topicName == null) {
            return false;
        }
        return pulsarTopicCache.containsKey(topicName) && pulsarTopicCache.get(topicName).containsKey(partitionId);
    }

}
