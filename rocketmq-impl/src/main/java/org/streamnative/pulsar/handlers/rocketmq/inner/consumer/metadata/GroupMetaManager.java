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

import java.nio.ByteBuffer;
import java.util.Map.Entry;
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
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.ConsumerOffsetManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Group meta manager.
 */
@Slf4j
public class GroupMetaManager {

    private final RocketMQBrokerController brokerController;
    private final ConsumerOffsetManager consumerOffsetManager;

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /**
     * group offset producer\reader.
     */
    private Producer<ByteBuffer> groupOffsetProducer;
    private Reader<ByteBuffer> groupOffsetReader;

    /**
     * group meta producer\reader.
     */
    private Producer<ByteBuffer> groupMetaProducer;
    private Reader<ByteBuffer> groupMeatReader;

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
     * group meta executor.
     */
    private final ExecutorService metaReaderExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("Rop-group-meta-reader");
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

    public GroupMetaManager(RocketMQBrokerController brokerController, ConsumerOffsetManager consumerOffsetManager) {
        this.brokerController = brokerController;
        this.consumerOffsetManager = consumerOffsetManager;
    }

    public void start() throws Exception {
        PulsarClient pulsarClient = brokerController.getBrokerService().getPulsar().getClient();

        this.groupOffsetProducer = pulsarClient.newProducer(Schema.BYTEBUFFER)
                .maxPendingMessages(1000)
                .sendTimeout(5, TimeUnit.SECONDS)
                .enableBatching(true)
                .blockIfQueueFull(false)
                .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPulsarFullName())
                .create();

        this.groupOffsetReader = pulsarClient.newReader(Schema.BYTEBUFFER)
                .topic(RocketMQTopic.getGroupMetaOffsetTopic().getPulsarFullName())
                .startMessageId(MessageId.earliest)
                .readCompacted(true)
                .create();

        this.groupMetaProducer = pulsarClient.newProducer(Schema.BYTEBUFFER)
                .maxPendingMessages(1000)
                .sendTimeout(5, TimeUnit.SECONDS)
                .enableBatching(true)
                .blockIfQueueFull(false)
                .topic(RocketMQTopic.getGroupMetaSubscriptionTopic().getPulsarFullName())
                .create();

        this.groupMeatReader = pulsarClient.newReader(Schema.BYTEBUFFER)
                .topic(RocketMQTopic.getGroupMetaSubscriptionTopic().getPulsarFullName())
                .startMessageId(MessageId.earliest)
                .readCompacted(true)
                .create();

        offsetReaderExecutor.execute(this::loadOffsets);

        persistOffsetExecutor.scheduleAtFixedRate(() -> {
            try {
                persist();
            } catch (Throwable e) {
                log.error("Persist consumerOffset error.", e);
            }
        }, 1000 * 10, brokerController.getServerConfig().getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * load offset.
     */
    private void loadOffsets() {
        while (!shuttingDown.get()) {
            try {
                Message<ByteBuffer> message = groupOffsetReader.readNext(1, TimeUnit.SECONDS);
                if (message == null) {
                    continue;
                }

                GroupOffsetKey groupOffsetKey = (GroupOffsetKey) GroupMetaKey
                        .decodeKey(ByteBuffer.wrap(message.getKeyBytes()));

                GroupOffsetValue groupOffsetValue = new GroupOffsetValue();
                groupOffsetValue.decode(message.getValue());

                String group = groupOffsetKey.getGroupName();
                String topic = groupOffsetKey.getSubTopic();
                int queueId = groupOffsetKey.getPartition();
                long offset = groupOffsetValue.getOffset();

                ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(group, topic);
                offsetTable.putIfAbsent(clientGroupAndTopicName, new ConcurrentHashMap<>());
                ConcurrentMap<Integer, Long> partitionOffsets = offsetTable.get(clientGroupAndTopicName);
                partitionOffsets.put(queueId, offset);
            } catch (Exception e) {
                log.warn("Rop load offset failed.", e);
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
            PersistentTopic persistentTopic = consumerOffsetManager
                    .getPulsarPersistentTopic(groupAndTopic.getClientTopicName(), queueId);
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

    private void commitOffset(final ClientGroupAndTopicName clientGroupAndTopicName, final int queueId,
            final long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(clientGroupAndTopicName);
        if (null == map) {
            map = new ConcurrentHashMap<>(32);
            map.put(queueId, offset);
            this.offsetTable.put(clientGroupAndTopicName, map);
            return;
        }

        Long storeOffset = map.get(queueId);
        if (storeOffset == null || offset >= storeOffset) {
            map.put(queueId, offset);
        }
    }

    public synchronized void persist() {
        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> entry : offsetTable.entrySet()) {
            ClientGroupAndTopicName groupAndTopic = entry.getKey();
            ConcurrentMap<Integer, Long> offsetMap = entry.getValue();

            String pulsarGroup = groupAndTopic.getClientGroupName().getPulsarGroupName();
            if (!consumerOffsetManager.isSystemGroup(pulsarGroup)) {

                for (Entry<Integer, Long> entry1 : offsetMap.entrySet()) {
                    int partitionId = entry1.getKey();
                    long offset = entry1.getValue();
                    try {
                        if (!this.brokerController.getTopicConfigManager().isPartitionTopicOwner(
                                TopicName.get(groupAndTopic.getClientTopicName().getPulsarTopicName()),
                                partitionId)) {
                            continue;
                        }

                        PersistentTopic persistentTopic = consumerOffsetManager.getPulsarPersistentTopic(
                                groupAndTopic.getClientTopicName(),
                                partitionId);
                        if (persistentTopic != null) {
                            PersistentSubscription subscription = persistentTopic.getSubscription(pulsarGroup);
                            if (subscription == null) {
                                subscription = (PersistentSubscription) persistentTopic
                                        .createSubscription(pulsarGroup,
                                                InitialPosition.Earliest,
                                                false)
                                        .get();
                            }
                            ManagedCursor cursor = subscription.getCursor();
                            PositionImpl markDeletedPosition = (PositionImpl) cursor.getMarkDeletedPosition();
                            PositionImpl commitPosition = MessageIdUtils.getPosition(offset);
                            PositionImpl lastPosition = (PositionImpl) persistentTopic.getLastPosition();

                            /*
                             * If the consumer has submitted the offset to the last position,
                             * when the broker restarts, the last message will be consumed repeatedly.
                             */
                            if (commitPosition.compareTo(lastPosition) > 0) {
                                commitPosition = lastPosition;
                                offset = MessageIdUtils.getOffset(
                                        new MessageIdImpl(commitPosition.getLedgerId(),
                                                commitPosition.getEntryId() + 1,
                                                partitionId));
                            }

                            String rmqGroupName = groupAndTopic.getClientGroupName().getRmqGroupName();
                            String rmqTopicName = groupAndTopic.getClientTopicName().getRmqTopicName();
                            storeOffset(rmqGroupName, rmqTopicName, partitionId, offset);

                            if (commitPosition.compareTo(markDeletedPosition) > 0) {
                                try {
                                    cursor.markDelete(commitPosition);
                                    log.debug("[{}] [{}] mark delete [position = {}] successfully.",
                                            rmqGroupName, rmqTopicName, commitPosition);
                                } catch (Exception e) {
                                    log.info("[{}] [{}] mark delete [position = {}] and deletedPosition[{}] error.",
                                            rmqGroupName, rmqTopicName, commitPosition, markDeletedPosition, e);
                                }
                            } else {
                                log.debug(
                                        "[{}] [{}] skip mark delete for [position = {}] less than [oldPosition = {}].",
                                        rmqGroupName, rmqTopicName, commitPosition, markDeletedPosition);
                            }
                        }
                    } catch (Exception e) {
                        log.warn("persist topic[{}] offset[{}] error. Exception: ", groupAndTopic, offset, e);
                    }
                }
            }
        }
    }

    /**
     * store offset.
     */
    private void storeOffset(final String rmqGroupName, final String rmqTopicName, final int queueId, long offset) {
        try {
            GroupOffsetKey groupOffsetKey = new GroupOffsetKey();
            groupOffsetKey.setGroupName(rmqGroupName);
            groupOffsetKey.setSubTopic(rmqTopicName);
            groupOffsetKey.setPartition(queueId);

            GroupOffsetValue groupOffsetValue = new GroupOffsetValue();
            groupOffsetValue.setOffset(offset);
            groupOffsetValue.setCommitTimestamp(System.currentTimeMillis());
            groupOffsetValue.setExpireTimestamp(System.currentTimeMillis());

            log.info("[{}] [{}] Store offset [{}] [{}] successfully.",
                    rmqGroupName, rmqTopicName, MessageIdUtils.getMessageId(offset), offset);
            groupOffsetProducer.newMessage()
                    .keyBytes(groupOffsetKey.encode().array())
                    .value(groupOffsetValue.encode())
                    .eventTime(System.currentTimeMillis()).sendAsync()
                    .whenCompleteAsync((msgId, e) -> {
                        if (e != null) {
                            log.warn("[{}] [{}] StoreOffsetMessage failed.", rmqGroupName, rmqTopicName, e);
                            return;
                        }
                        ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(rmqGroupName,
                                rmqTopicName);
                        offsetTable.putIfAbsent(clientGroupAndTopicName, new ConcurrentHashMap<>());
                        ConcurrentMap<Integer, Long> partitionOffsets = offsetTable
                                .get(clientGroupAndTopicName);
                        partitionOffsets.put(queueId, offset);
                    }, groupMetaCallbackExecutor);
        } catch (Exception e) {
            log.warn("[{}] [{}] StoreOffsetMessage failed.", rmqGroupName, rmqTopicName, e);
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
}
