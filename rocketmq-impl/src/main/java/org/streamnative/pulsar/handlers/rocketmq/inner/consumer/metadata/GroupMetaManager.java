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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Group meta manager.
 */
@Slf4j
public class GroupMetaManager {

    private final RocketMQBrokerController brokerController;

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /**
     * group offset producer\reader.
     */
    private final Producer<ByteBuffer> groupOffsetProducer;
    private final Reader<ByteBuffer> groupOffsetReader;

    /**
     * group meta producer\reader.
     */
    private final Producer<ByteBuffer> groupMetaProducer;
    private final Reader<ByteBuffer> groupMeatReader;

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

    /**
     * group offset table
     * key   => topic@group.
     * topic => tenant/namespace/topicName.
     * group => tenant/namespace/groupName.
     * map   => [key => queueId] & [value => offset].
     **/
    private final ConcurrentHashMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<>(512);

    public GroupMetaManager(RocketMQBrokerController brokerController) throws Exception {
        this.brokerController = brokerController;
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
    }

    /**
     * load offset.
     */
    private void loadOffsets() {
        while (shuttingDown.get()) {
            try {
                Message<ByteBuffer> message = groupOffsetReader.readNext(1, TimeUnit.SECONDS);

                GroupOffsetKey groupOffsetKey = new GroupOffsetKey();
                groupOffsetKey.decode(ByteBuffer.wrap(message.getKeyBytes()));

                GroupOffsetValue groupOffsetValue = new GroupOffsetValue();
                groupOffsetValue.decode(message.getValue());

                String group = groupOffsetKey.groupName;
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
        if (partitionOffsets == null) {
            return -1;
        }
        return partitionOffsets.getOrDefault(queueId, -1L);
    }

    /**
     * store offset.
     */
    public void storeOffset(final String group, final String topic, final int queueId, long offset) {
        try {
            GroupOffsetKey groupOffsetKey = new GroupOffsetKey();
            groupOffsetKey.setGroupName(group);
            groupOffsetKey.setSubTopic(topic);
            groupOffsetKey.setPartition(queueId);

            GroupOffsetValue groupOffsetValue = new GroupOffsetValue();
            groupOffsetValue.setOffset(offset);
            groupOffsetValue.setCommitTimestamp(System.currentTimeMillis());
            groupOffsetValue.setExpireTimestamp(System.currentTimeMillis());

            groupOffsetProducer.newMessage()
                    .keyBytes(groupOffsetKey.encode().array())
                    .value(groupOffsetValue.encode())
                    .eventTime(System.currentTimeMillis()).sendAsync()
                    .whenCompleteAsync((msgId, e) -> {
                        if (e != null) {
                            log.warn("[{}] [{}] StoreOffsetMessage failed.", group, topic, e);
                            return;
                        }
                        ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(group, topic);
                        offsetTable.putIfAbsent(clientGroupAndTopicName, new ConcurrentHashMap<>());
                        ConcurrentMap<Integer, Long> partitionOffsets = offsetTable
                                .get(clientGroupAndTopicName);
                        partitionOffsets.put(queueId, offset);
                    }, groupMetaCallbackExecutor);
        } catch (Exception e) {
            log.warn("[{}] [{}] StoreOffsetMessage failed.", group, topic, e);
        }
    }

    public void shutdown() {
        shuttingDown.set(true);
        offsetReaderExecutor.shutdown();
        groupMetaCallbackExecutor.shutdown();

        groupOffsetProducer.closeAsync();
        groupOffsetReader.closeAsync();
    }
}
