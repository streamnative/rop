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

package org.streamnative.pulsar.handlers.rocketmq.inner.consumer;

import static java.util.stream.Collectors.toMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupMetaManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetKey;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetValue;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.metrics.RopMetricsGroup;
import org.streamnative.pulsar.handlers.rocketmq.metrics.RopYammerMetrics;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.OffsetFinder;

/**
 * Consumer offset manager.
 */
@Slf4j
public class ConsumerOffsetManager extends RopMetricsGroup {

    private final GroupMetaManager groupMetaManager;
    private final RocketMQBrokerController brokerController;

    public ConsumerOffsetManager(RocketMQBrokerController brokerController, GroupMetaManager groupMetaManager) {
        this.brokerController = brokerController;
        this.groupMetaManager = groupMetaManager;
    }

    public void start() {
        brokerController.getBrokerService().getPulsar().addPrometheusRawMetricsProvider(this);
    }

    //restore topic cache from pulsar and offset info from pulsar
    public void putPulsarTopic(ClientTopicName clientTopicName, int pulsarPartitionId, PersistentTopic pulsarTopic) {
        groupMetaManager.putPulsarTopic(clientTopicName, pulsarPartitionId, pulsarTopic);
    }

    public void removePulsarTopic(ClientTopicName clientTopicName, int pulsarPartitionId) {
        groupMetaManager.removePulsarTopic(clientTopicName, pulsarPartitionId);
    }

    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<>();
        ClientGroupName clientGroupName = new ClientGroupName(group);
        for (Entry<GroupOffsetKey, GroupOffsetValue> next : groupMetaManager.getOffsetTable().asMap().entrySet()) {
            if (clientGroupName.getPulsarGroupName().equals(next.getKey().getGroupName())) {
                ClientTopicName clientTopicName = new ClientTopicName(TopicName.get(next.getKey().getTopicName()));
                topics.add(clientTopicName.getRmqTopicName());
            }
        }
        return topics;
    }

    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<>();
        for (Entry<GroupOffsetKey, GroupOffsetValue> next : groupMetaManager.getOffsetTable().asMap().entrySet()) {
            ClientTopicName clientTopicName = new ClientTopicName(TopicName.get(next.getKey().getTopicName()));
            if (topic.equals(clientTopicName.getRmqTopicName())) {
                groups.add(new ClientGroupName(TopicName.get(next.getKey().getGroupName())).getRmqGroupName());
            }
        }
        return groups;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
            final long offset) {
        groupMetaManager.commitOffset(group, topic, queueId, offset);
    }

    public void commitOffsetByPartitionId(final String clientHost, final String group, final String topic,
            final int partitionId,
            final long offset) {
        groupMetaManager.commitOffsetByPartitionId(group, topic, partitionId, offset);
    }

    public long queryOffset(final String group, final String topic, final int queueId) {
        return groupMetaManager.queryOffset(group, topic, queueId);
    }

    public long queryOffsetByPartitionId(final String group, final String topic, final int partitionId) {
        return groupMetaManager.queryOffsetByPartitionId(group, topic, partitionId);
    }

    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {
        Map<Integer, Long> queueMinOffset = new HashMap<>();
        ConcurrentMap<GroupOffsetKey, GroupOffsetValue> groupOffsetMap = groupMetaManager
                .getOffsetTable().asMap();
        Set<GroupOffsetKey> topicGroups = groupOffsetMap.keySet();
        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                topicGroups.removeIf(groupOffsetKey -> group
                        .equals(groupOffsetKey.getGroupName()));
            }
        }

        ClientTopicName searchTopic = new ClientTopicName(topic);
        for (GroupOffsetKey key : topicGroups) {
            if (searchTopic.getPulsarTopicName().equals(key.getTopicName())) {
                long minOffset = 0L;
                try {
                    int pulsarPartitionId = brokerController.getRopBrokerProxy()
                            .getPulsarTopicPartitionId(searchTopic.toPulsarTopicName(), key.getPulsarPartitionId());
                    minOffset = getMinOffsetInQueue(searchTopic, pulsarPartitionId);
                } catch (Exception ex) {
                    log.warn("get Pulsar Topic PartitionId error: ", ex);
                }
                for (Map.Entry<GroupOffsetKey, GroupOffsetValue> entry : groupOffsetMap.entrySet()) {
                    if (entry.getValue().getOffset() >= minOffset) {
                        queueMinOffset.merge(entry.getKey().getPulsarPartitionId(),
                                entry.getValue().getOffset(), (a, b) -> Math.min(b, a));
                    }
                }
            }
        }
        return queueMinOffset;
    }

    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(group, topic);
        ConcurrentMap<GroupOffsetKey, GroupOffsetValue> groupOffsetMap = groupMetaManager
                .getOffsetTable().asMap();

        return groupOffsetMap.entrySet().stream().filter(entry -> {
            GroupOffsetKey key = entry.getKey();
            return key.getTopicName().equals(groupAndTopicName.getClientTopicName().getPulsarTopicName())
                    && key.getGroupName().equals(groupAndTopicName.getClientGroupName().getPulsarGroupName());
        }).collect(toMap(entry -> entry.getKey().getPulsarPartitionId()
                , entry -> entry.getValue().getOffset()));
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ClientGroupAndTopicName srcGroupAndTopic = new ClientGroupAndTopicName(srcGroup, topic);
        ClientGroupAndTopicName destGroupAndTopic = new ClientGroupAndTopicName(destGroup, topic);

        groupMetaManager.getOffsetTable().asMap().entrySet().stream().filter((entry) -> {
            GroupOffsetKey key = entry.getKey();
            return key.getTopicName().equals(srcGroupAndTopic.getClientTopicName().getPulsarTopicName())
                    && key.getGroupName().equals(srcGroupAndTopic.getClientGroupName().getPulsarGroupName());
        }).forEach((srcEntry) -> {
            GroupOffsetKey key = srcEntry.getKey();
            GroupOffsetValue value = srcEntry.getValue();
            groupMetaManager
                    .commitOffset(key.getTopicName(), destGroupAndTopic.getClientGroupName().getPulsarGroupName(),
                            key.getPulsarPartitionId(), value.getOffset());
        });
    }

    /**
     * @param topic rocketmq topic name
     * @param queueId rocketmq queue id
     * @return
     */
    public long getMinOffsetInQueue(String topic, int queueId) {
        ClientTopicName clientTopicName = new ClientTopicName(topic);
        int pulsarPartitionId = brokerController.getRopBrokerProxy()
                .getPulsarTopicPartitionId(clientTopicName.toPulsarTopicName(), queueId);
        try {
            return getMinOffsetInQueue(clientTopicName, pulsarPartitionId);
        } catch (RopPersistentTopicException e) {
            return 0L;
        }
    }

    public long getMinOffsetInQueue(ClientTopicName clientTopicName, int pulsarPartitionId)
            throws RopPersistentTopicException {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(clientTopicName, pulsarPartitionId);
        PositionImpl firstPosition = MessageIdUtils.getFirstPosition(persistentTopic.getManagedLedger());
        return MessageIdUtils.getQueueOffsetByPosition(persistentTopic, firstPosition);
    }

    /**
     * @param topic rocketmq topic name
     * @param queueId rocketmq queue id
     * @return
     */
    public long getMaxOffsetInQueue(String topic, int queueId) throws RopPersistentTopicException {
        ClientTopicName clientTopicName = new ClientTopicName(topic);
        int pulsarPartitionId = brokerController.getRopBrokerProxy()
                .getPulsarTopicPartitionId(clientTopicName.toPulsarTopicName(), queueId);
        try {
            return getMaxOffsetInPulsarPartition(clientTopicName, pulsarPartitionId);
        } catch (RopPersistentTopicException ex) {
            log.warn("[{}] queueId: [{}] getMaxOffsetInQueue can't get on unowned broker.", topic, queueId, ex);
            throw ex;
        }
    }

    /**
     * @param topic rocketmq topic name
     * @param partitionId pulsar partition id
     * @return
     */
    public long getMaxOffsetInPartitionId(String topic, int partitionId) throws RopPersistentTopicException {
        ClientTopicName clientTopicName = new ClientTopicName(topic);
        try {
            return getMaxOffsetInPulsarPartition(clientTopicName, partitionId);
        } catch (RopPersistentTopicException ex) {
            log.warn("[{}] partition: [{}] getMaxOffsetInQueue can't get on unowned broker.", topic, partitionId, ex);
            throw ex;
        }
    }

    public long getMaxOffsetInPulsarPartition(ClientTopicName topicName, int pulsarPartitionId)
            throws RopPersistentTopicException {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(topicName, pulsarPartitionId);
        long lastMessageIndex = MessageIdUtils.getLastMessageIndex(persistentTopic.getManagedLedger());
        return lastMessageIndex < 0 ? 0L : lastMessageIndex;
    }

    public long getNextOffset(ClientTopicName topicName, int pulsarPartitionId)
            throws RopPersistentTopicException {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(topicName, pulsarPartitionId);
        long lastMessageIndex = MessageIdUtils.getLastMessageIndex(persistentTopic.getManagedLedger());
        return Math.max(lastMessageIndex + 1, 0L);
    }

    public long getLastTimestamp(ClientTopicName topicName, int pulsarPartitionId) {
        try {
            PersistentTopic persistentTopic = getPulsarPersistentTopic(topicName, pulsarPartitionId);
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();

            PositionImpl position = (PositionImpl) managedLedger.getLastConfirmedEntry();
            if (!managedLedger.ledgerExists(position.getLedgerId())) {
                log.info("[{}] [{}] position is not found, maybe has been deleted.", topicName.getPulsarTopicName(),
                        position);
                return 0L;
            }
            if (position.getEntryId() < 0L) {
                return 0L;
            }

            final CompletableFuture<Long> future = new CompletableFuture<>();
            managedLedger.asyncReadEntry((PositionImpl) managedLedger.getLastConfirmedEntry(),
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            future.completeExceptionally(exception);
                        }

                        @Override
                        public void readEntryComplete(org.apache.bookkeeper.mledger.Entry entry, Object ctx) {
                            MessageImpl msg = null;
                            try {
                                msg = MessageImpl.deserialize(entry.getDataBuffer());
                                future.complete(msg.getPublishTime());
                            } catch (Exception e) {
                                future.completeExceptionally(e);
                            } finally {
                                entry.release();
                                if (msg != null) {
                                    msg.recycle();
                                }
                            }
                        }
                    }, null);
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("[{}] partition [{}] getLastTimestamp error", topicName, pulsarPartitionId, e);
        }
        return 0L;
    }


    public long searchOffsetByTimestamp(ClientGroupAndTopicName groupAndTopic, int partitionId, long timestamp) {
        PersistentTopic persistentTopic;
        try {
            persistentTopic = getPulsarPersistentTopic(groupAndTopic.getClientTopicName(), partitionId);
        } catch (RopPersistentTopicException e) {
            return -1L;
        }
        if (persistentTopic != null) {
            // find with real wanted timestamp
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            OffsetFinder offsetFinder = new OffsetFinder(managedLedger);

            CompletableFuture<Long> finalOffset = new CompletableFuture<>();
            offsetFinder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
                @Override
                public void findEntryComplete(Position position, Object ctx) {
                    if (position == null) {
                        finalOffset.complete(-1L);
                    } else {
                        MessageIdUtils.getOffsetOfPosition(managedLedger,
                                (PositionImpl) position, true, timestamp).whenComplete((offset, throwable) -> {
                            if (throwable != null) {
                                log.error("[{}] Failed to get offset for position {}", persistentTopic.getName(),
                                        position, throwable);
                                return;
                            }
                            finalOffset.complete(offset);
                        });
                    }
                }

                @Override
                public void findEntryFailed(ManagedLedgerException exception,
                        Optional<Position> position, Object ctx) {
                    log.warn("Unable to find position for topic {} time {}. Exception:",
                            groupAndTopic.getClientTopicName(), timestamp, exception);
                    finalOffset.complete(-1L);
                }
            });

            try {
                return finalOffset.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.warn("SearchOffsetByTimestamp: topic [{}] search offset timeout.", persistentTopic.getName());
            }
        } else {
            log.warn("SearchOffsetByTimestamp: topic [{}] not found",
                    groupAndTopic.getClientTopicName().getPulsarTopicName());
        }
        return -1L;
    }

    public PersistentTopic getPulsarPersistentTopic(ClientTopicName topicName, int pulsarPartitionId)
            throws RopPersistentTopicException {
        return groupMetaManager.getPulsarPersistentTopic(topicName, pulsarPartitionId);
    }

    @Override
    public void generate(SimpleTextOutputStream stream) {
        log.info("RoP generate backlog relate metrics.");

        this.brokerController.getBrokerService().getTopics().forEach((originPulsarTopicName, future) -> {
            try {
                Optional<Topic> optionalTopic = BrokerService.extractTopic(future);
                if (optionalTopic.isPresent()) {
                    Topic topic = optionalTopic.get();
                    if (!(topic instanceof PersistentTopic)) {
                        return;
                    }

                    TopicName topicName = TopicName.get(originPulsarTopicName);
                    int pulsarPartitionId = topicName.getPartitionIndex();
                    if (pulsarPartitionId < 0) {
                        return;
                    }

                    topicName = TopicName.get(topicName.getPartitionedTopicName());
                    String localName = topicName.getLocalName();

                    // 获取到rop主题名
                    String ropTopicName;
                    if (localName.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        ropTopicName = MixAll.RETRY_GROUP_TOPIC_PREFIX + topicName.getTenant() + "|" + topicName
                                .getNamespaceObject().getLocalName() + "%" + topicName.getLocalName().substring(7);
                    } else if (localName.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                        ropTopicName =
                                MixAll.DLQ_GROUP_TOPIC_PREFIX + topicName.getTenant() + "|" + topicName
                                        .getNamespaceObject()
                                        .getLocalName() + "%" + topicName.getLocalName().substring(5);
                    } else {
                        ropTopicName = topicName.getTenant() + "|" + topicName.getNamespaceObject().getLocalName() + "%"
                                + topicName.getLocalName();
                    }

                    // 移除不存在的消费组
                    Set<String> ropGroups = whichGroupByTopic(ropTopicName);
                    ropGroups.removeIf(rmqGroup -> {
                        ClientGroupName clientGroupName = new ClientGroupName(rmqGroup);
                        return !brokerController.getRopBrokerProxy().getZkService()
                                .isGroupExist(clientGroupName.getPulsarGroupName());
                    });

                    PersistentTopic persistentTopic = (PersistentTopic) topic;

                    // 查询每个分区主题的最大offset
                    long brokerOffset = MessageIdUtils.getLastMessageIndex(persistentTopic.getManagedLedger());

                    for (String rmqGroupName : ropGroups) {
                        // 查询每个消费组的提交offset
                        long consumerOffset = queryOffsetByPartitionId(rmqGroupName, ropTopicName, pulsarPartitionId);

                        // 计算每个分区的积压数量
                        long msgBacklog = consumerOffset <= 0 ? Math.max(0, brokerOffset)
                                : Math.max(0, brokerOffset - consumerOffset + 1);

                        long lastTimestamp = this.brokerController.getConsumerOffsetManager()
                                .getLastTimestamp(new ClientTopicName(ropTopicName), pulsarPartitionId);
                        if (lastTimestamp <= 0) {
                            msgBacklog = 0;
                        }

                        // 记录到对应分区的 rop_msg_backlog gauge下
                        ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(rmqGroupName,
                                ropTopicName);
                        String pulsarGroupName = clientGroupAndTopicName.getClientGroupName().getPulsarGroupName();
                        String pulsarTopicName = clientGroupAndTopicName.getClientTopicName().getPulsarTopicName();

                        stream.write(RopYammerMetrics.ROP_MSG_BACKLOG)
                                .write("{cluster=\"").write(this.brokerController.getRopClusterName())
                                .write("\",topic=\"").write(pulsarTopicName + "-partition-" + pulsarPartitionId)
                                .write("\",group=\"").write(pulsarGroupName).write("\"} ");
                        stream.write(msgBacklog).write(' ').write(System.currentTimeMillis()).write('\n');
                    }

                }
            } catch (Exception e) {
                log.warn("RoP [{}] gatherBacklogMetrics failed.", originPulsarTopicName, e);
            }
        });
    }
}
