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
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.rocketmq.common.UtilAll;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupMetaManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetKey;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetValue;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.OffsetFinder;

/**
 * Consumer offset manager.
 */
@Slf4j
public class ConsumerOffsetManager {

    private final GroupMetaManager groupMetaManager;
    private final RocketMQBrokerController brokerController;

    public ConsumerOffsetManager(RocketMQBrokerController brokerController, GroupMetaManager groupMetaManager) {
        this.brokerController = brokerController;
        this.groupMetaManager = groupMetaManager;
    }

    //restore topic cache from pulsar and offset info from pulsar
    public void putPulsarTopic(ClientTopicName clientTopicName, int partitionId, PersistentTopic pulsarTopic) {
        groupMetaManager.putPulsarTopic(clientTopicName, partitionId, pulsarTopic);
    }

    public void removePulsarTopic(ClientTopicName clientTopicName, int partitionId) {
        groupMetaManager.removePulsarTopic(clientTopicName, partitionId);
    }

    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<>();
        for (Entry<GroupOffsetKey, GroupOffsetValue> next : groupMetaManager.getOffsetTable()
                .asMap().entrySet()) {
            ClientGroupName clientGroupName = new ClientGroupName(next.getKey().getGroupName());
            if (group.equals(clientGroupName.getRmqGroupName())) {
                topics.add(next.getKey().getTopicName());
            }
        }
        return topics;
    }

    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<>();
        for (Entry<GroupOffsetKey, GroupOffsetValue> next : groupMetaManager.getOffsetTable()
                .asMap().entrySet()) {
            ClientTopicName clientTopicName = new ClientTopicName(next.getKey().getTopicName());
            if (topic.equals(clientTopicName.getRmqTopicName())) {
                groups.add(next.getKey().getGroupName());
            }
        }
        return groups;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
            final long offset) {
        groupMetaManager.commitOffset(group, topic, queueId, offset);
    }

    public long queryOffset(final String group, final String topic, final int queueId) {
        return groupMetaManager.queryOffset(group, topic, queueId);
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
                            .getPulsarTopicPartitionId(searchTopic.toPulsarTopicName(), key.getQueueId());
                    minOffset = getMinOffsetInQueue(searchTopic, pulsarPartitionId);
                } catch (Exception ex) {
                    log.warn("get Pulsar Topic PartitionId error: ", ex);
                }
                if (groupOffsetMap.get(key).getOffset() >= minOffset) {
                    queueMinOffset.merge(key.getQueueId(),
                            groupOffsetMap.get(key).getOffset(), (a, b) -> Math.min(b, a));
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
        }).collect(toMap(entry -> entry.getKey().getQueueId()
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
                            key.getQueueId(), value.getOffset());
        });
    }

    public long getMinOffsetInQueue(ClientTopicName clientTopicName, int pulsarPartitionId)
            throws RopPersistentTopicException {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(clientTopicName, pulsarPartitionId);
        if (persistentTopic != null) {
            PositionImpl firstPosition = MessageIdUtils.getFirstPosition(persistentTopic.getManagedLedger());
            return MessageIdUtils.getQueueOffsetByPosition(persistentTopic, firstPosition);
        }
        throw new RopPersistentTopicException("PersistentTopic isn't exists when getMinOffsetInQueue.");
    }

    public long getMaxOffsetInQueue(ClientTopicName topicName, int pulsarPartitionId)
            throws RopPersistentTopicException {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(topicName, pulsarPartitionId);
        if (persistentTopic != null) {
            Position lastPosition = MessageIdUtils.getLastPosition(persistentTopic.getManagedLedger());
            return MessageIdUtils.getQueueOffsetByPosition(persistentTopic, lastPosition);
        }
        throw new RopPersistentTopicException("PersistentTopic isn't exists when getMaxOffsetInQueue.");
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
            OffsetFinder offsetFinder = new OffsetFinder((ManagedLedgerImpl) persistentTopic.getManagedLedger());

            CompletableFuture<Long> finalOffset = new CompletableFuture<>();
            offsetFinder.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
                @Override
                public void findEntryComplete(Position position, Object ctx) {
                    if (position == null) {
                        finalOffset.complete(-1L);
                    } else {
                        PositionImpl finalPosition = (PositionImpl) position;
                        long offset = MessageIdUtils
                                .getOffset(finalPosition.getLedgerId(), finalPosition.getEntryId(), partitionId);
                        finalOffset.complete(offset);
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
}
