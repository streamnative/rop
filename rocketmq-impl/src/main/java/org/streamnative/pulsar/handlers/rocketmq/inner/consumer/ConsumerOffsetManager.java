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

import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.SLASH_CHAR;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.UtilAll;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.OffsetFinder;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Consumer offset manager.
 */
@Slf4j
public class ConsumerOffsetManager {

    private final RocketMQBrokerController brokerController;
    /**
     * key   => topic@group.
     * topic => tenant/namespace/topicName.
     * group => tenant/namespace/groupName.
     * map   => [key => queueId] & [value => offset].
     **/
    private final ConcurrentHashMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<>(512);

    private final ConcurrentHashMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> offsetTableSnapshot =
            new ConcurrentHashMap<>(512);

    @Getter
    private final ConcurrentHashMap<ClientTopicName, ConcurrentMap<Integer, PersistentTopic>> pulsarTopicCache =
            new ConcurrentHashMap<>(512);

    public ConsumerOffsetManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    //restore topic cache from pulsar and offset info from pulsar
    public void putPulsarTopic(ClientTopicName clientTopicName, int partitionId, PersistentTopic pulsarTopic) {
        if (pulsarTopic == null) {
            return;
        }

        pulsarTopicCache.putIfAbsent(clientTopicName, new ConcurrentHashMap<>());
        pulsarTopicCache.get(clientTopicName).put(partitionId, pulsarTopic);
        pulsarTopic.getSubscriptions().forEach((grp, grpInfo) -> {
            if (!isSystemGroup(grp)) {
                ClientGroupName clientGroupName;
                ClientGroupAndTopicName groupAtTopic = null;
                try {
                    ManagedCursor cursor = grpInfo.getCursor();
                    PositionImpl readPosition = (PositionImpl) cursor.getReadPosition();
                    clientGroupName = new ClientGroupName(TopicName.get(grp));
                    groupAtTopic = new ClientGroupAndTopicName(clientGroupName, clientTopicName);
                    ConcurrentMap<Integer, Long> partitionOffset = offsetTable.get(groupAtTopic);
                    if (partitionOffset == null) {
                        offsetTable.putIfAbsent(groupAtTopic, new ConcurrentHashMap<>());
                    }
                    offsetTable.get(groupAtTopic).putIfAbsent(partitionId,
                            MessageIdUtils
                                    .getOffset(readPosition.getLedgerId(), readPosition.getEntryId(), partitionId));
                } catch (Exception e) {
                    log.warn("restore subscriptions[groupAtTopic={}] error.", groupAtTopic, e);
                }

            }
        });
    }

    public void removePulsarTopic(ClientTopicName clientTopicName, int partitionId) {
        if (pulsarTopicCache.containsKey(clientTopicName)) {
            pulsarTopicCache.get(clientTopicName).remove(partitionId);
            if (pulsarTopicCache.get(clientTopicName).isEmpty()) {
                pulsarTopicCache.remove(clientTopicName);
            }
        }
    }

    public void scanUnsubscribedTopic() {
        Iterator<Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet()
                .iterator();
        while (it.hasNext()) {
            Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> next = it.next();
            ClientGroupAndTopicName topicAtGroup = next.getKey();
            if (null == brokerController.getConsumerManager()
                    .findSubscriptionData(topicAtGroup.getClientGroupName().getRmqGroupName(),
                            topicAtGroup.getClientTopicName().getRmqTopicName())
                    && this
                    .offsetBehindMuchThanData(topicAtGroup, next.getValue())) {
                it.remove();
                log.warn("remove topic offset, {}", topicAtGroup);
            }
        }
    }

    private boolean offsetBehindMuchThanData(final ClientGroupAndTopicName topicAtGroup,
            ConcurrentMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();

        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            long minOffsetInStore = 0L;
            try {
                minOffsetInStore = getMinOffsetInQueue(topicAtGroup.getClientTopicName(), next.getKey());
            } catch (RopPersistentTopicException ignore) {
            }
            long offsetInPersist = next.getValue();
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }

    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<>();
        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> next : this.offsetTable.entrySet()) {
            ClientGroupName clientGroupName = next.getKey().getClientGroupName();
            if (group.equals(clientGroupName.getRmqGroupName())) {
                String topicAtGroup = next.getKey().getClientTopicName().getRmqTopicName();
                topics.add(topicAtGroup);
            }
        }
        return topics;
    }

    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<>();
        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> next : this.offsetTable.entrySet()) {
            ClientTopicName clientTopicName = next.getKey().getClientTopicName();
            if (topic.equals(clientTopicName.getRmqTopicName())) {
                ClientGroupName clientGroupName = next.getKey().getClientGroupName();
                groups.add(clientGroupName.getRmqGroupName());
            }
        }
        return groups;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
            final long offset) {

        // skip commit offset request if this broker not owner for the request queueId topic
        RocketMQTopic rmqTopic = new RocketMQTopic(topic);
        TopicName pulsarTopicName = rmqTopic.getPulsarTopicName();
        if (!this.brokerController.getTopicConfigManager()
                .isPartitionTopicOwner(pulsarTopicName, queueId)) {
            log.debug("Skip this commit offset request because of this broker not owner for the partition topic, "
                    + "topic: {} queueId: {}", topic, queueId);
            return;
        }

        log.debug("=======================> [{}@{}] ---> {}", topic, queueId, MessageIdUtils.getMessageId(offset));
        ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(group, topic);
        MessageIdImpl messageId = MessageIdUtils.getMessageId(offset);
        long fixedOffset = messageId.getEntryId() > 0 ? offset - 1 : offset; // fixed rocketmq client commit offset + 1
        this.commitOffset(clientHost, clientGroupAndTopicName, queueId, fixedOffset);
    }

    private void commitOffset(final String clientHost, final ClientGroupAndTopicName clientGroupAndTopicName,
            final int queueId, final long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(clientGroupAndTopicName);
        if (null == map) {
            map = new ConcurrentHashMap<>(32);
            map.put(queueId, offset);
            this.offsetTable.put(clientGroupAndTopicName, map);
        } else {
            Long storeOffset = map.get(queueId);
            if (storeOffset == null) {
                map.put(queueId, offset);
            } else if (offset >= storeOffset) {
                map.put(queueId, offset);
            }
        }
    }

    public long queryOffset(final String group, final String topic, final int queueId) {
        ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(group, topic);
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(clientGroupAndTopicName);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null) {
                return offset;
            }
        } else {
            long groupOffset = getGroupOffsetFromPulsar(clientGroupAndTopicName, queueId);
            if (groupOffset != -1L) {
                this.offsetTable.putIfAbsent(clientGroupAndTopicName, new ConcurrentHashMap<>());
                this.offsetTable.get(clientGroupAndTopicName).put(queueId, groupOffset);
            }
            return groupOffset;
        }
        return -1L;
    }

    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {
        Map<Integer, Long> queueMinOffset = new HashMap<>();
        Set<ClientGroupAndTopicName> topicGroups = this.offsetTable.keySet();
        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                topicGroups.removeIf(clientGroupAndTopicName -> group
                        .equals(clientGroupAndTopicName.getClientGroupName().getRmqGroupName()));
            }
        }

        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable
                .entrySet()) {
            ClientGroupAndTopicName topicGroup = offSetEntry.getKey();
            if (topic.equals(topicGroup.getClientTopicName().getRmqTopicName())) {
                for (Entry<Integer, Long> entry : offSetEntry.getValue().entrySet()) {
                    long minOffset = 0L;
                    try {
                        minOffset = getMinOffsetInQueue(topicGroup.getClientTopicName(), entry.getKey());
                    } catch (RopPersistentTopicException ignore) {
                    }
                    if (entry.getValue() >= minOffset) {
                        queueMinOffset.merge(entry.getKey(), entry.getValue(), (a, b) -> Math.min(b, a));
                    }
                }
            }

        }
        return queueMinOffset;
    }

    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        return this.offsetTable.get(new ClientGroupAndTopicName(group, topic));
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(new ClientGroupAndTopicName(srcGroup, topic));
        if (offsets != null) {
            this.offsetTable
                    .put(new ClientGroupAndTopicName(destGroup, topic), new ConcurrentHashMap<>(offsets));
        }
    }

    public long getMinOffsetInQueue(ClientTopicName clientTopicName, int partitionId)
            throws RopPersistentTopicException {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(clientTopicName, partitionId);
        if (persistentTopic != null) {
            try {
                PositionImpl firstPosition = persistentTopic.getFirstPosition();
                return MessageIdUtils.getOffset(firstPosition.getLedgerId(), firstPosition.getEntryId(), partitionId);
            } catch (ManagedLedgerException e) {
                log.warn("getMinOffsetInQueue error, ClientGroupAndTopicName=[{}], partitionId=[{}].", clientTopicName,
                        partitionId);
            }
        }
        return 0L;
    }

    public long getMaxOffsetInQueue(ClientTopicName topicName, int partitionId) throws RopPersistentTopicException {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(topicName, partitionId);
        if (persistentTopic != null) {
            PositionImpl lastPosition = (PositionImpl) persistentTopic.getLastPosition();
            return MessageIdUtils.getOffset(lastPosition.getLedgerId(), lastPosition.getEntryId(), partitionId);
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

    public CompletableFuture<Optional<Topic>> getPulsarPersistentTopicAsync(ClientTopicName clientTopicName,
            int partitionId) {
        // setup ownership of service unit to this broker
        TopicName pulsarTopicName = TopicName.get(clientTopicName.getPulsarTopicName());
        TopicName partitionTopicName = pulsarTopicName.getPartition(partitionId);
        String topicName = partitionTopicName.toString();
        PulsarService pulsarService = this.brokerController.getBrokerService().pulsar();
        return pulsarService.getBrokerService().getTopic(topicName, true);
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

    private long getGroupOffsetFromPulsar(ClientGroupAndTopicName groupAndTopic, int queueId) {
        try {
            PersistentTopic persistentTopic = getPulsarPersistentTopic(groupAndTopic.getClientTopicName(),
                    queueId);
            String pulsarGroup = groupAndTopic.getClientGroupName().getPulsarGroupName();
            PersistentSubscription subscription = persistentTopic.getSubscription(pulsarGroup);
            if (subscription != null) {
                PositionImpl markDeletedPosition = (PositionImpl) subscription.getCursor()
                        .getMarkDeletedPosition();
                MessageIdImpl messageId = new MessageIdImpl(markDeletedPosition.getLedgerId(),
                        markDeletedPosition.getEntryId(), queueId);
                return MessageIdUtils.getOffset(messageId);
            }
        } catch (RopPersistentTopicException ignore) {
        }
        return -1L;
    }

    private boolean isSystemGroup(String groupName) {
        return groupName.startsWith(RocketMQTopic.getMetaTenant() + SLASH_CHAR + RocketMQTopic.getMetaNamespace())
                || groupName
                .startsWith(RocketMQTopic.getDefaultTenant() + SLASH_CHAR + RocketMQTopic.getDefaultNamespace());
    }

    public synchronized void persist() {
        ConcurrentHashMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> neededPersistOffsets =
                neededPersistentOffsetTable();
        if (neededPersistOffsets != null && !neededPersistOffsets.isEmpty()) {
            neededPersistOffsets.forEach((groupAndTopic, offsetMap) -> {
                String pulsarGroup = groupAndTopic.getClientGroupName().getPulsarGroupName();
                if (!isSystemGroup(pulsarGroup)) {
                    offsetMap.forEach((partitionId, offset) -> {
                        try {
                            PersistentTopic persistentTopic = getPulsarPersistentTopic(
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
                                if (commitPosition.compareTo(markDeletedPosition) > 0
                                        && commitPosition.compareTo(lastPosition) <= 0) {
                                    try {
                                        cursor.markDelete(commitPosition);
                                        log.debug("markDelete commit offset [position = {}] successfully.",
                                                commitPosition);
                                    } catch (Exception e) {
                                        log.info("commit offset [position = {}] and deletedPosition[{}] error.",
                                                commitPosition, markDeletedPosition, e);
                                    }
                                } else {
                                    log.info("skip commit offset, for [position = {}] less than [oldPosition = {}].",
                                            commitPosition, markDeletedPosition);
                                }
                            }
                        } catch (Exception e) {
                            log.warn("persist topic[{}] offset[{}] error. Exception: ", groupAndTopic, offset, e);
                        }
                    });
                }
            });
        }
        createOffsetTableSnapshot();
    }

    private ConcurrentHashMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> createOffsetTableSnapshot() {
        offsetTableSnapshot.clear();
        offsetTable.forEach((groupAtTopic, partitionedOffset) -> {
            offsetTableSnapshot.putIfAbsent(groupAtTopic, new ConcurrentHashMap<>());
            partitionedOffset
                    .forEach((partition, offset) -> offsetTableSnapshot.get(groupAtTopic).put(partition, offset));
        });
        return offsetTableSnapshot;
    }

    private ConcurrentHashMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> neededPersistentOffsetTable() {
        if (offsetTableSnapshot.isEmpty()) {
            return createOffsetTableSnapshot();
        }
        offsetTableSnapshot.forEach((groupAtTopic, oldPartitionOffset) -> {
            if (offsetTable.containsKey(groupAtTopic)) {
                ConcurrentMap<Integer, Long> newPartitionOffset = offsetTable.get(groupAtTopic);
                newPartitionOffset.forEach((partition, newOffset) -> {
                    if (oldPartitionOffset.containsKey(partition) && oldPartitionOffset.get(partition) >= newOffset) {
                        oldPartitionOffset.remove(partition);
                        if (oldPartitionOffset.isEmpty()) {
                            offsetTableSnapshot.remove(groupAtTopic);
                        }
                    } else {
                        oldPartitionOffset.put(partition, newPartitionOffset.get(partition));
                    }
                });
            } else {
                offsetTableSnapshot.remove(groupAtTopic);
            }
        });
        return offsetTableSnapshot;
    }

    private boolean isPulsarTopicCached(ClientTopicName topicName, int partitionId) {
        if (topicName == null) {
            return false;
        }
        return pulsarTopicCache.containsKey(topicName) && pulsarTopicCache.get(topicName)
                .containsKey(partitionId);
    }

}
