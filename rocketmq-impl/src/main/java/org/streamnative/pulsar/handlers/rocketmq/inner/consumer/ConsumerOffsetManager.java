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
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.UtilAll;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupMetaManager;
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
    private final GroupMetaManager groupMetaManager;

    @Getter
    private final ConcurrentHashMap<ClientTopicName, ConcurrentMap<Integer, PersistentTopic>> pulsarTopicCache =
            new ConcurrentHashMap<>(512);

    public ConsumerOffsetManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.groupMetaManager = new GroupMetaManager(brokerController, this);
    }

    public void start() throws Exception {
        groupMetaManager.start();
    }

    public void shutdown() {
        groupMetaManager.shutdown();
    }

    //restore topic cache from pulsar and offset info from pulsar
    public void putPulsarTopic(ClientTopicName clientTopicName, int partitionId, PersistentTopic pulsarTopic) {
        if (pulsarTopic == null) {
            return;
        }

        pulsarTopicCache.putIfAbsent(clientTopicName, new ConcurrentHashMap<>());
        pulsarTopicCache.get(clientTopicName).put(partitionId, pulsarTopic);
//        pulsarTopic.getSubscriptions().forEach((grp, grpInfo) -> {
//            if (!isSystemGroup(grp)) {
//                ClientGroupName clientGroupName;
//                ClientGroupAndTopicName groupAtTopic = null;
//                try {
//                    ManagedCursor cursor = grpInfo.getCursor();
//                    PositionImpl readPosition = (PositionImpl) cursor.getReadPosition();
//                    clientGroupName = new ClientGroupName(TopicName.get(grp));
//                    groupAtTopic = new ClientGroupAndTopicName(clientGroupName, clientTopicName);
//                    ConcurrentMap<Integer, Long> partitionOffset = offsetTable.get(groupAtTopic);
//                    if (partitionOffset == null) {
//                        offsetTable.putIfAbsent(groupAtTopic, new ConcurrentHashMap<>());
//                    }
//                    offsetTable.get(groupAtTopic).putIfAbsent(partitionId,
//                            MessageIdUtils
//                                    .getOffset(readPosition.getLedgerId(), readPosition.getEntryId(), partitionId));
//                } catch (Exception e) {
//                    log.warn("restore subscriptions[groupAtTopic={}] error.", groupAtTopic, e);
//                }
//
//            }
//        });
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
        Iterator<Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>>> it = groupMetaManager.getOffsetTable()
                .entrySet()
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
        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> next : groupMetaManager.getOffsetTable()
                .entrySet()) {
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
        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> next : groupMetaManager.getOffsetTable()
                .entrySet()) {
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
        groupMetaManager.commitOffset(group, topic, queueId, offset);
    }

    public long queryOffset(final String group, final String topic, final int queueId) {
        return groupMetaManager.queryOffset(group, topic, queueId);
    }

    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {
        Map<Integer, Long> queueMinOffset = new HashMap<>();
        Set<ClientGroupAndTopicName> topicGroups = groupMetaManager.getOffsetTable().keySet();
        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                topicGroups.removeIf(clientGroupAndTopicName -> group
                        .equals(clientGroupAndTopicName.getClientGroupName().getRmqGroupName()));
            }
        }

        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> offSetEntry : groupMetaManager
                .getOffsetTable().entrySet()) {
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
        return groupMetaManager.getOffsetTable().get(new ClientGroupAndTopicName(group, topic));
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = groupMetaManager.getOffsetTable()
                .get(new ClientGroupAndTopicName(srcGroup, topic));
        if (offsets != null) {
            groupMetaManager.getOffsetTable()
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

    public boolean isSystemGroup(String groupName) {
        return groupName.startsWith(RocketMQTopic.getMetaTenant() + SLASH_CHAR + RocketMQTopic.getMetaNamespace())
                || groupName
                .startsWith(RocketMQTopic.getDefaultTenant() + SLASH_CHAR + RocketMQTopic.getDefaultNamespace());
    }

    private boolean isPulsarTopicCached(ClientTopicName topicName, int partitionId) {
        if (topicName == null) {
            return false;
        }
        return pulsarTopicCache.containsKey(topicName) && pulsarTopicCache.get(topicName)
                .containsKey(partitionId);
    }

}
