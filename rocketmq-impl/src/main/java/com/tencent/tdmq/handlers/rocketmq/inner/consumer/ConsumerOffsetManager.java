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

package com.tencent.tdmq.handlers.rocketmq.inner.consumer;

import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import com.tencent.tdmq.handlers.rocketmq.inner.producer.ClientGroupName;
import com.tencent.tdmq.handlers.rocketmq.inner.producer.ClientTopicName;
import com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.UtilAll;

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
    private ConcurrentMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<>(512);
    @Getter
    private ConcurrentHashMap<ClientTopicName, ConcurrentMap<Integer, PersistentTopic>> pulsarTopicCache =
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
        pulsarTopicCache.get(clientTopicName).putIfAbsent(partitionId, pulsarTopic);
        pulsarTopic.getSubscriptions().forEach((grp, grpInfo) -> {
            if (!isSystemGroup(grp)) {
                ManagedCursor cursor = grpInfo.getCursor();
                PositionImpl readPosition = (PositionImpl) cursor.getReadPosition();
                ClientGroupName clientGroupName = new ClientGroupName(TopicName.get(grp));
                ClientGroupAndTopicName groupAtTopic = new ClientGroupAndTopicName(clientGroupName, clientTopicName);
                ConcurrentMap<Integer, Long> partitionOffset = offsetTable.get(groupAtTopic);
                if (partitionOffset == null) {
                    offsetTable.putIfAbsent(groupAtTopic, new ConcurrentHashMap<>());
                }
                offsetTable.get(groupAtTopic).putIfAbsent(partitionId,
                        MessageIdUtils.getOffset(readPosition.getLedgerId(), readPosition.getEntryId(), partitionId));
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

    public void removePulsarTopic(ClientTopicName clientTopicName) {
        pulsarTopicCache.remove(clientTopicName);
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
            long minOffsetInStore = getMinOffsetInQueue(topicAtGroup, next.getKey());
            long offsetInPersist = next.getValue();
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }

    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<>();
        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> next : this.offsetTable.entrySet()) {
            String topicAtGroup = next.getKey().getClientTopicName().getRmqTopicName();
            topics.add(topicAtGroup);
        }
        return topics;
    }

    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<>();
        for (Entry<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> next : this.offsetTable.entrySet()) {
            ClientGroupName clientGroupName = next.getKey().getClientGroupName();
            groups.add(clientGroupName.getRmqGroupName());
        }
        return groups;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
            final long offset) {
        ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(group, topic);
        long fixedOffset = offset;
        MessageIdImpl messageId = MessageIdUtils.getMessageId(offset);
        // rocketmq client will add 1 to commitOffset sometimes, it break down pulsar offset management,so fix it
        if (messageId.getPartitionIndex() != queueId) {
            fixedOffset = MessageIdUtils.getOffset(messageId.getLedgerId(), messageId.getEntryId(), queueId);
        }
        this.commitOffset(clientHost, clientGroupAndTopicName, queueId, fixedOffset);
    }

    private void commitOffset(final String clientHost, final ClientGroupAndTopicName clientGroupAndTopicName,
            final int queueId, final long offset) {
        log.debug("commitOffset =======> ClientGroupAndTopicName=[{}], queueId=[{}], offset=[{}].",
                new Object[]{clientGroupAndTopicName,
                        queueId, offset});
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(clientGroupAndTopicName);
        if (null == map) {
            map = new ConcurrentHashMap<>(32);
            map.put(queueId, offset);
            this.offsetTable.put(clientGroupAndTopicName, map);
        } else {
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                log.warn(
                        "[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, "
                                + "requestOffset={}, storeOffset={}",
                        clientHost, clientGroupAndTopicName, queueId, offset, storeOffset);
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
        }
        return -1L;
    }

    public ConcurrentMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<ClientGroupAndTopicName, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
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
                    long minOffset = getMinOffsetInQueue(topicGroup, entry.getKey());
                    if (entry.getValue() >= minOffset) {
                        Long offset = queueMinOffset.get(entry.getKey());
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), entry.getValue());
                        } else {
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
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

    public long getMinOffsetInQueue(ClientGroupAndTopicName groupAndTopic, int partitionId) {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(groupAndTopic, partitionId);
        if (persistentTopic != null) {
            try {
                PositionImpl firstPosition = persistentTopic.getFirstPosition();
                return MessageIdUtils.getOffset(firstPosition.getLedgerId(), firstPosition.getEntryId(), partitionId);
            } catch (ManagedLedgerException e) {
                log.warn("getMinOffsetInQueue error, ClientGroupAndTopicName=[{}], partitionId=[{}].", groupAndTopic,
                        partitionId);
            }
        }
        return 0L;
    }

    public long getMaxOffsetInQueue(ClientGroupAndTopicName groupAndTopic, int partitionId) {
        PersistentTopic persistentTopic = getPulsarPersistentTopic(groupAndTopic, partitionId);
        if (persistentTopic != null) {
            PositionImpl lastPosition = (PositionImpl) persistentTopic.getLastPosition();
            return MessageIdUtils.getOffset(lastPosition.getLedgerId(), lastPosition.getEntryId(), partitionId);
        }
        return 0L;
    }

    public void getPulsarPersitentTopicAsync(ClientGroupAndTopicName groupAndTopic, int partitionId,
            CompletableFuture<PersistentTopic> topicCompletableFuture) {
        // setup ownership of service unit to this broker
        LookupOptions lookupOptions = LookupOptions.builder().authoritative(true).build();
        TopicName pulsarTopicName = TopicName.get(groupAndTopic.getClientTopicName().getPulsarTopicName());
        TopicName partitionTopicName = pulsarTopicName.getPartition(partitionId);
        String topicName = partitionTopicName.toString();
        PulsarService pulsarService = this.brokerController.getBrokerService().pulsar();
        pulsarService.getNamespaceService()
                .getBrokerServiceUrlAsync(partitionTopicName, lookupOptions).
                whenComplete((addr, th) -> {
                    log.info("Find getBrokerServiceUrl {}, return Topic: {}", addr, topicName);
                    if (th != null || addr == null || addr.get() == null) {
                        log.warn("Failed getBrokerServiceUrl {}, return null Topic. throwable: ", topicName, th);
                        topicCompletableFuture.complete(null);
                        return;
                    }
                    pulsarService.getBrokerService().getTopic(topicName, false)
                            .whenComplete((topicOptional, throwable) -> {
                                if (throwable != null) {
                                    log.error("Failed to getTopic {}. exception: {}", topicName, throwable);
                                    topicCompletableFuture.complete(null);
                                    return;
                                }
                                try {
                                    if (topicOptional.isPresent()) {
                                        Topic topic = topicOptional.get();
                                        PersistentTopic persistentTopic = (PersistentTopic) topic;
                                        persistentTopic.setDeleteWhileInactive(false);
                                        topicCompletableFuture.complete(persistentTopic);
                                    } else {
                                        log.error("Get empty topic for name {}", topicName);
                                        topicCompletableFuture.complete(null);
                                    }
                                } catch (Exception e) {
                                    log.error("Failed to get client in registerInPersistentTopic {}. "
                                            + "exception:", topicName, e);
                                    topicCompletableFuture.complete(null);
                                }
                            });
                });
    }

    public PersistentTopic getPulsarPersistentTopic(ClientGroupAndTopicName groupAndTopic, int partitionId) {
        ClientTopicName clientTopicName = groupAndTopic.getClientTopicName();
        if (isPulsarTopicCached(groupAndTopic, partitionId)) {
            return this.pulsarTopicCache.get(clientTopicName).get(partitionId);
        }
        CompletableFuture<PersistentTopic> feature = new CompletableFuture<>();
        getPulsarPersitentTopicAsync(groupAndTopic, partitionId, feature);
        try {
            PersistentTopic persistentTopic = feature.get();
            if (persistentTopic != null) {
                this.pulsarTopicCache.putIfAbsent(clientTopicName, new ConcurrentHashMap<>());
                this.pulsarTopicCache.get(clientTopicName).putIfAbsent(partitionId, persistentTopic);
            }
        } catch (Exception e) {
            log.warn("getPulsarPersistentTopic topic=[{}] and partition=[{}] timeout.", partitionId);
        }
        return this.pulsarTopicCache.get(groupAndTopic.getClientTopicName()).get(partitionId);
    }

    private boolean isSystemGroup(String groupName) {
        return groupName.startsWith(RocketMQTopic.metaTenant + "/" + RocketMQTopic.metaNamespace)
                || groupName.startsWith(RocketMQTopic.defaultTenant + "/" + RocketMQTopic.defaultNamespace);
    }

    public synchronized void persist() {
        offsetTable.forEach((groupAndTopic, offsetMap) -> {
            String pulsarGroup = groupAndTopic.getClientGroupName().getPulsarGroupName();
            if (!isSystemGroup(pulsarGroup)) {
                offsetMap.forEach((partitionId, offset) -> {
                    try {
                        PersistentTopic persistentTopic = getPulsarPersistentTopic(groupAndTopic, partitionId);
                        if (persistentTopic != null) {
                            PersistentSubscription subscription = persistentTopic.getSubscription(pulsarGroup);
                            if (subscription == null) {
                                subscription = (PersistentSubscription) persistentTopic
                                        .createSubscription(pulsarGroup, InitialPosition.Latest, false).get();
                            }
                            ManagedCursor cursor = subscription.getCursor();
                            cursor.markDelete(MessageIdUtils.getPosition(offset));
                            log.debug("markDelete =======> groupAndTopic=[{}], partitionId=[{}],  position=[{}].",
                                    new Object[]{groupAndTopic, partitionId,
                                            MessageIdUtils.getPosition(offset)});
                        }
                    } catch (Exception e) {
                        log.warn("persist topic[{}] offset[{}] error.", groupAndTopic, offset);
                    }
                });
            }
        });
    }

    private boolean isPulsarTopicCached(ClientGroupAndTopicName groupAndTopicName, int partitionId) {
        if (groupAndTopicName == null) {
            return false;
        }
        ClientTopicName clientTopicName = groupAndTopicName.getClientTopicName();
        return pulsarTopicCache.containsKey(clientTopicName) && pulsarTopicCache.get(clientTopicName)
                .containsKey(partitionId);
    }

    public void checkExistsAndAddCache(String rmqGroup, String rmqTopic, int partitionId) {
        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(rmqGroup, rmqTopic);
        ClientTopicName clientTopicName = groupAndTopicName.getClientTopicName();
        if (!pulsarTopicCache.containsKey(clientTopicName) || !pulsarTopicCache.get(clientTopicName)
                .containsKey(partitionId)) {
            this.getPulsarPersistentTopic(groupAndTopicName, partitionId);
        }
        if (!offsetTable.containsKey(groupAndTopicName) || !offsetTable.get(groupAndTopicName)
                .containsKey(partitionId)) {
            PersistentTopic persistentTopic = this.getPulsarPersistentTopic(groupAndTopicName, partitionId);
            if (persistentTopic != null) {
                String pulsarGroup = groupAndTopicName.getClientGroupName().getPulsarGroupName();
                ManagedLedger managedLedger = persistentTopic.getManagedLedger();

                PositionImpl position = (PositionImpl) persistentTopic.getSubscription(pulsarGroup).getCursor()
                        .getMarkDeletedPosition();
                long offset = MessageIdUtils.getOffset(position.getLedgerId(), position.getEntryId(), partitionId);
                offsetTable.putIfAbsent(groupAndTopicName, new ConcurrentHashMap<>());
                offsetTable.get(groupAndTopicName).putIfAbsent(partitionId, offset);
            }

        }
    }

}
