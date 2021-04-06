package com.tencent.tdmq.handlers.rocketmq.inner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

@Slf4j
public class ConsumerOffsetManager implements RocketMQLoader {

    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap(512);
    private transient RocketMQBrokerController brokerController;

    public ConsumerOffsetManager() {
    }

    public ConsumerOffsetManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void scanUnsubscribedTopic() {
        Iterator it = this.offsetTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = (Entry) it.next();
            String topicAtGroup = (String) next.getKey();
            String[] arrays = topicAtGroup.split("@");
            if (arrays.length == 2) {
                String topic = arrays[0];
                String group = arrays[1];
                if (null == this.brokerController.getConsumerManager().findSubscriptionData(group, topic) && this
                        .offsetBehindMuchThanData(topic, (ConcurrentMap) next.getValue())) {
                    it.remove();
                    log.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }

    }

    private boolean offsetBehindMuchThanData(String topic, ConcurrentMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();

        boolean result;
        long minOffsetInStore;
        long offsetInPersist;
        for (result = !table.isEmpty(); it.hasNext() && result; result = offsetInPersist <= minOffsetInStore) {
            Entry<Integer, Long> next = (Entry) it.next();
            minOffsetInStore = this.brokerController.getMessageStore()
                    .getMinOffsetInQueue(topic, (Integer) next.getKey());
            offsetInPersist = (Long) next.getValue();
        }

        return result;
    }

    public Set<String> whichTopicByConsumer(String group) {
        Set<String> topics = new HashSet();
        Iterator it = this.offsetTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = (Entry) it.next();
            String topicAtGroup = (String) next.getKey();
            String[] arrays = topicAtGroup.split("@");
            if (arrays.length == 2 && group.equals(arrays[1])) {
                topics.add(arrays[0]);
            }
        }

        return topics;
    }

    public Set<String> whichGroupByTopic(String topic) {
        Set<String> groups = new HashSet();
        Iterator it = this.offsetTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = (Entry) it.next();
            String topicAtGroup = (String) next.getKey();
            String[] arrays = topicAtGroup.split("@");
            if (arrays.length == 2 && topic.equals(arrays[0])) {
                groups.add(arrays[1]);
            }
        }

        return groups;
    }

    public void commitOffset(String clientHost, String group, String topic, int queueId, long offset) {
        String key = topic + "@" + group;
        this.commitOffset(clientHost, key, queueId, offset);
    }

    private void commitOffset(String clientHost, String key, int queueId, long offset) {
        ConcurrentMap<Integer, Long> map = (ConcurrentMap) this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            Long storeOffset = (Long) map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                log.warn(
                        "[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}",
                        new Object[]{clientHost, key, queueId, offset, storeOffset});
            }
        }

    }

    public long queryOffset(String group, String topic, int queueId) {
        String key = topic + "@" + group;
        ConcurrentMap<Integer, Long> map = (ConcurrentMap) this.offsetTable.get(key);
        if (null != map) {
            Long offset = (Long) map.get(queueId);
            if (offset != null) {
                return offset;
            }
        }

        return -1L;
    }

    public String encode() {
        return this.encode(false);
    }

    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOffsetManager obj = (ConsumerOffsetManager) RemotingSerializable
                    .fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }

    }

    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return this.offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public Map<Integer, Long> queryMinOffsetInAllGroup(String topic, String filterGroups) {
        Map<Integer, Long> queueMinOffset = new HashMap();
        Set<String> topicGroups = this.offsetTable.keySet();
        Iterator it;
        if (!UtilAll.isBlank(filterGroups)) {
            String[] var5 = filterGroups.split(",");
            int var6 = var5.length;

            for (int var7 = 0; var7 < var6; ++var7) {
                String group = var5[var7];
                it = topicGroups.iterator();

                while (it.hasNext()) {
                    if (group.equals(((String) it.next()).split("@")[1])) {
                        it.remove();
                    }
                }
            }
        }

        Iterator var14 = this.offsetTable.entrySet().iterator();

        while (true) {
            Entry offSetEntry;
            String[] topicGroupArr;
            do {
                if (!var14.hasNext()) {
                    return queueMinOffset;
                }

                offSetEntry = (Entry) var14.next();
                String topicGroup = (String) offSetEntry.getKey();
                topicGroupArr = topicGroup.split("@");
            } while (!topic.equals(topicGroupArr[0]));

            it = ((ConcurrentMap) offSetEntry.getValue()).entrySet().iterator();

            while (it.hasNext()) {
                Entry<Integer, Long> entry = (Entry) it.next();
                long minOffset = this.brokerController.getMessageStore()
                        .getMinOffsetInQueue(topic, (Integer) entry.getKey());
                if ((Long) entry.getValue() >= minOffset) {
                    Long offset = (Long) queueMinOffset.get(entry.getKey());
                    if (offset == null) {
                        queueMinOffset.put(entry.getKey(), Math.min(9223372036854775807L, (Long) entry.getValue()));
                    } else {
                        queueMinOffset.put(entry.getKey(), Math.min((Long) entry.getValue(), offset));
                    }
                }
            }
        }
    }

    public Map<Integer, Long> queryOffset(String group, String topic) {
        String key = topic + "@" + group;
        return (Map) this.offsetTable.get(key);
    }

    public void cloneOffset(String srcGroup, String destGroup, String topic) {
        ConcurrentMap<Integer, Long> offsets = (ConcurrentMap) this.offsetTable.get(topic + "@" + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + "@" + destGroup, new ConcurrentHashMap(offsets));
        }

    }

    @Override
    public boolean load() {
        return true;
    }

    @Override
    public boolean unLoad() {
        return false;
    }
}
