package com.tencent.tdmq.handlers.rocketmq.inner;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;

@Slf4j
public class TopicConfigManager implements RocketMQLoader {

    private static final long LOCK_TIMEOUT_MILLIS = 3000L;
    private final transient Lock lockTopicConfigTable = new ReentrantLock();
    private final ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap(1024);
    private final DataVersion dataVersion = new DataVersion();
    private final Set<String> systemTopicList = new HashSet();
    private transient RocketMQBrokerController brokerController;

    public TopicConfigManager() {
    }

    public TopicConfigManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        String topic = "SELF_TEST_TOPIC";
        TopicConfig topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (this.brokerController.getServerConfig().isAutoCreateTopicEnable()) {
            topic = "TBW102";
            topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(this.brokerController.getServerConfig().getDefaultTopicQueueNums());
            topicConfig.setWriteQueueNums(this.brokerController.getServerConfig().getDefaultTopicQueueNums());
            int perm = 7;
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }

        topic = "BenchmarkTest";
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        topicConfig.setReadQueueNums(1024);
        topicConfig.setWriteQueueNums(1024);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        topic = this.brokerController.getServerConfig().getClusterName();
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        int perm = 1;
        if (this.brokerController.getServerConfig().isClusterTopicEnable()) {
            perm |= 6;
        }

        topicConfig.setPerm(perm);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        topic = this.brokerController.getServerConfig().getBrokerName();
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        perm = 1;
        if (this.brokerController.getServerConfig().isBrokerTopicEnable()) {
            perm |= 6;
        }

        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        topicConfig.setPerm(perm);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        topic = "OFFSET_MOVED_EVENT";
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (this.brokerController.getServerConfig().isTraceTopicEnable()) {
            topic = this.brokerController.getServerConfig().getMsgTraceTopicName();
            topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }

        topic = this.brokerController.getServerConfig().getClusterName() + "_" + "REPLY_TOPIC";
        topicConfig = new TopicConfig(topic);
        this.systemTopicList.add(topic);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
    }

    public boolean isSystemTopic(String topic) {
        return this.systemTopicList.contains(topic);
    }

    public Set<String> getSystemTopic() {
        return this.systemTopicList;
    }

    public TopicConfig selectTopicConfig(String topic) {
        return (TopicConfig) this.topicConfigTable.get(topic);
    }

    public TopicConfig createTopicInSendMessageMethod(String topic, String defaultTopic, String remoteAddress,
            int clientDefaultTopicQueueNums, int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(3000L, TimeUnit.MILLISECONDS)) {
                label131:
                {
                    TopicConfig defaultTopicConfig;
                    try {
                        topicConfig = (TopicConfig) this.topicConfigTable.get(topic);
                        if (topicConfig == null) {
                            defaultTopicConfig = (TopicConfig) this.topicConfigTable.get(defaultTopic);
                            if (defaultTopicConfig != null) {
                                if (defaultTopic.equals("TBW102") && !this.brokerController.getServerConfig()
                                        .isAutoCreateTopicEnable()) {
                                    defaultTopicConfig.setPerm(6);
                                }

                                if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                                    topicConfig = new TopicConfig(topic);
                                    int queueNums = clientDefaultTopicQueueNums > defaultTopicConfig.getWriteQueueNums()
                                            ? defaultTopicConfig.getWriteQueueNums() : clientDefaultTopicQueueNums;
                                    if (queueNums < 0) {
                                        queueNums = 0;
                                    }

                                    topicConfig.setReadQueueNums(queueNums);
                                    topicConfig.setWriteQueueNums(queueNums);
                                    int perm = defaultTopicConfig.getPerm();
                                    perm &= -2;
                                    topicConfig.setPerm(perm);
                                    topicConfig.setTopicSysFlag(topicSysFlag);
                                    topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                                } else {
                                    log.warn(
                                            "Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                                            new Object[]{defaultTopic, defaultTopicConfig.getPerm(), remoteAddress});
                                }
                            } else {
                                log.warn(
                                        "Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                                        defaultTopic, remoteAddress);
                            }

                            if (topicConfig != null) {
                                log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]",
                                        new Object[]{defaultTopic, topicConfig, remoteAddress});
                                this.topicConfigTable.put(topic, topicConfig);
                                this.dataVersion.nextVersion();
                                createNew = true;
                                //this.persist();
                            }
                            break label131;
                        }

                        defaultTopicConfig = topicConfig;
                    } finally {
                        this.lockTopicConfigTable.unlock();
                    }

                    return defaultTopicConfig;
                }
            }
        } catch (InterruptedException var15) {
            log.error("createTopicInSendMessageMethod exception", var15);
        }

        if (createNew) {
            //this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicInSendMessageBackMethod(String topic, int clientDefaultTopicQueueNums, int perm,
            int topicSysFlag) {
        TopicConfig topicConfig = (TopicConfig) this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            return topicConfig;
        } else {
            boolean createNew = false;

            try {
                if (this.lockTopicConfigTable.tryLock(3000L, TimeUnit.MILLISECONDS)) {
                    label85:
                    {
                        TopicConfig var7;
                        try {
                            topicConfig = (TopicConfig) this.topicConfigTable.get(topic);
                            if (topicConfig == null) {
                                topicConfig = new TopicConfig(topic);
                                topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                                topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                                topicConfig.setPerm(perm);
                                topicConfig.setTopicSysFlag(topicSysFlag);
                                log.info("create new topic {}", topicConfig);
                                this.topicConfigTable.put(topic, topicConfig);
                                createNew = true;
                                this.dataVersion.nextVersion();
                                //this.persist();
                                break label85;
                            }

                            var7 = topicConfig;
                        } finally {
                            this.lockTopicConfigTable.unlock();
                        }

                        return var7;
                    }
                }
            } catch (InterruptedException var12) {
                log.error("createTopicInSendMessageBackMethod exception", var12);
            }

            if (createNew) {
                //this.brokerController.registerBrokerAll(false, true, true);
            }

            return topicConfig;
        }
    }

    public TopicConfig createTopicOfTranCheckMaxTime(int clientDefaultTopicQueueNums, int perm) {
        TopicConfig topicConfig = (TopicConfig) this.topicConfigTable.get("TRANS_CHECK_MAX_TIME_TOPIC");
        if (topicConfig != null) {
            return topicConfig;
        } else {
            boolean createNew = false;

            try {
                if (this.lockTopicConfigTable.tryLock(3000L, TimeUnit.MILLISECONDS)) {
                    label85:
                    {
                        TopicConfig var5;
                        try {
                            topicConfig = (TopicConfig) this.topicConfigTable.get("TRANS_CHECK_MAX_TIME_TOPIC");
                            if (topicConfig == null) {
                                topicConfig = new TopicConfig("TRANS_CHECK_MAX_TIME_TOPIC");
                                topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                                topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                                topicConfig.setPerm(perm);
                                topicConfig.setTopicSysFlag(0);
                                log.info("create new topic {}", topicConfig);
                                this.topicConfigTable.put("TRANS_CHECK_MAX_TIME_TOPIC", topicConfig);
                                createNew = true;
                                this.dataVersion.nextVersion();
                                //this.persist();
                                break label85;
                            }

                            var5 = topicConfig;
                        } finally {
                            this.lockTopicConfigTable.unlock();
                        }

                        return var5;
                    }
                }
            } catch (InterruptedException var10) {
                log.error("create TRANS_CHECK_MAX_TIME_TOPIC exception", var10);
            }

            if (createNew) {
                //this.brokerController.registerBrokerAll(false, true, true);
            }

            return topicConfig;
        }
    }

    public void updateTopicUnitFlag(String topic, boolean unit) {
        TopicConfig topicConfig = (TopicConfig) this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                    topicConfig.getTopicSysFlag());
            this.topicConfigTable.put(topic, topicConfig);
            this.dataVersion.nextVersion();
            //this.persist();
            //this.brokerController.registerBrokerAll(false, true, true);
        }

    }

    public void updateTopicUnitSubFlag(String topic, boolean hasUnitSub) {
        TopicConfig topicConfig = (TopicConfig) this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                    topicConfig.getTopicSysFlag());
            this.topicConfigTable.put(topic, topicConfig);
            this.dataVersion.nextVersion();
            //this.persist();
            //this.brokerController.registerBrokerAll(false, true, true);
        }

    }

    public void updateTopicConfig(TopicConfig topicConfig) {
        TopicConfig old = (TopicConfig) this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        this.dataVersion.nextVersion();
        //this.persist();
    }

    public void updateOrderTopicConfig(KVTable orderKVTableFromNs) {
        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            Iterator var4 = orderTopics.iterator();

            while (var4.hasNext()) {
                String topic = (String) var4.next();
                TopicConfig topicConfig = (TopicConfig) this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            var4 = this.topicConfigTable.entrySet().iterator();

            while (var4.hasNext()) {
                Entry<String, TopicConfig> entry = (Entry) var4.next();
                String topic = (String) entry.getKey();
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = (TopicConfig) entry.getValue();
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }

            if (isChange) {
                this.dataVersion.nextVersion();
                //this.persist();
            }
        }

    }

    public boolean isOrderTopic(String topic) {
        TopicConfig topicConfig = (TopicConfig) this.topicConfigTable.get(topic);
        return topicConfig == null ? false : topicConfig.isOrder();
    }

    public void deleteTopicConfig(String topic) {
        TopicConfig old = (TopicConfig) this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            this.dataVersion.nextVersion();
            //this.persist();
        } else {
            log.warn("delete topic config failed, topic: {} not exists", topic);
        }

    }

    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return this.topicConfigTable;
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
