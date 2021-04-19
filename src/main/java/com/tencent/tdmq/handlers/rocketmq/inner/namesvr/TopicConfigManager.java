package com.tencent.tdmq.handlers.rocketmq.inner.namesvr;

import com.google.common.base.Splitter;
import com.tencent.tdmq.handlers.rocketmq.RocketMQServiceConfiguration;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;

@Slf4j
public abstract class TopicConfigManager {

    protected static final long LOCK_TIMEOUT_MILLIS = 3000;
    protected transient final Lock lockTopicConfigTable = new ReentrantLock();

    //key = {tenant}/{ns}/{topic}
    protected final ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>(
            1024);
    protected final DataVersion dataVersion = new DataVersion();
    protected final Set<String> systemTopicList = new HashSet<String>();
    protected final RocketMQServiceConfiguration config;
    protected transient RocketMQBrokerController brokerController;

    public TopicConfigManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.config = this.brokerController.getServerConfig();
        int defaultPartitionNum = config.getDefaultNumPartitions();
        {
            // MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC
            if (this.brokerController.getServerConfig().isAutoCreateTopicEnable()) {
                String topic = RocketMQTopic.getPulsarMetaNoDomainTopic(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC);
                TopicConfig topicConfig = new TopicConfig(topic);
                this.systemTopicList.add(topic);
                topicConfig.setReadQueueNums(this.brokerController.getServerConfig()
                        .getDefaultTopicQueueNums());
                topicConfig.setWriteQueueNums(this.brokerController.getServerConfig()
                        .getDefaultTopicQueueNums());
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // MixAll.BENCHMARK_TOPIC
            String topic = RocketMQTopic.getPulsarMetaNoDomainTopic(MixAll.BENCHMARK_TOPIC);
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(defaultPartitionNum);
            topicConfig.setWriteQueueNums(defaultPartitionNum);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            String topic = RocketMQTopic
                    .getPulsarMetaNoDomainTopic(this.brokerController.getServerConfig().getClusterName());
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            int perm = PermName.PERM_INHERIT;
            if (this.brokerController.getServerConfig().isClusterTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // MixAll.OFFSET_MOVED_EVENT
            String topic = RocketMQTopic.getPulsarMetaNoDomainTopic(MixAll.OFFSET_MOVED_EVENT);
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
 /*       {
            if (this.brokerController.getServerConfig().isTraceTopicEnable()) {
                String topic = RocketMQTopic
                        .getPulsarMetaNoDomainTopic(this.brokerController.getServerConfig().getMsgTraceTopicName());
                TopicConfig topicConfig = new TopicConfig(topic);
                this.systemTopicList.add(topic);
                topicConfig.setReadQueueNums(1);
                topicConfig.setWriteQueueNums(1);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            String topic =
                    new RocketMQTopic(
                            this.brokerController.getServerConfig().getClusterName() + "_" + MixAll.REPLY_TOPIC_POSTFIX)
                            .getNoDomainTopicName();
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }*/
        {
            String delayedLevelStr = config.getMessageDelayLevel();
            Splitter.on(" ").omitEmptyStrings().split(delayedLevelStr).forEach(lvl -> {
                        String topic = RocketMQTopic.getPulsarMetaNoDomainTopic(config.getRmqScheduleTopic() + "_" + lvl);
                        TopicConfig topicConfig = new TopicConfig(topic);
                        this.systemTopicList.add(topic);
                        topicConfig.setReadQueueNums(config.getRmqScheduleTopicPartitionNum());
                        topicConfig.setWriteQueueNums(config.getRmqScheduleTopicPartitionNum());
                        this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
                    }
            );
        }

        {
            String topic = RocketMQTopic.getPulsarMetaNoDomainTopic(config.getRmqSysTransHalfTopic());
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(defaultPartitionNum);
            topicConfig.setWriteQueueNums(defaultPartitionNum);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }

        {
            String topic = RocketMQTopic.getPulsarMetaNoDomainTopic(config.getRmqSysTransOpHalfTopic());
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(defaultPartitionNum);
            topicConfig.setWriteQueueNums(defaultPartitionNum);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }

        {
            String topic = RocketMQTopic.getPulsarMetaNoDomainTopic(config.getRmqTransCheckMaxTimeTopic());
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }

    }

    public boolean isSystemTopic(final String topic) {
        return this.systemTopicList.contains(RocketMQTopic.getPulsarMetaNoDomainTopic(topic));
    }

    public Set<String> getSystemTopic() {
        return this.systemTopicList;
    }

    public TopicConfig selectTopicConfig(final String topic) {
        if (log.isDebugEnabled()) {
            log.debug("[TopicConfigManager] The topic{} is in {}.", RocketMQTopic.getPulsarOrigNoDomainTopic(topic),
                    this.topicConfigTable);
        }
        return this.topicConfigTable.get(RocketMQTopic.getPulsarOrigNoDomainTopic(topic));
    }

    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic,
            final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        String pulsarTopicName = RocketMQTopic.getPulsarOrigNoDomainTopic(topic);
        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(pulsarTopicName);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    String pulsarDefaultTopic = RocketMQTopic.getPulsarMetaNoDomainTopic(defaultTopic);
                    TopicConfig defaultTopicConfig = this.topicConfigTable.get(pulsarDefaultTopic);
                    if (defaultTopicConfig != null) {
                        if (pulsarDefaultTopic
                                .equals(RocketMQTopic.getPulsarMetaNoDomainTopic(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC))) {
                            if (!this.brokerController.getServerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }

                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            topicConfig = new TopicConfig(pulsarTopicName);

                            int queueNums =
                                    clientDefaultTopicQueueNums > defaultTopicConfig.getWriteQueueNums()
                                            ? defaultTopicConfig
                                            .getWriteQueueNums() : clientDefaultTopicQueueNums;

                            if (queueNums < 0) {
                                queueNums = 0;
                            }

                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            log.warn(
                                    "Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                                    defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                                defaultTopic, remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]",
                                defaultTopic, topicConfig, remoteAddress);

                        this.topicConfigTable.put(pulsarTopicName, topicConfig);

                        this.dataVersion.nextVersion();

                        createNew = true;

                        this.createPulsarPartitionedTopic(topicConfig);
                    }
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        return topicConfig;
    }

    protected abstract void createPulsarPartitionedTopic(TopicConfig topicConfig);

    public TopicConfig createTopicInSendMessageBackMethod(
            final String topic,
            final int clientDefaultTopicQueueNums,
            final int perm,
            final int topicSysFlag) {
        String pulsarTopicName = RocketMQTopic.getPulsarOrigNoDomainTopic(topic);
        TopicConfig topicConfig = this.topicConfigTable.get(pulsarTopicName);
        if (topicConfig != null) {
            return topicConfig;
        }

        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(pulsarTopicName);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    topicConfig = new TopicConfig(pulsarTopicName);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(pulsarTopicName, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    //TODO: this.persist();
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        return topicConfig;
    }

    public TopicConfig createTopicOfTranCheckMaxTime(final int clientDefaultTopicQueueNums, final int perm) {
        TopicConfig topicConfig = this.topicConfigTable.get(MixAll.TRANS_CHECK_MAX_TIME_TOPIC);
        if (topicConfig != null) {
            return topicConfig;
        }

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(MixAll.TRANS_CHECK_MAX_TIME_TOPIC);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    topicConfig = new TopicConfig(MixAll.TRANS_CHECK_MAX_TIME_TOPIC);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(0);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(MixAll.TRANS_CHECK_MAX_TIME_TOPIC, topicConfig);
                    this.dataVersion.nextVersion();
                    //TODO: this.persist();
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("create TRANS_CHECK_MAX_TIME_TOPIC exception", e);
        }

        return topicConfig;
    }

    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        this.dataVersion.nextVersion();

        //TODO this.persist();
    }

    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            for (Map.Entry<String, TopicConfig> entry : this.topicConfigTable.entrySet()) {
                String topic = entry.getKey();
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = entry.getValue();
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }

            if (isChange) {
                this.dataVersion.nextVersion();
                //TODO this.persist();
            }
        }
    }

    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig == null) {
            return false;
        } else {
            return topicConfig.isOrder();
        }
    }

    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            this.dataVersion.nextVersion();
            //TODO this.persist();
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

    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper.toJson(prettyFormat);
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }
}
