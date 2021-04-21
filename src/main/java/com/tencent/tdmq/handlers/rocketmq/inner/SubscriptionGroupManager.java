package com.tencent.tdmq.handlers.rocketmq.inner;

import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;

@Slf4j
public class SubscriptionGroupManager {

    private final ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap(512);
    private final DataVersion dataVersion = new DataVersion();
    private transient RocketMQBrokerController brokerController;

    public SubscriptionGroupManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    private void init() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("TOOLS_CONSUMER");
        this.subscriptionGroupTable.put("TOOLS_CONSUMER", subscriptionGroupConfig);

        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("SELF_TEST_C_GROUP");
        this.subscriptionGroupTable.put("SELF_TEST_C_GROUP", subscriptionGroupConfig);
    }

    public void updateSubscriptionGroupConfig(SubscriptionGroupConfig config) {
        String tdmqGroupName = CommonUtils.tdmqGroupName(config.getGroupName());
        config.setGroupName(tdmqGroupName);
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(tdmqGroupName, config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }
        this.dataVersion.nextVersion();
    }

    public void disableConsume(String groupName) {
        String tdmqGroupName = CommonUtils.tdmqGroupName(groupName);
        SubscriptionGroupConfig old = this.subscriptionGroupTable.get(groupName);
        if (old != null) {
            old.setConsumeEnable(false);
            this.dataVersion.nextVersion();
        }

    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(String group) {
        RocketMQTopic rmqGroup = new RocketMQTopic(group);
        String tdmqGroupName = rmqGroup.getOrigNoDomainTopicName();
        String noNamespaceGroupName = rmqGroup.getPulsarTopicName().getLocalName();
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(tdmqGroupName);
        if (null == subscriptionGroupConfig && (this.brokerController.getServerConfig().isAutoCreateSubscriptionGroup()
                || MixAll.isSysConsumerGroup(noNamespaceGroupName))) {
            subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(tdmqGroupName);
            SubscriptionGroupConfig preConfig = this.subscriptionGroupTable
                    .putIfAbsent(tdmqGroupName, subscriptionGroupConfig);
            if (null == preConfig) {
                log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
            }
            this.dataVersion.nextVersion();
        }
        return subscriptionGroupConfig;
    }


    public ConcurrentMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return this.subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return this.dataVersion;
    }

    public void deleteSubscriptionGroupConfig(String groupName) {
//TODO: need to implement it
    }
}

