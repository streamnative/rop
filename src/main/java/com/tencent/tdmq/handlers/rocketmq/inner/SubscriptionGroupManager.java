package com.tencent.tdmq.handlers.rocketmq.inner;

import com.tencent.tdmq.handlers.rocketmq.RocketMQBrokerController;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;

@Slf4j
public class SubscriptionGroupManager implements RocketMQLoader{

    private final ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap(1024);
    private final DataVersion dataVersion = new DataVersion();
    private transient RocketMQBrokerController brokerController;

    public SubscriptionGroupManager() {
        this.init();
    }

    public SubscriptionGroupManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    private void init() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("TOOLS_CONSUMER");
        this.subscriptionGroupTable.put("TOOLS_CONSUMER", subscriptionGroupConfig);
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("FILTERSRV_CONSUMER");
        this.subscriptionGroupTable.put("FILTERSRV_CONSUMER", subscriptionGroupConfig);
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("SELF_TEST_C_GROUP");
        this.subscriptionGroupTable.put("SELF_TEST_C_GROUP", subscriptionGroupConfig);
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("CID_ONS-HTTP-PROXY");
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        this.subscriptionGroupTable.put("CID_ONS-HTTP-PROXY", subscriptionGroupConfig);
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("CID_ONSAPI_PULL");
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        this.subscriptionGroupTable.put("CID_ONSAPI_PULL", subscriptionGroupConfig);
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("CID_ONSAPI_PERMISSION");
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        this.subscriptionGroupTable.put("CID_ONSAPI_PERMISSION", subscriptionGroupConfig);
        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("CID_ONSAPI_OWNER");
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        this.subscriptionGroupTable.put("CID_ONSAPI_OWNER", subscriptionGroupConfig);
    }

    public void updateSubscriptionGroupConfig(SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = (SubscriptionGroupConfig) this.subscriptionGroupTable
                .put(config.getGroupName(), config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }

        this.dataVersion.nextVersion();
        //this.persist();
    }

    public void disableConsume(String groupName) {
        SubscriptionGroupConfig old = (SubscriptionGroupConfig) this.subscriptionGroupTable.get(groupName);
        if (old != null) {
            old.setConsumeEnable(false);
            this.dataVersion.nextVersion();
        }

    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(String group) {
        SubscriptionGroupConfig subscriptionGroupConfig = (SubscriptionGroupConfig) this.subscriptionGroupTable
                .get(group);
        if (null == subscriptionGroupConfig && (this.brokerController.getServerConfig().isAutoCreateSubscriptionGroup()
                || MixAll.isSysConsumerGroup(group))) {
            subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(group);
            SubscriptionGroupConfig preConfig = (SubscriptionGroupConfig) this.subscriptionGroupTable
                    .putIfAbsent(group, subscriptionGroupConfig);
            if (null == preConfig) {
                log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
            }

            this.dataVersion.nextVersion();
            //this.persist();
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

    }

    @Override
    public boolean load() {
        return false;
    }

    @Override
    public boolean unLoad() {
        return false;
    }
}

