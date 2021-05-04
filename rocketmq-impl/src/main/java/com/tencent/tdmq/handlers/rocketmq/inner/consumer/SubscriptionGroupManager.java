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

import com.google.common.base.Preconditions;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.inner.producer.ClientGroupName;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;

/**
 * Subscription group manager.
 */
@Slf4j
public class SubscriptionGroupManager {

    private final ConcurrentMap<ClientGroupName, SubscriptionGroupConfig> subscriptionGroupTable =
            new ConcurrentHashMap(512);
    private final DataVersion dataVersion = new DataVersion();
    private final RocketMQBrokerController brokerController;

    public SubscriptionGroupManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    private void init() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("TOOLS_CONSUMER");
        this.subscriptionGroupTable.put(new ClientGroupName("TOOLS_CONSUMER"), subscriptionGroupConfig);

        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("SELF_TEST_C_GROUP");
        this.subscriptionGroupTable.put(new ClientGroupName("SELF_TEST_C_GROUP"), subscriptionGroupConfig);
    }

    public void start() {
        log.info("starting SubscriptionGroupManager service...");
        Preconditions.checkNotNull(brokerController);
        ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();
        consumerOffsetManager.getPulsarTopicCache().forEach(((clientTopicName, persistentTopicMap) -> {
            persistentTopicMap.values().forEach((topic) -> {
                topic.getSubscriptions().forEach((grp, subscription) -> {
                    SubscriptionGroupConfig config = new SubscriptionGroupConfig();
                    ClientGroupName clientGroupName = new ClientGroupName(TopicName.get(grp));
                    config.setGroupName(clientGroupName.getRmqGroupName());
                    subscriptionGroupTable.put(clientGroupName, config);
                });
            });
        }));
    }

    public void updateSubscriptionGroupConfig(SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable
                .put(new ClientGroupName(config.getGroupName()), config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }
        this.dataVersion.nextVersion();
    }

    public void disableConsume(String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.get(new ClientGroupName(groupName));
        if (old != null) {
            old.setConsumeEnable(false);
            this.dataVersion.nextVersion();
        }

    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(String group) {
        ClientGroupName groupName = new ClientGroupName(group);
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(groupName);
        if (null == subscriptionGroupConfig && (this.brokerController.getServerConfig().isAutoCreateSubscriptionGroup()
                || MixAll.isSysConsumerGroup(groupName.getRmqGroupName()))) {
            subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(groupName.getRmqGroupName());
            SubscriptionGroupConfig preConfig = this.subscriptionGroupTable
                    .putIfAbsent(groupName, subscriptionGroupConfig);
            if (null == preConfig) {
                log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
            }
            this.dataVersion.nextVersion();
        }
        return subscriptionGroupConfig;
    }


    public ConcurrentMap<ClientGroupName, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return this.subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return this.dataVersion;
    }

    public void deleteSubscriptionGroupConfig(String groupName) {
        ClientGroupName clientGroupName = new ClientGroupName(groupName);
        subscriptionGroupTable.remove(clientGroupName);
    }
}

