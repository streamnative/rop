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

import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupMetaManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;

/**
 * Subscription group manager.
 */
@Slf4j
public class SubscriptionGroupManager {

    private final DataVersion dataVersion = new DataVersion();
    private final GroupMetaManager groupMetaManager;

    public SubscriptionGroupManager(RocketMQBrokerController brokerController, GroupMetaManager groupMetaManager) {
        this.groupMetaManager = groupMetaManager;
        this.init();
    }

    private void init() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("TOOLS_CONSUMER");
        groupMetaManager.getSubscriptionGroupTable()
                .put(new ClientGroupName("TOOLS_CONSUMER"), subscriptionGroupConfig);

        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("SELF_TEST_C_GROUP");
        groupMetaManager.getSubscriptionGroupTable()
                .put(new ClientGroupName("SELF_TEST_C_GROUP"), subscriptionGroupConfig);
    }

    public void start() {
        log.info("starting SubscriptionGroupManager service...");
//        Preconditions.checkNotNull(brokerController);
//        groupMetaManager.getPulsarTopicCache().forEach(((clientTopicName, persistentTopicMap) -> {
//            persistentTopicMap.values().forEach((topic) -> {
//                topic.getSubscriptions().forEach((grp, subscription) -> {
//                    SubscriptionGroupConfig config = new SubscriptionGroupConfig();
//                    ClientGroupName clientGroupName = new ClientGroupName(TopicName.get(grp));
//                    config.setGroupName(clientGroupName.getRmqGroupName());
//                    groupMetaManager.getSubscriptionGroupTable().put(clientGroupName, config);
//                });
//            });
//        }));
    }

    public void updateSubscriptionGroupConfig(SubscriptionGroupConfig config) {
        groupMetaManager.updateGroup(config);
    }

    public void disableConsume(String groupName) {
        SubscriptionGroupConfig old = groupMetaManager.getSubscriptionGroupTable().get(new ClientGroupName(groupName));
        if (old != null) {
            old.setConsumeEnable(false);
            this.dataVersion.nextVersion();
        }

    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(String group) {
        return groupMetaManager.queryGroup(group);
    }

    public ConcurrentMap<ClientGroupName, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return groupMetaManager.getSubscriptionGroupTable();
    }

    public DataVersion getDataVersion() {
        return groupMetaManager.getDataVersion();
    }

    public void deleteSubscriptionGroupConfig(String groupName) {
        groupMetaManager.deleteGroup(groupName);
    }
}

