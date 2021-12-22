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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.inner.proxy.RopZookeeperCacheService;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopGroupContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils;

/**
 * Subscription group manager.
 */
@Slf4j
public class SubscriptionGroupManager implements Closeable {

    private final RocketMQBrokerController brokerController;
    private final DataVersion dataVersion = new DataVersion();

    private final AtomicReference<RopZookeeperCacheService> zkServiceRef = new AtomicReference<>();
    private volatile boolean isRunning = false;


    public SubscriptionGroupManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        log.info("starting SubscriptionGroupManager service ...");
        if (!isRunning) {
            this.zkServiceRef.set(brokerController.getRopBrokerProxy().getZkService());
            isRunning = true;
            log.info("SubscriptionGroupManager has been started.");
        }

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("TOOLS_CONSUMER");
        updateSubscriptionGroupConfig(subscriptionGroupConfig);

        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("SELF_TEST_C_GROUP");
        updateSubscriptionGroupConfig(subscriptionGroupConfig);
    }

    @Override
    public void close() {
        zkServiceRef.set(null);
        isRunning = false;
        log.info("SubscriptionGroupManager have been closed.");
    }

    public void updateSubscriptionGroupConfig(SubscriptionGroupConfig config) {
        log.info("RoP update subscription group config: [{}]", config);
        Preconditions.checkArgument(isRunning, "SubscriptionGroupManager hasn't been initialized.");
        Preconditions.checkArgument(config != null && Strings.isNotBlank(config.getGroupName()),
                "GroupName in SubscriptionGroupConfig can't be empty.");
        try {
            RopGroupContent groupConfigContent = zkServiceRef.get().getGroupConfig(config);
            SubscriptionGroupConfig oldGroupConfig = groupConfigContent != null ? groupConfigContent.getConfig() : null;
            if (!config.equals(oldGroupConfig)) {
                zkServiceRef.get().updateOrCreateGroupConfig(config);
                dataVersion.nextVersion();
            }
        } catch (Exception e) {
            log.error("Update subscription group [{}] config error.", config.getGroupName(), e);
            throw new RuntimeException("Update subscription group config failed.");
        }
    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(String ropGroup) {
        try {
            //ropGroup: tenant|ns%groupName
            RopGroupContent groupConfigContent = zkServiceRef.get().getGroupConfig(ropGroup);
            if (groupConfigContent != null) {
                return groupConfigContent.getConfig();
            }

            if (brokerController.getServerConfig().isAutoCreateSubscriptionGroup()
                    || MixAll.isSysConsumerGroup(ropGroup)) {
                log.info("RoP auto create group: [{}]", ropGroup);
                SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(ropGroup);
                updateSubscriptionGroupConfig(subscriptionGroupConfig);
                return subscriptionGroupConfig;
            }
        } catch (Exception e) {
            log.error("Find subscription group [{}] config error.", ropGroup, e);
            throw new RuntimeException("Find subscription group config failed.");
        }
        return null;
    }

    public ConcurrentMap<ClientGroupName, SubscriptionGroupConfig> getSubscriptionGroupTable()
            throws RemotingCommandException {
        ConcurrentMap<ClientGroupName, SubscriptionGroupConfig> result = new ConcurrentHashMap<>();
        try {
            Set<String> tenants = zkServiceRef.get().getZookeeperCache().getChildren(RopZkUtils.GROUP_BASE_PATH);
            for (String tenant : tenants) {
                if (tenant.startsWith("rocketmq-")) {
                    String tenantNodePath = String.format(RopZkUtils.GROUP_BASE_PATH_MATCH, tenant);
                    Set<String> namespaces = zkServiceRef.get().getZookeeperCache().getChildren(tenantNodePath);
                    for (String namespace : namespaces) {
                        String namespaceNodePath = String
                                .format(RopZkUtils.GROUP_BASE_PATH_MATCH, tenant + "/" + namespace);
                        Set<String> groups = zkServiceRef.get().getZookeeperCache().getChildren(namespaceNodePath);
                        for (String group : groups) {
                            String fullGroupName = tenant + "|" + namespace + "%" + group;
                            ClientGroupName clientGroupName = new ClientGroupName(fullGroupName);
                            try {
                                result.put(clientGroupName, findSubscriptionGroupConfig(fullGroupName));
                            } catch (Exception e) {
                                log.info("RoP getSubscriptionGroupTable failed for group [{}]", fullGroupName);
                            }
                        }
                    }
                }
            }
            return result;
        } catch (Exception e) {
            log.warn("RoP getSubscriptionGroupTable failed.", e);
            throw new RemotingCommandException(e.getMessage(), e);
        }
    }

    public void deleteSubscriptionGroupConfig(String rmqGroup) {
        log.info("RoP delete group: [{}]", rmqGroup);
        ClientGroupName clientGroupName = new ClientGroupName(rmqGroup);
        zkServiceRef.get().deleteGroupConfig(clientGroupName.getPulsarGroupName());

        String retryTopic = MixAll.getRetryTopic(rmqGroup);
        log.info("RoP delete group retry topic: [{}]", retryTopic);
        this.brokerController.getTopicConfigManager().deleteTopic(retryTopic);

        String dlqTopic = MixAll.getDLQTopic(rmqGroup);
        log.info("RoP delete group dlq topic: [{}]", dlqTopic);
        this.brokerController.getTopicConfigManager().deleteTopic(dlqTopic);
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }
}

