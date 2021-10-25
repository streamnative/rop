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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopGroupContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath;
import org.streamnative.pulsar.handlers.rocketmq.utils.ZookeeperUtils;

/**
 * Subscription group manager.
 */
@Slf4j
public class SubscriptionGroupManager {

    private final int maxCacheSize = 1000 * 1000;
    private final int maxCacheTimeInSec = 60;

    private final RocketMQBrokerController brokerController;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final DataVersion dataVersion = new DataVersion();
    private final ScheduledExecutorService cleanUpExecutor;
    private ZooKeeper zkClient;

    protected final Cache<ClientGroupName, SubscriptionGroupConfig> subscriptionGroupTableCache = CacheBuilder
            .newBuilder()
            .initialCapacity(maxCacheSize)
            .expireAfterAccess(maxCacheTimeInSec, TimeUnit.SECONDS)
            .removalListener((RemovalNotification<ClientGroupName, SubscriptionGroupConfig> notification) ->
                    log.info("Remove key [{}] from subscriptionGroupTableCache", notification.getKey().toString()))
            .build();

    public SubscriptionGroupManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();

        cleanUpExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("Rop-subscriptionGroupTableCache-cleanUp");
            return t;
        });
        cleanUpExecutor.scheduleWithFixedDelay(subscriptionGroupTableCache::cleanUp, 1, 1, TimeUnit.MINUTES);
    }

    private void init() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("TOOLS_CONSUMER");
        subscriptionGroupTableCache.put(new ClientGroupName("TOOLS_CONSUMER"), subscriptionGroupConfig);

        subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName("SELF_TEST_C_GROUP");
        subscriptionGroupTableCache.put(new ClientGroupName("SELF_TEST_C_GROUP"), subscriptionGroupConfig);
    }

    public void start() {
        log.info("starting SubscriptionGroupManager service ...");
        this.zkClient = brokerController.getBrokerService().pulsar().getZkClient();
    }

    public void shutdown() {
        cleanUpExecutor.shutdownNow();
    }

    public void updateSubscriptionGroupConfig(SubscriptionGroupConfig config) {
        ClientGroupName clientGroupName = new ClientGroupName(config.getGroupName());

        TopicName topicName = TopicName.get(clientGroupName.getPulsarGroupName());
        String groupNodePath = String.format(RopZkPath.GROUP_BASE_PATH_MATCH, clientGroupName.getPulsarGroupName());

        try {
            RopGroupContent ropGroupContent = new RopGroupContent(config);
            byte[] content = jsonMapper.writeValueAsBytes(ropGroupContent);
            zkClient.setData(groupNodePath, content, -1);
            subscriptionGroupTableCache.put(clientGroupName, ropGroupContent.getConfig());
        } catch (KeeperException.NoNodeException e) {
            try {
                String tenantNodePath = String.format(RopZkPath.GROUP_BASE_PATH_MATCH, topicName.getTenant());
                ZookeeperUtils.createPersistentNodeIfNotExist(zkClient, tenantNodePath);

                String nsNodePath = String.format(RopZkPath.GROUP_BASE_PATH_MATCH, topicName.getNamespace());
                ZookeeperUtils.createPersistentNodeIfNotExist(zkClient, nsNodePath);

                RopGroupContent ropGroupContent = new RopGroupContent(config);
                byte[] content = jsonMapper.writeValueAsBytes(ropGroupContent);
                ZookeeperUtils.createPersistentNodeIfNotExist(zkClient, groupNodePath, content);
                subscriptionGroupTableCache.put(clientGroupName, ropGroupContent.getConfig());
            } catch (Exception ee) {
                log.error("Update subscription group [{}] config error.", config.getGroupName(), ee);
                throw new RuntimeException("Update subscription group config failed.");
            }

        } catch (Exception e) {
            log.error("Update subscription group [{}] config error.", config.getGroupName(), e);
            throw new RuntimeException("Update subscription group config failed.");
        }
    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(String group) {
        ClientGroupName clientGroupName = new ClientGroupName(group);

        SubscriptionGroupConfig subscriptionGroupConfig = subscriptionGroupTableCache.getIfPresent(clientGroupName);
        if (subscriptionGroupConfig != null) {
            return subscriptionGroupConfig;
        }

        try {
            String groupNodePath = String.format(RopZkPath.GROUP_BASE_PATH_MATCH, clientGroupName.getPulsarGroupName());
            byte[] content = zkClient.getData(groupNodePath, null, null);
            RopGroupContent ropGroupContent = jsonMapper.readValue(content, RopGroupContent.class);
            subscriptionGroupTableCache.put(clientGroupName, ropGroupContent.getConfig());
            return ropGroupContent.getConfig();
        } catch (Exception e) {
            if (brokerController.getServerConfig().isAutoCreateSubscriptionGroup()
                    || MixAll.isSysConsumerGroup(group)) {
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                updateSubscriptionGroupConfig(subscriptionGroupConfig);
                return subscriptionGroupConfig;
            }
            log.error("Find subscription group [{}] config error.", group, e);
            throw new RuntimeException("Find subscription group config failed.");
        }
    }

    public ConcurrentMap<ClientGroupName, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTableCache.asMap();
    }

    public void deleteSubscriptionGroupConfig(String group) {
        ClientGroupName clientGroupName = new ClientGroupName(group);

        String groupNodePath = String.format(RopZkPath.GROUP_BASE_PATH_MATCH, clientGroupName.getPulsarGroupName());
        ZookeeperUtils.deleteData(zkClient, groupNodePath);
        subscriptionGroupTableCache.invalidate(clientGroupName);
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }
}

