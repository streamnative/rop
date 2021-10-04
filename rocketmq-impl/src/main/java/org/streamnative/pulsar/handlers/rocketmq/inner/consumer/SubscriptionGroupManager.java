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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.inner.proxy.RopZookeeperCacheService;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopGroupContent;

/**
 * Subscription group manager.
 */
@Slf4j
public class SubscriptionGroupManager implements Closeable {

    private final int cacheInitCapacity = 1024;
    private final int maxCacheTimeInSec = 60;
    private final RocketMQBrokerController brokerController;
    private final DataVersion dataVersion = new DataVersion();

    private final AtomicReference<RopZookeeperCacheService> zkServiceRef = new AtomicReference<>();
    private volatile boolean isRunning = false;


    private final Cache<ClientGroupName, SubscriptionGroupConfig> subscriptionGroupTableCache = CacheBuilder
            .newBuilder()
            .initialCapacity(cacheInitCapacity)
            .expireAfterAccess(maxCacheTimeInSec, TimeUnit.SECONDS)
            .removalListener((RemovalNotification<ClientGroupName, SubscriptionGroupConfig> notification) ->
                    log.info("Remove key [{}] from subscriptionGroupTableCache", notification.getKey().toString()))
            .build();

    public SubscriptionGroupManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
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
        if (!isRunning) {
            this.zkServiceRef.set(brokerController.getRopBrokerProxy().getZkService());
            isRunning = true;
            log.info("SubscriptionGroupManager has been started.");
        }
    }

    @Override
    public void close() {
        subscriptionGroupTableCache.cleanUp();
        zkServiceRef.set(null);
        isRunning = false;
        log.info("SubscriptionGroupManager have been closed.");
    }

    public void updateSubscriptionGroupConfig(SubscriptionGroupConfig config) {
        Preconditions.checkArgument(isRunning, "SubscriptionGroupManager hasn't been initialized.");
        Preconditions.checkArgument(config != null && Strings.isNotBlank(config.getGroupName()),
                "GroupName in SubscriptionGroupConfig can't be empty.");
        try {
            SubscriptionGroupConfig oldGroupConfig = zkServiceRef.get().getGroupConfig(config).getConfig();
            if (!config.equals(oldGroupConfig)) {
                zkServiceRef.get().updateOrCreateGroupConfig(config);
                subscriptionGroupTableCache.put(new ClientGroupName(config.getGroupName()), config);
                dataVersion.nextVersion();
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
            RopGroupContent groupConfigContent = zkServiceRef.get().getGroupConfig(group);
            if (Objects.nonNull(groupConfigContent)) {
                subscriptionGroupTableCache.put(clientGroupName, groupConfigContent.getConfig());
            } else if (brokerController.getServerConfig().isAutoCreateSubscriptionGroup()
                    || MixAll.isSysConsumerGroup(group)) {
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                updateSubscriptionGroupConfig(subscriptionGroupConfig);
                return subscriptionGroupConfig;
            }
            return groupConfigContent.getConfig();
        } catch (Exception e) {
            log.error("Find subscription group [{}] config error.", group, e);
            throw new RuntimeException("Find subscription group config failed.");
        }
    }

    public ConcurrentMap<ClientGroupName, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        //return activity subscription group config
        return subscriptionGroupTableCache.asMap();
    }

    public void deleteSubscriptionGroupConfig(String group) {
        ClientGroupName clientGroupName = new ClientGroupName(group);
        zkServiceRef.get().deleteGroupConfig(clientGroupName.getPulsarGroupName());
        subscriptionGroupTableCache.invalidate(clientGroupName);
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }
}

