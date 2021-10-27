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

package org.streamnative.pulsar.handlers.rocketmq.inner.proxy;

import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.BROKERS_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.BROKER_CLUSTER_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.GROUP_BASE_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.TOPIC_BASE_PATH;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopServerException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopClusterContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopGroupContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopTopicContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.PulsarUtil;

/**
 * Rop Zookeeper Cache Service.
 */
@Slf4j
@Data
public class RopZookeeperCacheService implements AutoCloseable {

    private final RopZookeeperCache cache;
    private final ZooKeeperDataCache<RopTopicContent> topicDataCache;
    private final ZooKeeperDataCache<RopClusterContent> clusterDataCache;
    private final ZooKeeperDataCache<String> brokerCache;
    private final ZooKeeperDataCache<RopGroupContent> subscribeGroupConfigCache;

    public RopZookeeperCacheService(RopZookeeperCache cache) throws RopServerException {
        this.cache = cache;
        this.topicDataCache = new ZooKeeperDataCache<RopTopicContent>(cache) {
            @Override
            public RopTopicContent deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, RopTopicContent.class);
            }
        };
        this.clusterDataCache = new ZooKeeperDataCache<RopClusterContent>(cache) {
            @Override
            public RopClusterContent deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, RopClusterContent.class);
            }
        };
        this.brokerCache = new ZooKeeperDataCache<String>(cache) {
            @Override
            public String deserialize(String key, byte[] content) {
                return new String(content, StandardCharsets.UTF_8);
            }
        };
        this.subscribeGroupConfigCache = new ZooKeeperDataCache<RopGroupContent>(cache) {
            @Override
            public RopGroupContent deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, RopGroupContent.class);
            }
        };
    }


    private void initZK() throws RopServerException {
        String[] paths = new String[]{BROKERS_PATH, TOPIC_BASE_PATH, GROUP_BASE_PATH, BROKER_CLUSTER_PATH};
        try {
            ZooKeeper zk = cache.getZooKeeper();
            for (String path : paths) {
                if (cache.exists(path)) {
                    continue;
                }

                try {
                    ZkUtils.createFullPathOptimistic(zk, path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {
                    // Ok
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RopServerException(e);
        }

    }

    public void start() throws RopServerException {
        cache.start();
        initZK();
    }

    @Override
    public void close() throws Exception {
        this.cache.stop();
        this.topicDataCache.close();
    }

    public RopTopicContent getTopicContent(TopicName topicName) throws Exception {
        Preconditions.checkNotNull(topicName);
        String topicZNodePath = String.format(RopZkUtils.TOPIC_BASE_PATH_MATCH,
                PulsarUtil.getNoDomainTopic(topicName));
        RopTopicContent topicContent = topicDataCache.getDataIfPresent(topicZNodePath);
        try {
            topicContent = Objects.isNull(topicContent) ? topicDataCache.get(topicZNodePath).get() : topicContent;
        } catch (Exception e) {
            log.warn("RopTopicContent[topicPath:{}] isn't exists in metadata.", topicZNodePath, e);
        }
        return topicContent;
    }

    public void setTopicContent(TopicName topicName, Object jsonObj) throws Exception {
        Preconditions.checkNotNull(topicName);
        String topicZNodePath = String.format(RopZkUtils.TOPIC_BASE_PATH_MATCH,
                PulsarUtil.getNoDomainTopic(topicName));
        setJsonObjectForPath(topicZNodePath, jsonObj);
        topicDataCache.reloadCache(topicZNodePath);
    }

    public void createTopicContent(TopicName topicName, Object jsonObj) throws Exception {
        Preconditions.checkNotNull(topicName);
        String topicZNodePath = String.format(RopZkUtils.TOPIC_BASE_PATH_MATCH,
                PulsarUtil.getNoDomainTopic(topicName));
        createFullPathWithJsonObject(topicZNodePath, jsonObj);
        topicDataCache.reloadCache(topicZNodePath);
    }

    public void setJsonObjectForPath(String zNodePath, Object jsonObj)
            throws Exception {
        Preconditions.checkNotNull(jsonObj, "json object can't be null.");
        String strContent = ObjectMapperFactory.getThreadLocal().writeValueAsString(jsonObj);
        cache.getZooKeeper().setData(zNodePath, strContent.getBytes(StandardCharsets.UTF_8), -1);
    }

    public void createFullPathWithJsonObject(String zNodePath, Object jsonObj)
            throws Exception {
        Preconditions.checkNotNull(jsonObj, "json object can't be null.");
        String strContent = ObjectMapperFactory.getThreadLocal().writeValueAsString(jsonObj);
        ZkUtils.createFullPathOptimistic(cache.getZooKeeper(), zNodePath, strContent.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void deleteFullPath(String zNodePath) throws Exception {
        if (!Strings.isNullOrEmpty(zNodePath)) {
            ZkUtils.deleteFullPathOptimistic(cache.getZooKeeper(), zNodePath, -1);
        }
    }

    public RopClusterContent getClusterContent() {
        try {
            return clusterDataCache.get(BROKER_CLUSTER_PATH).get();
        } catch (Exception e) {
            log.info("RoP cluster configuration is missing, please reset it after RoP cluster initialized.");
            return null;
        }
    }

    public RopGroupContent getGroupConfig(String group) {
        ClientGroupName clientGroupName = new ClientGroupName(group);
        String groupNodePath = String.format(RopZkUtils.GROUP_BASE_PATH_MATCH, clientGroupName.getPulsarGroupName());
        RopGroupContent groupContent = subscribeGroupConfigCache.getDataIfPresent(groupNodePath);
        try {
            groupContent = (groupContent == null) ? subscribeGroupConfigCache.get(groupNodePath).get() : groupContent;
        } catch (Exception e) {
            log.warn("GroupConfig[{}] and zkPath[{}] isn't exists in metadata.", group, groupNodePath);
        }
        return groupContent;
    }

    public RopGroupContent getGroupConfig(SubscriptionGroupConfig groupConfig) {
        return getGroupConfig(groupConfig.getGroupName());
    }

    public RopGroupContent updateOrCreateGroupConfig(SubscriptionGroupConfig groupConfig) throws Exception {
        ClientGroupName clientGroupName = new ClientGroupName(groupConfig.getGroupName());
        String groupNodePath = String.format(RopZkUtils.GROUP_BASE_PATH_MATCH, clientGroupName.getPulsarGroupName());
        RopGroupContent tmpGroupContent = getGroupConfig(groupConfig);
        if (tmpGroupContent == null) {
            //create
            tmpGroupContent = new RopGroupContent();
            tmpGroupContent.setConfig(groupConfig);
            createFullPathWithJsonObject(groupNodePath, tmpGroupContent);
        } else {
            //update
            tmpGroupContent.setConfig(groupConfig);
            setJsonObjectForPath(groupNodePath, tmpGroupContent);
        }
        return tmpGroupContent;
    }

    public void deleteGroupConfig(String group) {
        String groupNodePath = String.format(RopZkUtils.GROUP_BASE_PATH_MATCH, group);
        try {
            deleteFullPath(groupNodePath);
        } catch (Exception e) {
            log.warn("RopZookeeperCacheService deleteGroupConfig for group[{}] error.", group, e);
        }
    }
}
