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

import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.BROKER_CLUSTER_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.BROKERS_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.GROUP_BASE_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils.TOPIC_BASE_PATH;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopServerException;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopClusterContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopTopicContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.PulsarUtil;

/**
 * Rop Zookeeper Cache Service.
 */
@Slf4j
@Data
public class RopZookeeperCacheService implements AutoCloseable {

    private final ZooKeeperCache cache;
    private ZooKeeperDataCache<RopTopicContent> topicDataCache;
    private ZooKeeperDataCache<RopClusterContent> clusterDataCache;
    private ZooKeeperDataCache<String> brokerCache;

    public RopZookeeperCacheService(ZooKeeperCache cache) throws RopServerException {
        this.cache = cache;
        initZK();
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
            public String deserialize(String key, byte[] content) throws Exception {
                return new String(content, StandardCharsets.UTF_8);
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

    @Override
    public void close() throws Exception {
        this.cache.stop();
        this.topicDataCache.close();
    }

    public RopTopicContent getTopicContent(TopicName topicName) throws Exception {
        Preconditions.checkNotNull(topicName);
        String topicZNodePath = String.format(RopZkUtils.TOPIC_BASE_PATH_MATCH,
                PulsarUtil.getNoDomainTopic(topicName));
        return topicDataCache.get(topicZNodePath).get();
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
            throws JsonProcessingException, KeeperException, InterruptedException {
        Preconditions.checkNotNull(jsonObj, "json object can't be null.");
        String strContent = ObjectMapperFactory.getThreadLocal().writeValueAsString(jsonObj);
        cache.getZooKeeper().setData(zNodePath, strContent.getBytes(StandardCharsets.UTF_8), -1);
    }

    public void createFullPathWithJsonObject(String zNodePath, Object jsonObj)
            throws JsonProcessingException, KeeperException, InterruptedException {
        Preconditions.checkNotNull(jsonObj, "json object can't be null.");
        String strContent = ObjectMapperFactory.getThreadLocal().writeValueAsString(jsonObj);
        ZkUtils.createFullPathOptimistic(cache.getZooKeeper(), zNodePath, strContent.getBytes(StandardCharsets.UTF_8),
                null, CreateMode.PERSISTENT);
    }

    public RopClusterContent getClusterContent() {
        try {
            return clusterDataCache.get(BROKER_CLUSTER_PATH).get();
        } catch (Exception e) {
            log.warn("RoP cluster configuration is missing, please reset it when RoP cluster initialized.");
            RopClusterContent defaultClusterContent = new RopClusterContent();
            defaultClusterContent.setClusterName("DefaultCluster");
            Set<String> allBrokers = getAllBrokers();
            log.info(allBrokers.toString());
            return null;
        }
    }

    public Set<String> getAllBrokers() {
        try {
            return cache.getChildren(BROKERS_PATH);
        } catch (Exception e) {
            log.warn("RopZookeeperCacheService getAllBrokers error, caused by:", e);
        }
        return Collections.emptySet();
    }
}
