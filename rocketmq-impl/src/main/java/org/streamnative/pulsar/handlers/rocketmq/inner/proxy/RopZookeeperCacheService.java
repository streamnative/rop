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

import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath.BROKER_CLUSTER_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath.BROKER_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath.GROUP_BASE_PATH;
import static org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath.TOPIC_BASE_PATH;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
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

/**
 * Rop Zookeeper Cache Service.
 */
@Slf4j
@Data
public class RopZookeeperCacheService implements AutoCloseable {

    private final ZooKeeperCache cache;
    private ZooKeeperDataCache<RopTopicContent> ropTopicDataCache;
    private ZooKeeperDataCache<RopClusterContent> ropClusterDataCache;

    public RopZookeeperCacheService(ZooKeeperCache cache) throws RopServerException {
        this.cache = cache;
        initZK();
        this.ropTopicDataCache = new ZooKeeperDataCache<RopTopicContent>(cache) {
            @Override
            public RopTopicContent deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, RopTopicContent.class);
            }
        };
        this.ropClusterDataCache = new ZooKeeperDataCache<RopClusterContent>(cache) {
            @Override
            public RopClusterContent deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, RopClusterContent.class);
            }
        };
    }


    private void initZK() throws RopServerException {
        String[] paths = new String[]{BROKER_PATH, TOPIC_BASE_PATH, GROUP_BASE_PATH, BROKER_CLUSTER_PATH};
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
        this.ropTopicDataCache.close();
    }
}
