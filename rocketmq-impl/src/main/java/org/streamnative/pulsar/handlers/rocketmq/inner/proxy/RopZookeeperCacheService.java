package org.streamnative.pulsar.handlers.rocketmq.inner.proxy;

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
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopTopicContent;

@Slf4j
@Data
public class RopZookeeperCacheService implements AutoCloseable {

    private final ZooKeeperCache cache;
    private ZooKeeperDataCache<RopTopicContent> ropTopicDataCache;

    public RopZookeeperCacheService(ZooKeeperCache cache) throws RopServerException {
        this.cache = cache;
        initZK();
        this.ropTopicDataCache = new ZooKeeperDataCache<RopTopicContent>(cache) {
            @Override
            public RopTopicContent deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, RopTopicContent.class);
            }
        };
    }


    private void initZK() throws RopServerException {
        String[] paths = new String[]{TOPIC_BASE_PATH, GROUP_BASE_PATH};
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
