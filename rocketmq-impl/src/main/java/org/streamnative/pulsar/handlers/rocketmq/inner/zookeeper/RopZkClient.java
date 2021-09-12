package org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;

@Slf4j
public class RopZkClient implements Watcher {

    private final RocketMQBrokerController brokerController;

    private ZooKeeper zooKeeper;

    public RopZkClient(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        this.zooKeeper = brokerController.getBrokerService().pulsar().getZkClient();

        /*
         * init rop zk node
         */
        try {
            Stat stat = zooKeeper.exists(RopZkPath.ropPath, false);
            if (stat == null) {
                zooKeeper.create(RopZkPath.ropPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.ropPath);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.ropPath, e);
            throw new RuntimeException(e);
        }

        try {
            Stat stat = zooKeeper.exists(RopZkPath.brokerPath, false);
            if (stat == null) {
                zooKeeper.create(RopZkPath.brokerPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.brokerPath);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.brokerPath, e);
            throw new RuntimeException(e);
        }

        try {
            Stat stat = zooKeeper.exists(RopZkPath.topicBasePath, false);
            if (stat == null) {
                zooKeeper.create(RopZkPath.topicBasePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.topicBasePath);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.topicBasePath, e);
            throw new RuntimeException(e);
        }

        try {
            Stat stat = zooKeeper.exists(RopZkPath.groupBasePath, false);
            if (stat == null) {
                zooKeeper.create(RopZkPath.groupBasePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.groupBasePath);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.groupBasePath, e);
            throw new RuntimeException(e);
        }
    }

    public void create(String path, byte[] content) {
        try {
            zooKeeper.create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.groupBasePath);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.groupBasePath, e);
            throw new RuntimeException(e);
        }
    }

    public List<String> getChildren(String path) {
        try {
            return zooKeeper.getChildren(path, null);
        } catch (KeeperException | InterruptedException e) {
            log.error("Failed to get children from {}: {}", path, e);
            throw new RuntimeException(e);
        }
    }


    public byte[] getData(String path) {
        try {
            return zooKeeper.getData(path, null, null);
        } catch (KeeperException | InterruptedException e) {
            log.error("Failed to get data from {}: {}", path, e);
            throw new RuntimeException(e);
        }
    }

    public void delete(String path) {
        try {
            zooKeeper.delete(path, -1);
        } catch (KeeperException | InterruptedException e) {
            log.error("Failed to get data from {}: {}", path, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(WatchedEvent event) {

    }
}
