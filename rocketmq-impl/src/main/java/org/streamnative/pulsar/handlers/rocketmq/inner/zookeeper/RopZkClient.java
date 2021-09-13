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

package org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper;

import java.nio.charset.StandardCharsets;
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

/**
 * Rop Zk client.
 */
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
            Stat stat = zooKeeper.exists(RopZkPath.ROP_PATH, false);
            if (stat == null) {
                zooKeeper.create(RopZkPath.ROP_PATH,
                        "".getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.ROP_PATH);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.ROP_PATH, e);
            throw new RuntimeException(e);
        }

        try {
            Stat stat = zooKeeper.exists(RopZkPath.BROKER_PATH, false);
            if (stat == null) {
                zooKeeper.create(RopZkPath.BROKER_PATH,
                        "".getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.BROKER_PATH);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.BROKER_PATH, e);
            throw new RuntimeException(e);
        }

        try {
            Stat stat = zooKeeper.exists(RopZkPath.TOPIC_BASE_PATH, false);
            if (stat == null) {
                zooKeeper.create(RopZkPath.TOPIC_BASE_PATH,
                        "".getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.TOPIC_BASE_PATH);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.TOPIC_BASE_PATH, e);
            throw new RuntimeException(e);
        }

        try {
            Stat stat = zooKeeper.exists(RopZkPath.GROUP_BASE_PATH, false);
            if (stat == null) {
                zooKeeper.create(RopZkPath.GROUP_BASE_PATH,
                        "".getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.GROUP_BASE_PATH);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.GROUP_BASE_PATH, e);
            throw new RuntimeException(e);
        }
    }

    public void create(String path, byte[] content) {
        try {
            zooKeeper.create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            log.info("Zk node [{}] has exist.", RopZkPath.GROUP_BASE_PATH);
        } catch (Exception e) {
            log.error("Failed to create zk node {}", RopZkPath.GROUP_BASE_PATH, e);
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
