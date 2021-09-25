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

package org.streamnative.pulsar.handlers.rocketmq.inner.coordinator;

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.proxy.RopZookeeperCacheService;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopCoordinatorContent;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.ZookeeperUtils;

/**
 * Rop coordinator.
 */
@Slf4j
public class RopCoordinator implements AutoCloseable {

    private final RocketMQBrokerController brokerController;
    private final PulsarService pulsar;
    private final ExecutorService executor;
    private final ObjectMapper jsonMapper;

    private final AtomicReference<RopCoordinatorContent> currentCoordinator = new AtomicReference<>();
    private final AtomicBoolean isCoordinator = new AtomicBoolean(false);
    private volatile boolean elected = false;
    private volatile boolean stopped = true;

    public RopCoordinator(RocketMQBrokerController brokerController,
            RopZookeeperCacheService ropZookeeperCacheService) {
        this.brokerController = brokerController;
        this.pulsar = brokerController.getBrokerService().pulsar();
        this.executor = brokerController.getScheduledExecutorService();
        this.jsonMapper = new ObjectMapper();
    }

    public void start() {
        checkState(stopped);
        stopped = false;
        log.info("RopCoordinator started");
        elect();
    }

    private void elect() {
        try {
            byte[] data = pulsar.getLocalZkCache().getZooKeeper().getData(RopZkUtils.COORDINATOR_PATH, event -> {
                log.warn("Type of the event is [{}] and path is [{}]", event.getType(), event.getPath());
                if (event.getType() == EventType.NodeDeleted) {
                    log.warn("Election node {} is deleted, attempting re-election...", event.getPath());
                    if (event.getPath().equals(RopZkUtils.COORDINATOR_PATH)) {
                        log.info("This should call elect again...");
                        executor.execute(() -> {
                            // If the node is deleted, attempt the re-election
                            log.info("Broker [{}] is calling re-election from the thread",
                                    brokerController.getBrokerAddress());
                            elect();
                        });
                    }
                } else {
                    log.warn("Got something wrong on watch: {}", event);
                }
            }, null);

            RopCoordinatorContent leaderBroker = jsonMapper.readValue(data, RopCoordinatorContent.class);
            currentCoordinator.set(leaderBroker);
            isCoordinator.set(false);
            elected = true;
//            brokerIsAFollowerNow();

            // If broker comes here it is a follower. Do nothing, wait for the watch to trigger
            log.info("Rop broker [{}] is the follower now. Waiting for the watch to trigger...",
                    brokerController.getBrokerAddress());

        } catch (NoNodeException nne) {
            // There's no leader yet... try to become the leader
            try {
                // Create the root node and add current broker's URL as its contents
                RopCoordinatorContent leaderBroker = new RopCoordinatorContent(brokerController.getBrokerAddress());
                ZkUtils.createFullPathOptimistic(pulsar.getLocalZkCache().getZooKeeper(), RopZkUtils.COORDINATOR_PATH,
                        jsonMapper.writeValueAsBytes(leaderBroker), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                // Update the current leader and set the flag to true
                currentCoordinator.set(leaderBroker);
                isCoordinator.set(true);
                elected = true;

                // Notify the listener that this broker is now the leader so that it can collect usage and start load
                // manager.
                log.info("Rop broker [{}] is the leader now, notifying the listener...",
                        brokerController.getBrokerAddress());
                becomeCoordinator();
            } catch (NodeExistsException nee) {
                // Re-elect the new leader
                log.warn("Got exception [{}] while creating election node because it already exists. "
                        + "Attempting re-election...", nee.getMessage());
                executor.execute(this::elect);
            } catch (Exception e) {
                // Kill the broker because this broker's session with zookeeper might be stale. Killing the broker will
                // make sure that we get the fresh zookeeper session.
                log.error("Got exception [{}] while creating the election node", e.getMessage());
                pulsar.getShutdownService().shutdown(-1);
            }

        } catch (Exception e) {
            // Kill the broker
            log.error("Could not get the content of [{}], got exception [{}]. Shutting down the broker...",
                    RopZkUtils.COORDINATOR_PATH, e);
            pulsar.getShutdownService().shutdown(-1);
        }
    }

    /**
     * broker become coordinator.
     */
    public void becomeCoordinator() {
        // TODO: hanmz 2021/9/8 加载topics

        // TODO: hanmz 2021/9/8 加载broker

    }

    public void removeBroker() {

    }

    public boolean isCoordinator() {
        return isCoordinator.get();
    }

    public boolean isElected() {
        return elected;
    }


    @Override
    public void close() throws Exception {
        log.info("Shutdown RopCoordinator");
        if (isCoordinator()) {
            try {
                ZookeeperUtils.deleteData(pulsar.getLocalZkCache().getZooKeeper(), RopZkUtils.COORDINATOR_PATH);
            } catch (Exception ex) {
                log.error("Delete rop coordinator zk node error", ex);
                throw ex;
            }
        }
    }
}
