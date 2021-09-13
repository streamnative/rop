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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath;
import org.testng.collections.Maps;

/**
 * Rop coordinator
 */
@Slf4j
public class RopCoordinator {

    private final RocketMQBrokerController brokerController;
    private final ObjectMapper jsonMapper;

    private PulsarService pulsar;
    private ExecutorService executor;
    private ZooKeeper zkClient;


    private final AtomicReference<RopCoordinatorBroker> currentCoordinator = new AtomicReference<RopCoordinatorBroker>();
    private final AtomicBoolean isCoordinator = new AtomicBoolean();
    private boolean elected = false;

    private final Map<String, Map<String, Integer[]>> topicRouteTableCache = Maps.newConcurrentMap();

    public RopCoordinator(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.jsonMapper = new ObjectMapper();
    }

    public void start() {
        this.pulsar = brokerController.getBrokerService().pulsar();
        this.zkClient = pulsar.getZkClient();
        this.executor = pulsar.getExecutor();
        elect();
    }

    private void elect() {
        try {
            byte[] data = zkClient.getData(RopZkPath.coordinatorPath, event -> {
                log.warn("Type of the event is [{}] and path is [{}]", event.getType(), event.getPath());
                if (event.getType() == EventType.NodeDeleted) {
                    log.warn("Election node {} is deleted, attempting re-election...", event.getPath());
                    if (event.getPath().equals(RopZkPath.coordinatorPath)) {
                        log.info("This should call elect again...");
                        executor.execute(() -> {
                            // If the node is deleted, attempt the re-election
                            log.info("Broker [{}] is calling re-election from the thread",
                                    pulsar.getSafeWebServiceAddress());
                            elect();
                        });
                    }
                } else {
                    log.warn("Got something wrong on watch: {}", event);
                }
            }, null);

            RopCoordinatorBroker leaderBroker = jsonMapper.readValue(data, RopCoordinatorBroker.class);
            currentCoordinator.set(leaderBroker);
            isCoordinator.set(false);
            elected = true;
//            brokerIsAFollowerNow();

            // If broker comes here it is a follower. Do nothing, wait for the watch to trigger
            log.info("Rop broker [{}] is the follower now. Waiting for the watch to trigger...",
                    pulsar.getSafeWebServiceAddress());

        } catch (NoNodeException nne) {
            // There's no leader yet... try to become the leader
            try {
                // Create the root node and add current broker's URL as its contents
                LeaderBroker leaderBroker = new LeaderBroker(pulsar.getSafeWebServiceAddress());
                ZkUtils.createFullPathOptimistic(pulsar.getLocalZkCache().getZooKeeper(), RopZkPath.coordinatorPath,
                        jsonMapper.writeValueAsBytes(leaderBroker), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                // Update the current leader and set the flag to true
                currentCoordinator.set(new RopCoordinatorBroker(leaderBroker.getServiceUrl()));
                isCoordinator.set(true);
                elected = true;

                // Notify the listener that this broker is now the leader so that it can collect usage and start load
                // manager.
                log.info("Rop broker [{}] is the leader now, notifying the listener...",
                        pulsar.getSafeWebServiceAddress());
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
                    RopZkPath.coordinatorPath, e);
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

    public void addBroker() {

    }

    public void removeBroker() {

    }

    public void rebalance() {

    }

    public void close() {
        if (isCoordinator()) {
            try {
                pulsar.getLocalZkCache().getZooKeeper().delete(RopZkPath.coordinatorPath, -1);
            } catch (Throwable t) {
                log.warn("Failed to cleanup election root znode", t);
            }
        }
        log.info("RopCoordinator stopped");
    }

    public boolean isCoordinator() {
        return isCoordinator.get();
    }

    public boolean isElected() {
        return elected;
    }


}
