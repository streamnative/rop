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

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.utils.ZookeeperUtils;

/**
 * Rop Zk client.
 */
@Slf4j
@Data
public class RopZkClient implements Watcher {

    private final RocketMQBrokerController brokerController;
    private ZooKeeper zooKeeper;

    public RopZkClient(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        this.zooKeeper = brokerController.getBrokerService().pulsar().getZkClient();

        // init rop zk node
        ZookeeperUtils.createPersistentNodeIfNotExist(zooKeeper, RopZkPath.ROP_PATH);

        // init broker zk node
        ZookeeperUtils.createPersistentNodeIfNotExist(zooKeeper, RopZkPath.BROKER_PATH);

        // init topic zk node
        ZookeeperUtils.createPersistentNodeIfNotExist(zooKeeper, RopZkPath.TOPIC_BASE_PATH);

        // init group zk node
        ZookeeperUtils.createPersistentNodeIfNotExist(zooKeeper, RopZkPath.GROUP_BASE_PATH);
    }

    @Override
    public void process(WatchedEvent event) {

    }
}
