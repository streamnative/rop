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

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooKeeper;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath;
import org.streamnative.pulsar.handlers.rocketmq.utils.ZookeeperUtils;

/**
 * Rop broker.
 */
@Slf4j
public class RopBroker {

    private final RocketMQBrokerController brokerController;
    private ZooKeeper zkClient;
    private String zkNodePath;

    public RopBroker(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        log.info("Start RopBroker");
        this.zkClient = brokerController.getBrokerService().pulsar().getZkClient();
        this.zkNodePath = RopZkPath.BROKER_PATH + "/" + brokerController.getBrokerAddress();
        ZookeeperUtils.createPersistentNodeIfNotExist(zkClient, zkNodePath);
    }

    public void shutdown() {
        log.info("Shutdown RopBroker");
        try {
            ZookeeperUtils.deleteData(zkClient, zkNodePath);
        } catch (Throwable t) {
            log.error("Delete rop broker zk node error", t);
        }
    }
}
