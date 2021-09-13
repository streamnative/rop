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

import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkClient;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopZkPath;

/**
 * Rop broker
 */
@Slf4j
public class RopBroker {

    private final RopZkClient ropZkClient;

    private final String zkNodePath;


    public RopBroker(RocketMQBrokerController rocketBroker, RopZkClient ropZkClient) {
        this.ropZkClient = ropZkClient;
        this.zkNodePath = RopZkPath.brokerPath + "/" + rocketBroker.getBrokerHost();
    }

    public void start() {
        ropZkClient.create(zkNodePath, "".getBytes(StandardCharsets.UTF_8));
    }

    public void close() {
        try {
            ropZkClient.delete(zkNodePath);
        } catch (Throwable t) {
            log.warn("Failed to delete rop broker.", t);
        }
        log.info("RopBroker stopped");
    }

}
