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

package com.tencent.tdmq.handlers.rocketmq;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;

/**
 * Builder RocketMQ Standalone env.
 */
public class RocketMQStandaloneBuilder {

    private RocketMQStandalone rocketmqStandalone;

    private RocketMQStandaloneBuilder() {
        rocketmqStandalone = new RocketMQStandalone();
        rocketmqStandalone.setWipeData(true);
        rocketmqStandalone.setNoFunctionsWorker(true);
    }

    public static RocketMQStandaloneBuilder instance() {
        return new RocketMQStandaloneBuilder();
    }

    public RocketMQStandaloneBuilder withConfig(ServiceConfiguration config) {
        rocketmqStandalone.setConfig(config);
        return this;
    }

    public RocketMQStandaloneBuilder withWipeData(boolean wipeData) {
        rocketmqStandalone.setWipeData(wipeData);
        return this;
    }

    public RocketMQStandaloneBuilder withNumOfBk(int numOfBk) {
        rocketmqStandalone.setNumOfBk(numOfBk);
        return this;
    }

    public RocketMQStandaloneBuilder withZkPort(int zkPort) {
        rocketmqStandalone.setZkPort(zkPort);
        return this;
    }

    public RocketMQStandaloneBuilder withBkPort(int bkPort) {
        rocketmqStandalone.setBkPort(bkPort);
        return this;
    }

    public RocketMQStandaloneBuilder withZkDir(String zkDir) {
        rocketmqStandalone.setZkDir(zkDir);
        return this;
    }

    public RocketMQStandaloneBuilder withBkDir(String bkDir) {
        rocketmqStandalone.setBkDir(bkDir);
        return this;
    }

    public RocketMQStandaloneBuilder withNoBroker(boolean noBroker) {
        rocketmqStandalone.setNoBroker(noBroker);
        return this;
    }

    public RocketMQStandaloneBuilder withOnlyBroker(boolean onlyBroker) {
        rocketmqStandalone.setOnlyBroker(onlyBroker);
        return this;
    }

    public RocketMQStandaloneBuilder withNoStreamStorage(boolean noStreamStorage) {
        rocketmqStandalone.setNoStreamStorage(noStreamStorage);
        return this;
    }

    public RocketMQStandaloneBuilder withStreamStoragePort(int streamStoragePort) {
        rocketmqStandalone.setStreamStoragePort(streamStoragePort);
        return this;
    }

    public RocketMQStandaloneBuilder withAdvertisedAddress(String advertisedAddress) {
        rocketmqStandalone.setAdvertisedAddress(advertisedAddress);
        return this;
    }

    public RocketMQStandalone build() {
        ServiceConfiguration config = rocketmqStandalone.getConfig();
        if (config == null) {
            config = new ServiceConfiguration();
            config.setClusterName("standalone");
            rocketmqStandalone.setConfig(config);
        }

        String zkServers = "127.0.0.1";

        if (rocketmqStandalone.getAdvertisedAddress() != null) {
            // Use advertised address from command line
            rocketmqStandalone.getConfig().setAdvertisedAddress(rocketmqStandalone.getAdvertisedAddress());
            zkServers = rocketmqStandalone.getAdvertisedAddress();
        } else if (isBlank(rocketmqStandalone.getConfig().getAdvertisedAddress())) {
            // Use advertised address as local hostname
            rocketmqStandalone.getConfig().setAdvertisedAddress(ServiceConfigurationUtils.unsafeLocalhostResolve());
        } else {
            // Use advertised address from config file
        }

        // Set ZK server's host to localhost
        rocketmqStandalone.getConfig().setZookeeperServers(zkServers + ":" + rocketmqStandalone.getZkPort());
        rocketmqStandalone.getConfig().setConfigurationStoreServers(zkServers + ":" + rocketmqStandalone.getZkPort());
        rocketmqStandalone.getConfig().setRunningStandalone(true);
        return rocketmqStandalone;
    }

}