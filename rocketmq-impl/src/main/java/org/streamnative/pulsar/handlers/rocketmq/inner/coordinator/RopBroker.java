package org.streamnative.pulsar.handlers.rocketmq.inner.coordinator;

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
        ropZkClient.create(zkNodePath, "".getBytes());
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
