package org.streamnative.pulsar.handlers.rocketmq.inner;

import org.apache.rocketmq.store.PutMessageResult;

/**
 * Put message callback.
 */
public interface PutMessageCallback {

    default void callback(PutMessageResult putMessageResult) {
    }
}
