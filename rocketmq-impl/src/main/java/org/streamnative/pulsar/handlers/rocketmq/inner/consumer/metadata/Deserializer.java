package org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata;

import java.nio.ByteBuffer;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopDecodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;

public interface Deserializer<T> {

    ByteBuffer encode() throws RopEncodeException;

    T decode(ByteBuffer buffer) throws RopDecodeException;

    int estimateSize();
}
