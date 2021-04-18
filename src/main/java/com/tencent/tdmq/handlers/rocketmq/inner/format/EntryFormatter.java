package com.tencent.tdmq.handlers.rocketmq.inner.format;

import com.tencent.tdmq.handlers.rocketmq.inner.exception.RopEncodeException;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandMessage;
import org.apache.pulsar.client.api.Message;

public interface EntryFormatter<T> {

    /**
     * Encode RocketMQ record/Batch records to a ByteBuf.
     *
     * @param record with RocketMQ's format
     * @param numMessages the number of messages
     * @return the ByteBuf of an entry that is to be written to Bookie
     */
    List<ByteBuffer> encode(final T record, final int numMessages) throws RopEncodeException;

    List<T> decode(CommandMessage commandMessage, ByteBuf headersAndPayload);
    List<T> decodePulsarEntry(final List<Message> entries);
    List<T> decodePulsarMessage(final List<Message> entries, Predicate predicate);

    default int parseNumMessages(final T record) {
        return 1;
    }
}
