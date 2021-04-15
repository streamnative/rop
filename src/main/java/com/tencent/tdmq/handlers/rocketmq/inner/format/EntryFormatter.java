package com.tencent.tdmq.handlers.rocketmq.inner.format;

import com.tencent.tdmq.handlers.rocketmq.inner.exception.RopEncodeException;
import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;

public interface EntryFormatter<T> {

    /**
     * Encode RocketMQ record/Batch records to a ByteBuf.
     *
     * @param record with RocketMQ's format
     * @param numMessages the number of messages
     * @return the ByteBuf of an entry that is to be written to Bookie
     */
    List<ByteBuf> encode(final T record, final int numMessages) throws RopEncodeException;

    List<T> decode(final List<Entry> entries, final byte magic);

    default int parseNumMessages(final T record) {
        return 1;
    }
}
