package com.tencent.tdmq.handlers.rocketmq.inner.format;

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
    ByteBuf encode(final T record, final int numMessages);

    T decode(final Entry entry, final byte magic);

    default int parseNumMessages(final T record) {
        return 1;
    }
}
