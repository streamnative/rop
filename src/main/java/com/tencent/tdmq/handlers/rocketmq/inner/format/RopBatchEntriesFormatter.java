package com.tencent.tdmq.handlers.rocketmq.inner.format;

import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.rocketmq.common.message.MessageExtBatch;

public class RopBatchEntriesFormatter implements EntryFormatter<MessageExtBatch> {

    @Override
    public ByteBuf encode(MessageExtBatch record, int numMessages) {
        return null;
    }

    @Override
    public MessageExtBatch decode(List<Entry> entries, byte magic) {
        return null;
    }

    @Override
    public int parseNumMessages(MessageExtBatch record) {
        return 0;
    }
}
