package com.tencent.tdmq.handlers.rocketmq.utils;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

public class MessageIdUtils {

    // use 32 bits for ledgerId,
    // 32 bits for entryId,
    public static final int LEDGER_BITS = 32;
    public static final int ENTRY_BITS = 32;

    public static final long getOffset(long ledgerId, long entryId) {
        // Combine ledger id and entry id to form offset
        checkArgument(ledgerId >= 0, "Expected ledgerId >= 0, but get " + ledgerId);
        checkArgument(entryId >= 0, "Expected entryId >= 0, but get " + entryId);

        return  (ledgerId << (LEDGER_BITS) | (entryId << ENTRY_BITS));
    }

    public static final MessageId getMessageId(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset > 0, "Expected Offset > 0, but get " + offset);

        long ledgerId = offset >>> (LEDGER_BITS);
        long entryId = offset >>> (ENTRY_BITS);

        return new MessageIdImpl(ledgerId, entryId, -1);
    }

    public static final PositionImpl getPosition(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);

        long ledgerId = offset >>> (LEDGER_BITS);
        long entryId = offset >>> (ENTRY_BITS);

        return new PositionImpl(ledgerId, entryId);
    }

    // get the batchIndex contained in offset.
    public static final int getBatchIndex(long offset) {
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);

        return (int) (offset & 0x0F_FF);
    }

    // get next offset that after batch Index.
    // In TopicConsumerManager, next read offset is updated after each entry reads,
    // if it read a batched message previously, the next offset waiting read is next entry.
    public static final long offsetAfterBatchIndex(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);
        return offset;
    }
}
