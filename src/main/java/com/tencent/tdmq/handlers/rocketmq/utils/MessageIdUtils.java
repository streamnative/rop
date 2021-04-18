package com.tencent.tdmq.handlers.rocketmq.utils;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

public class MessageIdUtils {

    // use 32 bits for ledgerId,
    // 22 bits for entryId,
    // 10 bits for partitionId.
    public static final int LEDGER_BITS = 48;
    public static final int ENTRY_BITS = 16;
    public static final int PARTITION_BITS = 0;

    public static final long getOffset(long ledgerId, long entryId) {
        // Combine ledger id and entry id to form offset
        checkArgument(ledgerId >= 0, "Expected ledgerId >= 0, but get " + ledgerId);
        checkArgument(entryId >= 0, "Expected entryId >= 0, but get " + entryId);

        long offset = (ledgerId << (ENTRY_BITS + PARTITION_BITS) | (entryId << PARTITION_BITS));
        return offset;
    }

    public static final long getOffset(long ledgerId, long entryId, int partitionId) {
        checkArgument(ledgerId >= 0, "Expected ledgerId >= 0, but get " + ledgerId);
        checkArgument(entryId >= 0, "Expected entryId >= 0, but get " + entryId);
        checkArgument(partitionId >= 0, "Expected batchIndex >= 0, but get " + partitionId);
        checkArgument(partitionId < (1 << PARTITION_BITS),
                "Expected batchIndex only take " + PARTITION_BITS + " bits, but it is " + partitionId);

        long offset = (ledgerId << (ENTRY_BITS + PARTITION_BITS) | (entryId << PARTITION_BITS)) + partitionId;
        return offset;
    }

    public static final MessageIdImpl getMessageId(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset > 0, "Expected Offset > 0, but get " + offset);

        long ledgerId = offset >>> (ENTRY_BITS + PARTITION_BITS);
        long entryId = (offset & 0x0F_FF_FF_FF_FF_FFL) >>> PARTITION_BITS;
        //int partitionId = (int) (offset & 0x2FF);

        return new MessageIdImpl(ledgerId, entryId, -1);
    }

    public static final PositionImpl getPosition(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);
        long ledgerId = offset >>> (ENTRY_BITS + PARTITION_BITS);
        long entryId = (offset & 0x0F_FF_FF_FF_FF_FFL) >>> PARTITION_BITS;

        return new PositionImpl(ledgerId, entryId);
    }

    // get the batchIndex contained in offset.
    public static final int getPartitionId(long offset) {
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);
        return (int) (offset & ((1 << PARTITION_BITS) - 1));
    }

    // get next offset that after partition Id.
    // In TopicConsumerManager, next read offset is updated after each entry reads,
    // if it read a batched message previously, the next offset waiting read is next entry.
    public static final long offsetAfterPartitionId(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);
        int partitionId = getPartitionId(offset);
        // this is a for
        if (partitionId != 0) {
            return (offset - partitionId) + (1 << PARTITION_BITS);
        }
        return offset;
    }
}
