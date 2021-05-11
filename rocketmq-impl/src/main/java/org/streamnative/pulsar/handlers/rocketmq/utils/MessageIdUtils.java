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

package org.streamnative.pulsar.handlers.rocketmq.utils;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * MessageID utils class.
 */
@Slf4j
public class MessageIdUtils {

    // use 32 bits for ledgerId,
    // 24 bits for entryId,
    // 8 bits for partitionId.
    public static final int LEDGER_BITS = 32;
    public static final int ENTRY_BITS = 24;
    public static final int PARTITION_BITS = 8;
    public static final long MAX_LEDGER_ID = (1L << LEDGER_BITS) - 2L;
    public static final long MAX_ENTRY_ID = (1L << ENTRY_BITS) - 2L;
    public static final long MAX_PARTITION_ID = (1L << (PARTITION_BITS - 1)) - 2L;
    public static final long MIN_ROP_OFFSET = 0L;
    private static final long MASK_LEDGER_ID = (1L << LEDGER_BITS) - 1L;
    private static final long MASK_ENTRY_ID = (1L << ENTRY_BITS) - 1L;
    private static final long MASK_PARTITION_ID = (1L << (PARTITION_BITS - 1)) - 1L;
    public static final long MAX_ROP_OFFSET =
            (MASK_PARTITION_ID << (LEDGER_BITS + ENTRY_BITS)) | (MASK_LEDGER_ID << ENTRY_BITS) | MASK_ENTRY_ID;

    public static final long getOffset(long ledgerId, long entryId, long partitionId) {
        entryId = entryId < 0L ? -1L : entryId;
        ledgerId = ledgerId < 0L ? -1L : ledgerId;
        partitionId = partitionId < 0L ? -1L : partitionId;
        if (entryId == Long.MAX_VALUE && ledgerId == Long.MAX_VALUE) {
            return MAX_ROP_OFFSET;
        }

        Preconditions.checkArgument(ledgerId <= MAX_LEDGER_ID, "ledgerId has overflow in rop.");
        Preconditions.checkArgument(entryId <= MAX_ENTRY_ID, "entryId has overflow in rop.");
        Preconditions.checkArgument(partitionId <= MAX_PARTITION_ID, "entryId has overflow in rop.");
        ledgerId = ledgerId + 1L;
        entryId = entryId + 1L;
        partitionId = partitionId + 1;
        return ((partitionId & MASK_PARTITION_ID) << (LEDGER_BITS + ENTRY_BITS))
                | ((ledgerId & MASK_LEDGER_ID) << ENTRY_BITS)
                | (entryId & MASK_ENTRY_ID);
    }

    public static final long getOffset(MessageIdImpl messageId) {
        return getOffset(messageId.getLedgerId(), messageId.getEntryId(), messageId.getPartitionIndex());
    }

    public static final MessageIdImpl getMessageId(long offset) {
        if (offset <= MIN_ROP_OFFSET) {
            return (MessageIdImpl) MessageId.earliest;
        } else if (offset == MAX_ROP_OFFSET) {
            return (MessageIdImpl) MessageId.latest;
        }
        int partitionId = (int) ((offset >>> (LEDGER_BITS + ENTRY_BITS)) & MASK_PARTITION_ID);
        long ledgerId = (offset >>> ENTRY_BITS) & MASK_LEDGER_ID;
        long entryId = offset & MASK_ENTRY_ID;

        partitionId -= 1L;
        ledgerId -= 1L;
        entryId -= 1L;

        return new MessageIdImpl(ledgerId, entryId, partitionId);
    }

    public static final PositionImpl getPosition(long offset) {
        MessageIdImpl messageId = getMessageId(offset);
        return new PositionImpl(messageId.getLedgerId(), messageId.getEntryId());
    }

    public static boolean isMinOffset(long offset) {
        return offset <= MIN_ROP_OFFSET;
    }

    public static boolean isMaxOffset(long offset) {
        return offset == MAX_ROP_OFFSET;
    }

    public static boolean isMessageEquals(MessageId left, MessageId right) {
        if (left != null && right != null) {
            MessageIdImpl leftMsgId = (MessageIdImpl) left;
            MessageIdImpl rightMsgId = (MessageIdImpl) right;
            return (leftMsgId.getLedgerId() == rightMsgId.getLedgerId())
                    && (leftMsgId.getEntryId() == rightMsgId.getEntryId())
                    && (leftMsgId.getPartitionIndex() == rightMsgId.getPartitionIndex());
        }
        return false;
    }
}
