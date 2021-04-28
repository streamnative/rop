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

package com.tencent.tdmq.handlers.rocketmq.utils;

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

    // use 40 bits for ledgerId,
    // 24 bits for entryId,
    // 0 bits for partitionId.
    public static final int LEDGER_BITS = 40;
    public static final int ENTRY_BITS = 24;
    public static final long MAX_LEDGER_ID = (1L << (LEDGER_BITS - 1)) - 1L;
    public static final long MAX_ENTRY_ID =
            (1L << ENTRY_BITS) - 1; // 65535 stand for the offset of -1, 65534 stand fo the max offset;
    public static final long MAX_ROP_OFFSET = (MAX_LEDGER_ID << ENTRY_BITS) | MAX_ENTRY_ID;
    public static final long MIN_ROP_OFFSET = -1L;

    public static final long getOffset(long ledgerId, long entryId) {
        entryId = entryId < 0L ? -1L : entryId;
        ledgerId = ledgerId < 0L ? -1L : ledgerId;
        if (entryId == -1 && ledgerId == -1) {
            return -1L;
        } else if (entryId == Long.MAX_VALUE && ledgerId == Long.MAX_VALUE) {
            return MAX_ROP_OFFSET;
        }
        if (entryId >= MAX_ENTRY_ID) {
            log.error("The entryID has overflow in [{} : {}]", ledgerId, entryId);
        }
        Preconditions.checkArgument(ledgerId <= MAX_LEDGER_ID, "ledgerId has overflow in rop.");
        Preconditions.checkArgument(entryId < MAX_ENTRY_ID, "entryId has overflow in rop.");
        entryId = entryId + 1L;
        long offset = ((ledgerId & MAX_LEDGER_ID) << ENTRY_BITS) | (entryId & MAX_ENTRY_ID);
        return offset;
    }

    public static final long getOffset(long ledgerId, long entryId, int partitionId) {
        return getOffset(ledgerId, entryId);
    }

    public static final long getOffset(MessageIdImpl messageId) {
        return getOffset(messageId.getLedgerId(), messageId.getEntryId());
    }

    public static final MessageIdImpl getMessageId(long offset) {
        if (offset < 0) {
            return (MessageIdImpl) MessageId.earliest;
        } else if (offset == MAX_ROP_OFFSET) {
            return (MessageIdImpl) MessageId.latest;
        }
        long ledgerId = (offset >>> ENTRY_BITS) & MAX_LEDGER_ID;
        long entryId = offset & MAX_ENTRY_ID;
        entryId -= 1;
        return new MessageIdImpl(ledgerId, entryId, -1);
    }

    public static final PositionImpl getPosition(long offset) {
        MessageIdImpl messageId = getMessageId(offset);
        return new PositionImpl(messageId.getLedgerId(), messageId.getEntryId());
    }

    public boolean isMinOffset(long offset) {
        return offset <= MIN_ROP_OFFSET;
    }

    public boolean isMaxOffset(long offset) {
        return offset == MAX_ROP_OFFSET;
    }
}
