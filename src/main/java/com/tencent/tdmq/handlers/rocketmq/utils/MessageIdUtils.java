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

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * MessageID utils class.
 */
public class MessageIdUtils {

    // use 48 bits for ledgerId,
    // 16 bits for entryId,
    // 0 bits for partitionId.
    public static final int LEDGER_BITS = 48;
    public static final int ENTRY_BITS = 16;
    public static final long MAX_LEDGER_ID = (1L << (LEDGER_BITS -1)) - 1;
    public static final long MAX_ENTRY_ID = (1L << ENTRY_BITS) - 1;
    public static final long MAX_ROP_OFFSET = (MAX_LEDGER_ID << ENTRY_BITS) | MAX_ENTRY_ID;
    public static final long MIN_ROP_OFFSET = 0L;

    public static final long getOffset(long ledgerId, long entryId) {
        entryId = entryId < 0L ? 0L : entryId;
        ledgerId = ledgerId < 0L ? 0L : ledgerId;
        long offset = ((ledgerId & MAX_LEDGER_ID) << ENTRY_BITS) | (entryId & MAX_ENTRY_ID);
        return offset;
    }

    public static final long getOffset(long ledgerId, long entryId, int partitionId) {
        return getOffset(ledgerId, entryId);
    }

    public static final MessageIdImpl getMessageId(long offset) {
        long ledgerId = (offset >>> ENTRY_BITS) & MAX_LEDGER_ID;
        long entryId = offset & MAX_ENTRY_ID;
        return new MessageIdImpl(ledgerId, entryId, -1);
    }

    public static final PositionImpl getPosition(long offset) {
        MessageIdImpl messageId = getMessageId(offset);
        return new PositionImpl(messageId.getLedgerId(), messageId.getEntryId());
    }
}
