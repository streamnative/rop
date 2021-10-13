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
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

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
        Preconditions.checkArgument(partitionId <= MAX_PARTITION_ID, "partitionId has overflow in rop.");
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


    public static long getLastMessageIndex(ManagedLedger managedLedger) {
        return ((ManagedLedgerInterceptorImpl) managedLedger.getManagedLedgerInterceptor()).getIndex();
    }

    public static boolean hasMessagesInQueue(ManagedLedger managedLedger) {
        return getLastMessageIndex(managedLedger) >= 0;
    }

    public static long getLogEndOffset(ManagedLedger managedLedger) {
        return getLastMessageIndex(managedLedger) + 1;
    }

    public static PositionImpl getFirstPosition(ManagedLedger managedLedger) {
        ManagedLedgerImpl managedLedgerImpl = (ManagedLedgerImpl) managedLedger;
        Long ledgeId = managedLedgerImpl.getLedgersInfo().firstKey();
        Preconditions.checkNotNull(ledgeId);
        PositionImpl firstPosition = new PositionImpl(ledgeId, -1);
        return managedLedgerImpl.getNextValidPosition(firstPosition);
    }

    public static long getPublishTime(final ByteBuf byteBuf) {
        final int readerIndex = byteBuf.readerIndex();
        Commands.skipBrokerEntryMetadataIfExist(byteBuf);
        final MessageMetadata metadata = Commands.parseMessageMetadata(byteBuf);
        byteBuf.readerIndex(readerIndex);
        return metadata.getPublishTime();
    }

    public static long getQueueOffsetByPosition(PersistentTopic pulsarTopic, Position pulsarPosition) {
        Preconditions.checkNotNull(pulsarTopic);
        Preconditions.checkArgument(pulsarPosition instanceof PositionImpl);
        Long queueOffset = getLogEndOffset(pulsarTopic.getManagedLedger());
        try {
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) pulsarTopic.getManagedLedger();
            return getOffsetOfPosition(managedLedger,
                    (PositionImpl) pulsarPosition, false, -1).join();
        } catch (Exception e) {
            log.warn("get offset of position[{}] error.", pulsarPosition);
        }
        return queueOffset;
    }

    public static CompletableFuture<Long> getOffsetOfPosition(
            ManagedLedgerImpl managedLedger,
            PositionImpl position,
            boolean needCheckMore,
            long timestamp) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        managedLedger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                log.debug("{}--->{}", managedLedger.toString(), position);
                future.completeExceptionally(exception);
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    if (needCheckMore) {
                        long offset = peekOffsetFromEntry(entry);
                        final long publishTime = getPublishTime(entry.getDataBuffer());
                        if (publishTime >= timestamp) {
                            future.complete(offset);
                        } else {
                            future.complete(offset + 1);
                        }
                    } else {
                        future.complete(peekBaseOffsetFromEntry(entry));
                    }
                } catch (Exception e) {
                    future.completeExceptionally(e);
                } finally {
                    if (entry != null) {
                        entry.release();
                    }
                }

            }
        }, null);
        return future;
    }

    public static PositionImpl getPositionForOffset(ManagedLedger managedLedger, Long offset) {
        try {
            return (PositionImpl) managedLedger.asyncFindPosition(new OffsetSearchPredicate(offset)).get();
        } catch (Exception e) {
            log.error("[{}] Failed to find position for offset {}", managedLedger.getName(), offset);
            throw new RuntimeException(managedLedger.getName() + " failed to find position for offset " + offset);
        }
    }

    public static PositionImpl getPreviousPosition(ManagedLedger managedLedger, PositionImpl position) {
        try {
            return ((ManagedLedgerImpl) managedLedger).getPreviousPosition(position);
        } catch (Exception e) {
            log.error("[{}] Failed to find position for offset {}", managedLedger.getName(), position);
            throw new RuntimeException(managedLedger.getName() + " failed to find position for offset " + position);
        }
    }

    public static BrokerEntryMetadata peekBrokerEntryMetadata(ByteBuf byteBuf) {
        final int readerIndex = byteBuf.readerIndex();
        BrokerEntryMetadata entryMetadata =
                Commands.parseBrokerEntryMetadataIfExist(byteBuf);
        byteBuf.readerIndex(readerIndex);
        return entryMetadata;
    }

    public static long peekOffsetFromEntry(Entry entry) {
        return Commands.peekBrokerEntryMetadataIfExist(entry.getDataBuffer()).getIndex();
    }

    public static long peekBaseOffsetFromEntry(Entry entry) {

        return peekOffsetFromEntry(entry)
                - Commands.peekMessageMetadata(entry.getDataBuffer(), null, 0)
                .getNumMessagesInBatch() + 1;
    }

    public static long getMockOffset(long ledgerId, long entryId) {
        return ledgerId + entryId;
    }

    public static long peekTimestampFromEntry(Entry entry) {
        return Commands.peekBrokerEntryMetadataIfExist(entry.getDataBuffer()).getBrokerTimestamp();
    }
}
