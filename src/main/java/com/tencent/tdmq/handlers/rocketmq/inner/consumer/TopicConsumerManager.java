package com.tencent.tdmq.handlers.rocketmq.inner.consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils.offsetAfterBatchIndex;

import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils;
import io.netty.channel.ChannelHandlerContext;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;

@Slf4j
public class TopicConsumerManager {

    private final PersistentTopic topic;
    private final RocketMQBrokerController brokerController;

    // the lock for closed status change.
    // once closed, should not add new cursor back, since consumers are cleared.
    private final ReentrantReadWriteLock rwLock;
    // keep fetch offset and related cursor. keep cursor and its last offset in Pair. <offset, pair>
    @Getter
    private final ConcurrentLongHashMap<Pair<ManagedCursor, Long>> consumers;
    // used to track all created cursor, since above consumers may be remove and in fly,
    // use this map will not leak cursor when close.
    private final ConcurrentMap<String, ManagedCursor> createdCursors;
    // track last access time(millis) for offsets <offset, time>
    @Getter
    private final ConcurrentLongHashMap<Long> lastAccessTimes;
    private boolean closed;

    public TopicConsumerManager(RocketMQBrokerController brokerController, PersistentTopic topic) {
        this.topic = topic;
        this.consumers = new ConcurrentLongHashMap<>();
        this.createdCursors = new ConcurrentHashMap<>();
        this.lastAccessTimes = new ConcurrentLongHashMap<>();
        this.brokerController = brokerController;
        this.rwLock = new ReentrantReadWriteLock();
        this.closed = false;
    }

    // delete expired cursors, so backlog can be cleared.
    void deleteExpiredCursor(final ChannelHandlerContext ctx, long current, long expirePeriodMillis) {
        lastAccessTimes.forEach((offset, lastAccessTime) -> {
            if (current - lastAccessTime - expirePeriodMillis > 0) {
                deleteOneExpiredCursor(ctx, offset);
            }
        });
    }

    void deleteOneExpiredCursor(final ChannelHandlerContext ctx, long offset) {
        Pair<ManagedCursor, Long> pair;

        // need not do anything, since this tcm already in closing state. and close() will delete every thing.
        rwLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            pair = consumers.remove(offset);
            lastAccessTimes.remove(offset);
        } finally {
            rwLock.readLock().unlock();
        }

        if (pair != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cursor timed out for offset: {} - {}, cursors cache size: {}",
                        ctx.channel(), offset, MessageIdUtils.getPosition(offset), consumers.size());
            }

            ManagedCursor managedCursor = pair.getKey();
            deleteOneCursorAsync(ctx, managedCursor, "cursor expired");
        }
    }

    // delete passed in cursor.
    void deleteOneCursorAsync(final ChannelHandlerContext ctx, ManagedCursor cursor, String reason) {
        if (cursor != null) {
            topic.getManagedLedger().asyncDeleteCursor(cursor.getName(), new DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object obj) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Cursor {} for topic {} deleted successfully for reason: {}.",
                                ctx.channel(), cursor.getName(), topic.getName(), reason);
                    }
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object obj) {
                    log.warn("[{}] Error deleting cursor for topic {} for reason: {}.",
                            ctx.channel(), cursor.getName(), topic.getName(), reason, exception);
                }
            }, null);
            createdCursors.remove(cursor.getName());
        }
    }

    // get one cursor offset pair.
    // remove from cache, so another same offset read could happen.
    // each success remove should have a following add.
    public Pair<ManagedCursor, Long> remove(final ChannelHandlerContext ctx, long offset) {
        Pair<ManagedCursor, Long> cursor;

        // should not return cursor for Fetch to read, since this tcm already in closing state.
        rwLock.readLock().lock();
        try {
            if (closed) {
                return null;
            }
            cursor = consumers.remove(offset);
            lastAccessTimes.remove(offset);
        } finally {
            rwLock.readLock().unlock();
        }

        if (cursor != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Get cursor for offset: {} - {} in cache. cache size: {}",
                        ctx.channel(), offset, MessageIdUtils.getPosition(offset), consumers.size());
            }
            return cursor;
        }

        return createCursorIfNotExists(ctx, offset);
    }

    private Pair<ManagedCursor, Long> createCursorIfNotExists(final ChannelHandlerContext ctx, long offset) {
        // This is for read a new entry, first check if offset is from a batched message request.
        offset = offsetAfterBatchIndex(offset);

        Pair<ManagedCursor, Long> cursor;

        rwLock.readLock().lock();
        try {
            if (closed) {
                return null;
            }
            // handle offset not exist in consumers, need create cursor.
            consumers.computeIfAbsent(
                    offset,
                    off -> {
                        PositionImpl position = MessageIdUtils.getPosition(off);

                        String cursorName = "kop-consumer-cursor-" + topic.getName()
                                + "-" + position.getLedgerId() + "-" + position.getEntryId()
                                + "-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);

                        // get previous position, because NonDurableCursor is read from next position.
                        ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
                        PositionImpl previous = ledger.getPreviousPosition(position);
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Create cursor {} for offset: {}. position: {}, previousPosition: {}",
                                    ctx.channel(), cursorName, off, position, previous);
                        }
                        ManagedCursor newCursor;
                        try {
                            newCursor = ledger.newNonDurableCursor(previous, cursorName);
                            createdCursors.put(newCursor.getName(), newCursor);
                        } catch (ManagedLedgerException e) {
                            log.error(
                                    "[{}] Error new cursor for topic {} at offset {} - {}. will cause fetch data error.",
                                    ctx.channel(), topic.getName(), off, previous, e);
                            return null;
                        }

                        lastAccessTimes.put(off, System.currentTimeMillis());
                        return Pair.of(newCursor, off);
                    });

            // notice:  above would add a <offset, null-Pair>
            cursor = consumers.remove(offset);
            lastAccessTimes.remove(offset);
        } finally {
            rwLock.readLock().unlock();
        }

        return cursor;
    }

    // once entry read complete, add new offset back.
    public void add(final ChannelHandlerContext ctx, long offset, Pair<ManagedCursor, Long> pair) {
        checkArgument(offset == pair.getRight(),
                "offset not equal. key: " + offset + " value: " + pair.getRight());

        rwLock.readLock().lock();
        // should delete the cursor since this tcm already in closing state.
        try {
            if (closed) {
                ManagedCursor managedCursor = pair.getLeft();
                deleteOneCursorAsync(ctx, managedCursor, "A race - add cursor back but tcm already closed");
                return;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        Pair<ManagedCursor, Long> oldPair = consumers.putIfAbsent(offset, pair);
        if (oldPair != null) {
            deleteOneCursorAsync(ctx, pair.getLeft(), "reason: A race - same cursor already cached");
        }
        lastAccessTimes.put(offset, System.currentTimeMillis());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Add cursor back {} for offset: {} - {}",
                    ctx.channel(), pair.getLeft().getName(), offset, MessageIdUtils.getPosition(offset));
        }
    }

    // called when channel closed.
    public void close() {
        ConcurrentLongHashMap<Pair<ManagedCursor, Long>> consumersToClose;
        ConcurrentMap<String, ManagedCursor> cursorsToClose;
        rwLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Close TCM for topic {}.", topic.getName());
            }
            consumersToClose = new ConcurrentLongHashMap<>();
            consumers.forEach((k, v) -> consumersToClose.put(k, v));
            consumers.clear();
            lastAccessTimes.clear();
            cursorsToClose = new ConcurrentHashMap<>();
            createdCursors.forEach((k, v) -> cursorsToClose.put(k, v));
            createdCursors.clear();
        } finally {
            rwLock.writeLock().unlock();
        }

/*        consumersToClose.values()
                .forEach(pair -> {
                    ManagedCursor cursor = pair.getLeft();
                    deleteOneCursorAsync(cursor, "TopicConsumerManager close");
                    if (null != cursor) {
                        cursorsToClose.remove(cursor.getName());
                    }
                });

        // delete dangling createdCursors
        cursorsToClose.values().forEach(cursor ->
                deleteOneCursorAsync(cursor, "TopicConsumerManager close but cursor is still outstanding"));*/
        cursorsToClose.clear();
    }
}
