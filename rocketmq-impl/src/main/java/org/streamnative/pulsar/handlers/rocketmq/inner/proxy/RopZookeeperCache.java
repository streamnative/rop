package org.streamnative.pulsar.handlers.rocketmq.inner.proxy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopServerException;

/**
 * Per ZK client ZooKeeper cache supporting ZNode data and children list caches. A cache entry is identified, accessed
 * and invalidated by the ZNode path. For the data cache, ZNode data parsing is done at request time with the given
 * {@link Deserializer} argument.
 */
@Slf4j
public class RopZookeeperCache extends ZooKeeperCache implements Closeable {

    private final ZooKeeperClientFactory zlClientFactory;
    private final int zkSessionTimeoutMillis;
    private final String ropZkConnect;
    private final ScheduledExecutorService scheduledExecutor;
    private final OrderedExecutor orderedExecutor;

    public RopZookeeperCache(ZooKeeperClientFactory zkClientFactory, int zkSessionTimeoutMillis,
            int zkOperationTimeoutSeconds, String ropZkConnect, OrderedExecutor orderedExecutor,
            ScheduledExecutorService scheduledExecutor, int cacheExpirySeconds) {
        super("rop-zk", null, zkOperationTimeoutSeconds, orderedExecutor, cacheExpirySeconds);
        this.zlClientFactory = zkClientFactory;
        this.zkSessionTimeoutMillis = zkSessionTimeoutMillis;
        this.ropZkConnect = ropZkConnect;
        this.orderedExecutor = orderedExecutor;
        this.scheduledExecutor = scheduledExecutor;
    }

    public void start() throws RopServerException {
        CompletableFuture<ZooKeeper> zkFuture = zlClientFactory.create(ropZkConnect, SessionType.ReadWrite,
                zkSessionTimeoutMillis);
        // Initial session creation with global ZK must work
        try {
            ZooKeeper newSession = zkFuture.get(zkSessionTimeoutMillis, TimeUnit.MILLISECONDS);
            // Register self as a watcher to receive notification when session expires and trigger a new session to be
            // created
            newSession.register(this);
            zkSession.set(newSession);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to establish rop zookeeper session: {}", e.getMessage(), e);
            throw new RopServerException(e);
        }
    }

    public void close() throws IOException {
        ZooKeeper currentSession = zkSession.getAndSet(null);
        if (currentSession != null) {
            try {
                currentSession.close();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        super.stop();
    }

    @Override
    public <T> void process(WatchedEvent event, final CacheUpdater<T> updater) {
        synchronized (this) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Got Rop ZooKeeper WatchdEvent: EventType: {}, KeeperState: {}, path: {}",
                        this.hashCode(), event.getType(), event.getState(), event.getPath());
            }
            if (event.getType() == Event.EventType.None) {
                switch (event.getState()) {
                    case Expired:
                        // in case of expired, the zkSession is no longer good for sure.
                        // We need to restart the session immediately.
                        // cancel any timer event since it is already bad
                        ZooKeeper oldSession = this.zkSession.getAndSet(null);
                        log.warn("Global ZK session lost. Triggering reconnection {}", oldSession);
                        safeCloseZkSession(oldSession);
                        asyncRestartZooKeeperSession();
                        return;
                    case SyncConnected:
                    case ConnectedReadOnly:
                        checkNotNull(zkSession.get());
                        log.info("Global ZK session {} restored connection.", zkSession.get());

                        dataCache.synchronous().invalidateAll();
                        childrenCache.synchronous().invalidateAll();
                        return;
                    default:
                        break;
                }
            }
        }

        // Other types of events
        super.process(event, updater);

    }

    protected void asyncRestartZooKeeperSession() {
        // schedule a re-connect event using this as the watch
        log.info("Re-starting rop ZK session in the background...");

        CompletableFuture<ZooKeeper> zkFuture = zlClientFactory.create(ropZkConnect, SessionType.AllowReadOnly,
                zkSessionTimeoutMillis);
        zkFuture.thenAccept(zk -> {
            if (zkSession.compareAndSet(null, zk)) {
                log.info("Successfully re-created rop ZK session: {}", zk);
            } else {
                // Other reconnection happened, we can close the session
                safeCloseZkSession(zk);
            }
        }).exceptionally(ex -> {
            log.warn("Failed to re-create rop ZK session: {}", ex.getMessage());

            // Schedule another attempt for later
            scheduledExecutor.schedule(() -> {
                asyncRestartZooKeeperSession();
            }, 10, TimeUnit.SECONDS);
            return null;
        });
    }

    private void safeCloseZkSession(ZooKeeper zkSession) {
        if (zkSession != null) {
            if (log.isDebugEnabled()) {
                log.debug("Closing rop zkSession:{}", zkSession.getSessionId());
            }
            try {
                zkSession.close();
            } catch (Exception e) {
                log.info("Closing rop ZK Session encountered an exception: {}. Disposed anyway.", e.getMessage());
            }
        }
    }
}
