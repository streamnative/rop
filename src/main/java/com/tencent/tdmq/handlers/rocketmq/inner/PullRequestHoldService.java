package com.tencent.tdmq.handlers.rocketmq.inner;

import com.tencent.tdmq.handlers.rocketmq.RocketMQBrokerController;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.ManyPullRequest;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt.CqExtUnit;
@Slf4j
public class PullRequestHoldService extends ServiceThread {
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final RocketMQBrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();
    private ConcurrentMap<String, ManyPullRequest> pullRequestTable = new ConcurrentHashMap(1024);

    public PullRequestHoldService(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendPullRequest(String topic, int queueId, PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = (ManyPullRequest)this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = (ManyPullRequest)this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(String topic, int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append("@");
        sb.append(queueId);
        return sb.toString();
    }

    public void run() {
        log.info("{} service started", this.getServiceName());

        while(!this.isStopped()) {
            try {
                if (this.brokerController.getServerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5000L);
                } else {
                    this.waitForRunning(this.brokerController.getServerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5000L) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable var5) {
                log.warn(this.getServiceName() + " service has exception. ", var5);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    public String getServiceName() {
        return org.apache.rocketmq.broker.longpolling.PullRequestHoldService.class.getSimpleName();
    }

    private void checkHoldRequest() {
        Iterator var1 = this.pullRequestTable.keySet().iterator();

        while(var1.hasNext()) {
            String key = (String)var1.next();
            String[] kArray = key.split("@");
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);

                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable var9) {
                    log.error("check hold request failed. topic={}, queueId={}", new Object[]{topic, queueId, var9});
                }
            }
        }

    }

    public void notifyMessageArriving(String topic, int queueId, long maxOffset) {
        this.notifyMessageArriving(topic, queueId, maxOffset, (Long)null, 0L, (byte[])null, (Map)null);
    }

    public void notifyMessageArriving(String topic, int queueId, long maxOffset, Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = (ManyPullRequest)this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList();
                Iterator var14 = requestList.iterator();

                while(true) {
                    while(var14.hasNext()) {
                        PullRequest request = (PullRequest)var14.next();
                        long newestOffset = maxOffset;
                        if (maxOffset <= request.getPullFromThisOffset()) {
                            newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                        }

                        if (newestOffset > request.getPullFromThisOffset()) {
                            boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode, new CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                            if (match && properties != null) {
                                match = request.getMessageFilter().isMatchedByCommitLog((ByteBuffer)null, properties);
                            }

                            if (match) {
                                try {
                                    this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                                } catch (Throwable var21) {
                                    log.error("execute request when wakeup failed.", var21);
                                }
                                continue;
                            }
                        }

                        if (System.currentTimeMillis() >= request.getSuspendTimestamp() + request.getTimeoutMillis()) {
                            try {
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                            } catch (Throwable var20) {
                                log.error("execute request when wakeup failed.", var20);
                            }
                        } else {
                            replayList.add(request);
                        }
                    }

                    if (!replayList.isEmpty()) {
                        mpr.addPullRequest(replayList);
                    }
                    break;
                }
            }
        }

    }
}

