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

package org.streamnative.pulsar.handlers.rocketmq.inner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.longpolling.ManyPullRequest;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;

/**
 * Pull request hold service.
 */
@Slf4j
public class PullRequestHoldService extends ServiceThread {

    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    private final RocketMQBrokerController brokerController;
    private final SystemClock systemClock = new SystemClock();

    // key       => topicName@partitionId
    // topicName => tenant/ns/topicName
    private final ConcurrentMap<String, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<>(1024);

    public PullRequestHoldService(final RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendPullRequest(final String topic, final int partitionId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, partitionId);

        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int partitionId) {
        return topic + TOPIC_QUEUEID_SEPARATOR + partitionId;
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getServerConfig().isLongPollingEnable()) {
                    this.waitForRunning(5 * 1000);
                } else {
                    this.waitForRunning(this.brokerController.getServerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int partitionId = Integer.parseInt(kArray[1]);
                try {
                    long offset = this.brokerController.getConsumerOffsetManager()
                            .getMaxOffsetInPartitionId(topic, partitionId);
                    this.notifyMessageArriving(topic, partitionId, offset);
                } catch (RopPersistentTopicException ex) {
                    log.info("unowned-broker topic and remove the hold request.");
                    this.pullRequestTable.remove(key);
                } catch (Throwable th) {
                    log.warn("check hold request failed. topic: {}, partitionId: {} ", topic, partitionId, th);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int partitionId, final long maxOffset) {
        notifyMessageArriving(topic, partitionId, maxOffset, null, 0, null, null);
    }

    public void notifyMessageArriving(final String topic, final int partitionId, final long maxOffset,
            final Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        log.debug("notifyMessageArriving ==========> (topic={} and partitionId={} and maxoffset={})", topic,
                partitionId,
                maxOffset);
        String key = this.buildKey(topic, partitionId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {

                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        try {
                            newestOffset = this.brokerController.getConsumerOffsetManager()
                                    .getMaxOffsetInPartitionId(topic, partitionId);
                        } catch (RopPersistentTopicException e) {
                            log.info("unowned-broker topic and remove the hold request. "
                                    + "remove the request from request-hold-service");
                            continue;
                        }
                    }

                    if (newestOffset > request.getPullFromThisOffset()) {
                        try {
                            this.brokerController.getRopBrokerProxy().getPullMessageProcessor()
                                    .executeRequestWhenWakeup(request.getClientChannel(),
                                            request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getRopBrokerProxy().getPullMessageProcessor()
                                    .executeRequestWhenWakeup(request.getClientChannel(),
                                            request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        continue;
                    }

                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}