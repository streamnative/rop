package com.tencent.tdmq.handlers.rocketmq.inner.listener;

import com.tencent.tdmq.handlers.rocketmq.inner.PullRequestHoldService;
import java.util.Map;
import org.apache.rocketmq.store.MessageArrivingListener;

public class NotifyMessageArrivingListener implements MessageArrivingListener {

    private final PullRequestHoldService pullRequestHoldService;

    public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService) {
        this.pullRequestHoldService = pullRequestHoldService;
    }

    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
            long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode,
                msgStoreTime, filterBitMap, properties);
    }
}
