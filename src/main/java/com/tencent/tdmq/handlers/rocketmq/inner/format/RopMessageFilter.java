package com.tencent.tdmq.handlers.rocketmq.inner.format;

import java.nio.ByteBuffer;
import java.util.function.Predicate;
import org.apache.pulsar.client.api.Message;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class RopMessageFilter implements Predicate<Message> {

    protected final SubscriptionData subscriptionData;

    public RopMessageFilter(SubscriptionData subscriptionData) {
        this.subscriptionData = subscriptionData;
    }

    @Override
    public boolean test(Message message) {
        if (this.subscriptionData != null && message != null
                && ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }

            byte[] body = message.getData();
            Long tagsCode = null;
            if (body != null && body.length >= 4) {
                tagsCode = ByteBuffer.wrap(body).getLong();
            }
            return (tagsCode != null) && subscriptionData.getCodeSet().contains(tagsCode);
        }
        return true;
    }
}
