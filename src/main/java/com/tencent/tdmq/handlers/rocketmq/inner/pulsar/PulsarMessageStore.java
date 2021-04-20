package com.tencent.tdmq.handlers.rocketmq.inner.pulsar;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;

public interface PulsarMessageStore {

    PutMessageResult putMessage(MessageExtBrokerInner messageExtBrokerInner, String producerGroup);

    GetMessageResult getMessage(RemotingCommand request, PullMessageRequestHeader requestHeader);

    PutMessageResult putMessages(MessageExtBatch batchMessage, String producerGroup);

    MessageExt lookMessageByMessageId(String topic, String msgId);

    /**
     * Reset the subscription associated with this reader to a specific message publish time.
     *
     * @param topic the sub-partitioned topic(is one topic)
     * @param timestamp the message publish time where to reposition the reader
     * @return Return rocketmq MessageExt object
     */
    MessageExt lookMessageByTimestamp(String topic, long timestamp);

    long now();
}
