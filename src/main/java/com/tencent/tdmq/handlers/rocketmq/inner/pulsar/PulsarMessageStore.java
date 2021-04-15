package com.tencent.tdmq.handlers.rocketmq.inner.pulsar;

import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;

public interface PulsarMessageStore extends MessageStore {

    PutMessageResult putMessage(MessageExtBrokerInner messageExtBrokerInner, RemotingCommand request,
            SendMessageRequestHeader requestHeader);

    GetMessageResult getMessage(RemotingCommand request, PullMessageRequestHeader requestHeader);
}
