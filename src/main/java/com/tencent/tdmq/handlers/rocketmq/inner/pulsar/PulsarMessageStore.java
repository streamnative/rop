/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tdmq.handlers.rocketmq.inner.pulsar;

import com.tencent.tdmq.handlers.rocketmq.inner.consumer.RopGetMessageResult;
import com.tencent.tdmq.handlers.rocketmq.inner.format.RopMessageFilter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;

public interface PulsarMessageStore {

    PutMessageResult putMessage(MessageExtBrokerInner messageExtBrokerInner, String producerGroup);

    RopGetMessageResult getMessage(RemotingCommand request, PullMessageRequestHeader requestHeader,
            RopMessageFilter messageFilter);

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
