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

package org.streamnative.pulsar.handlers.rocketmq.inner.pulsar;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageCallback;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.RopGetMessageResult;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopMessageFilter;

/**
 * Pulsar message store interface.
 */
public interface PulsarMessageStore {

    void putMessage(MessageExtBrokerInner messageExtBrokerInner, String producerGroup, PutMessageCallback callback)
            throws Exception;

    RopGetMessageResult getMessage(RemotingCommand request, PullMessageRequestHeader requestHeader,
            RopMessageFilter messageFilter);

    void putMessages(MessageExtBatch batchMessage, String producerGroup, PutMessageCallback callback) throws Exception;

    MessageExt lookMessageByMessageId(String topic, String msgId);

    MessageExt lookMessageByMessageId(String topic, long offset);

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
