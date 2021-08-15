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

package org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Date;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopDecodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;

/**
 * Test GroupOffsetValueTest Class.
 */
public class GroupOffsetKeyTest {
    private GroupOffsetKey groupOffsetKey;

    @Before
    public void init() {
        groupOffsetKey = new GroupOffsetKey();
        groupOffsetKey.setVersion((short) 2);
        groupOffsetKey.setGroupName("my_first_group");
        groupOffsetKey.setSubTopic("my_first_topic");
        groupOffsetKey.setPartition(10);

        SubscriptionGroupConfig subscription = new SubscriptionGroupConfig();
        subscription.setGroupName("my_first_group");
        subscription.setRetryMaxTimes(10);
        subscription.setRetryQueueNums(2);
        subscription.setBrokerId(1);
        subscription.setConsumeEnable(true);
        subscription.setConsumeFromMinEnable(true);
        subscription.setNotifyConsumerIdsChangedEnable(true);
        subscription.setConsumeBroadcastEnable(true);
        groupOffsetKey.setSubscriptionGroupConfig(subscription);
    }

    @Test
    public void encodeDecodeTest() throws RopEncodeException, RopDecodeException {
        ByteBuffer buffer = groupOffsetKey.encode();
        buffer.rewind();
        GroupOffsetKey temp = new GroupOffsetKey();
        temp.decode(buffer);
        assertEquals(groupOffsetKey, temp);
    }
}
