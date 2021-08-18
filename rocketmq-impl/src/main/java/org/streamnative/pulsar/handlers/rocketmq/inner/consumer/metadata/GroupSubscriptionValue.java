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

import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_SUBSCRIPTION_FORMAT_VERSION_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_SUBSCRIPTION_KEY_BROKER_ID_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_SUBSCRIPTION_KEY_BROKER_SELECTED_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_SUBSCRIPTION_KEY_GRP_NAME_LEN_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_SUBSCRIPTION_KEY_RETRY_MAX_TIMES_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_SUBSCRIPTION_KEY_RETRY_QUEUE_NUM_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_SUBSCRIPTION_KEY_TAG_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_SUBSCRIPTION_KEY_TOTAL_HEAD_LEN;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopDecodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;

@Data
@EqualsAndHashCode(callSuper = true)
public class GroupSubscriptionValue extends SubscriptionGroupConfig implements Deserializer<GroupSubscriptionValue> {

    private short version = 1;

    @Override
    public ByteBuffer encode() throws RopEncodeException {
        try {
            byte[] groupBytes = this.getGroupName().getBytes(StandardCharsets.UTF_8);
            ByteBuffer byteBuffer = ByteBuffer.allocate(estimateSize());
            byteBuffer.putShort(GROUP_SUBSCRIPTION_FORMAT_VERSION_POS, version);
            byteBuffer.putShort(GROUP_SUBSCRIPTION_KEY_TAG_POS, createSubscriptionTag());
            byteBuffer.putInt(GROUP_SUBSCRIPTION_KEY_RETRY_QUEUE_NUM_POS, this.getRetryQueueNums());
            byteBuffer.putInt(GROUP_SUBSCRIPTION_KEY_RETRY_MAX_TIMES_POS, this.getRetryMaxTimes());
            byteBuffer.putLong(GROUP_SUBSCRIPTION_KEY_BROKER_ID_POS, this.getBrokerId());
            byteBuffer.putLong(GROUP_SUBSCRIPTION_KEY_BROKER_SELECTED_POS,
                    this.getWhichBrokerWhenConsumeSlowly());
            byteBuffer.putInt(GROUP_SUBSCRIPTION_KEY_GRP_NAME_LEN_POS, groupBytes.length);
            byteBuffer.position(GROUP_SUBSCRIPTION_KEY_TOTAL_HEAD_LEN);
            byteBuffer.put(groupBytes);
            return byteBuffer;
        } catch (Exception e) {
            throw new RopEncodeException("GroupSubscriptionValue encode error: " + e.getMessage());
        }
    }

    @Override
    public GroupSubscriptionValue decode(ByteBuffer buffer) throws RopDecodeException {
        try {
            this.version = buffer.getShort(GROUP_SUBSCRIPTION_FORMAT_VERSION_POS);
            short tag = buffer.getShort(GROUP_SUBSCRIPTION_KEY_TAG_POS);
            this.parseSubscriptionTag(tag);

            this.setRetryQueueNums(buffer.getInt(GROUP_SUBSCRIPTION_KEY_RETRY_QUEUE_NUM_POS));
            this.setRetryMaxTimes(buffer.getInt(GROUP_SUBSCRIPTION_KEY_RETRY_MAX_TIMES_POS));
            this.setBrokerId(buffer.getLong(GROUP_SUBSCRIPTION_KEY_BROKER_ID_POS));
            this.setWhichBrokerWhenConsumeSlowly(buffer.getLong(GROUP_SUBSCRIPTION_KEY_BROKER_SELECTED_POS));

            int grpNameLen = buffer.getInt(GROUP_SUBSCRIPTION_KEY_GRP_NAME_LEN_POS);
            byte[] grpNameBytes = new byte[grpNameLen];

            buffer.position(GROUP_SUBSCRIPTION_KEY_TOTAL_HEAD_LEN);
            buffer.get(grpNameBytes);
            this.setGroupName(new String(grpNameBytes, StandardCharsets.UTF_8));
            return this;
        } catch (Exception e) {
            throw new RopDecodeException("GroupSubscriptionValue decode error: " + e.getMessage());
        }
    }

    @Override
    public int estimateSize() {
        return GROUP_SUBSCRIPTION_KEY_TOTAL_HEAD_LEN + this.getGroupName().getBytes(StandardCharsets.UTF_8).length;
    }

    //consumeEnable  consumeFromMinEnable consumeBroadcastEnable notifyConsumerIdsChangedEnable
    private short createSubscriptionTag() {
        int tag = 0;
        tag = this.isConsumeEnable() ? (tag | 0x01) : (tag & 0xFE);
        tag = this.isConsumeFromMinEnable() ? (tag | 0x02) : (tag & 0xFD);
        tag = this.isConsumeBroadcastEnable() ? (tag | 0x04) : (tag & 0xFC);
        tag = this.isNotifyConsumerIdsChangedEnable() ? (tag | 0x08) : (tag & 0xFB);
        return (short) tag;
    }

    private SubscriptionGroupConfig parseSubscriptionTag(short tag) {
        this.setConsumeEnable((tag & 0x01) == 0x01);
        this.setConsumeFromMinEnable((tag & 0x02) == 0x02);
        this.setConsumeBroadcastEnable((tag & 0x04) == 0x04);
        this.setConsumeBroadcastEnable((tag & 0x08) == 0x08);
        return this;
    }
}
