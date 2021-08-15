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

import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_FORMAT_VERSION_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_BROKER_ID_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_BROKER_SELECTED_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_GRP_NAME_LEN_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_PARTITION_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_RETRY_MAX_TIMES_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_RETRY_QUEUE_NUM_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_TAG_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_TOPIC_NAME_LEN_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_TOTAL_HEAD_LEN;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopDecodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;
import org.testng.util.Strings;

@Data
@EqualsAndHashCode
public class GroupOffsetKey implements Deserializer<GroupOffsetKey> {

    private short version = 1;
    private String groupName;
    private String subTopic;
    private int partition;
    private SubscriptionGroupConfig subscriptionGroupConfig;

    @Override
    public ByteBuffer encode() throws RopEncodeException {
        if (Strings.isNullOrEmpty(groupName)) {
            throw new RopEncodeException("GroupOffsetKey groupName can't be null or empty");
        }
        if (Strings.isNullOrEmpty(subTopic)) {
            throw new RopEncodeException("GroupOffsetKey subTopic can't be null or empty");
        }
        if (null == subscriptionGroupConfig) {
            throw new RopEncodeException("subscription information can't be null");
        }
        try {
            byte[] groupBytes = groupName.getBytes(StandardCharsets.UTF_8);
            byte[] topicBytes = subTopic.getBytes(StandardCharsets.UTF_8);
            ByteBuffer byteBuffer = ByteBuffer.allocate(estimateSize());
            byteBuffer.putShort(GROUP_OFFSET_FORMAT_VERSION_POS, version);
            byteBuffer.putShort(GROUP_OFFSET_KEY_TAG_POS, createSubscriptionTag(subscriptionGroupConfig));
            byteBuffer.putInt(GROUP_OFFSET_KEY_PARTITION_POS, partition);
            byteBuffer.putInt(GROUP_OFFSET_KEY_RETRY_QUEUE_NUM_POS, subscriptionGroupConfig.getRetryQueueNums());
            byteBuffer.putInt(GROUP_OFFSET_KEY_RETRY_MAX_TIMES_POS, subscriptionGroupConfig.getRetryMaxTimes());
            byteBuffer.putLong(GROUP_OFFSET_KEY_BROKER_ID_POS, subscriptionGroupConfig.getBrokerId());
            byteBuffer.putLong(GROUP_OFFSET_KEY_BROKER_SELECTED_POS,
                    subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
            byteBuffer.putInt(GROUP_OFFSET_KEY_GRP_NAME_LEN_POS, groupBytes.length);
            byteBuffer.putInt(GROUP_OFFSET_KEY_TOPIC_NAME_LEN_POS, topicBytes.length);
            byteBuffer.position(GROUP_OFFSET_KEY_TOTAL_HEAD_LEN);
            byteBuffer.put(groupBytes);
            byteBuffer.put(topicBytes);
            return byteBuffer;
        } catch (Exception e) {
            throw new RopEncodeException("GroupOffsetKey encode error: " + e.getMessage());
        }
    }

    @Override
    public GroupOffsetKey decode(ByteBuffer buffer) throws RopDecodeException {
        try {
            this.version = buffer.getShort(GROUP_OFFSET_FORMAT_VERSION_POS);
            short tag = buffer.getShort(GROUP_OFFSET_KEY_TAG_POS);
            this.partition = buffer.getInt(GROUP_OFFSET_KEY_PARTITION_POS);

            this.subscriptionGroupConfig =
                    subscriptionGroupConfig == null ? new SubscriptionGroupConfig() : subscriptionGroupConfig;
            this.parseSubscriptionTag(subscriptionGroupConfig, tag);
            this.subscriptionGroupConfig.setRetryQueueNums(buffer.getInt(GROUP_OFFSET_KEY_RETRY_QUEUE_NUM_POS));
            this.subscriptionGroupConfig.setRetryMaxTimes(buffer.getInt(GROUP_OFFSET_KEY_RETRY_MAX_TIMES_POS));
            this.subscriptionGroupConfig.setBrokerId(buffer.getLong(GROUP_OFFSET_KEY_BROKER_ID_POS));
            this.subscriptionGroupConfig
                    .setWhichBrokerWhenConsumeSlowly(buffer.getLong(GROUP_OFFSET_KEY_BROKER_SELECTED_POS));

            int grpNameLen = buffer.getInt(GROUP_OFFSET_KEY_GRP_NAME_LEN_POS);
            int topicNameLen = buffer.getInt(GROUP_OFFSET_KEY_TOPIC_NAME_LEN_POS);
            byte[] grpNameBytes = new byte[grpNameLen];
            byte[] topicNameBytes = new byte[topicNameLen];

            buffer.position(GROUP_OFFSET_KEY_TOTAL_HEAD_LEN);
            buffer.get(grpNameBytes);
            buffer.get(topicNameBytes);
            this.groupName = new String(grpNameBytes, StandardCharsets.UTF_8);
            this.subTopic = new String(topicNameBytes, StandardCharsets.UTF_8);
            this.subscriptionGroupConfig.setGroupName(this.groupName);
            return this;
        } catch (Exception e) {
            throw new RopDecodeException("GroupOffsetKey decode error: " + e.getMessage());
        }
    }

    @Override
    public int estimateSize() {
        return GROUP_OFFSET_KEY_TOTAL_HEAD_LEN + groupName.getBytes(StandardCharsets.UTF_8).length + subTopic
                .getBytes(StandardCharsets.UTF_8).length;
    }

    //consumeEnable  consumeFromMinEnable consumeBroadcastEnable notifyConsumerIdsChangedEnable
    private static short createSubscriptionTag(SubscriptionGroupConfig subscriptionGroupConfig) {
        Preconditions.checkNotNull(subscriptionGroupConfig, "subscription can't be null");
        int tag = 0;
        tag = subscriptionGroupConfig.isConsumeEnable() ? (tag | 0x01) : (tag & 0xFE);
        tag = subscriptionGroupConfig.isConsumeFromMinEnable() ? (tag | 0x02) : (tag & 0xFD);
        tag = subscriptionGroupConfig.isConsumeBroadcastEnable() ? (tag | 0x04) : (tag & 0xFC);
        tag = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable() ? (tag | 0x08) : (tag & 0xFB);
        return (short) tag;
    }

    private static SubscriptionGroupConfig parseSubscriptionTag(SubscriptionGroupConfig subscriptionGroupConfig,
            short tag) {
        Preconditions.checkNotNull(subscriptionGroupConfig, "subscription can't be null");
        subscriptionGroupConfig.setConsumeEnable((tag & 0x01) == 0x01);
        subscriptionGroupConfig.setConsumeFromMinEnable((tag & 0x02) == 0x02);
        subscriptionGroupConfig.setConsumeBroadcastEnable((tag & 0x04) == 0x04);
        subscriptionGroupConfig.setConsumeBroadcastEnable((tag & 0x08) == 0x08);
        return subscriptionGroupConfig;
    }
}
