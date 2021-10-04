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

package org.streamnative.pulsar.handlers.rocketmq.utils;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutableTriple;

/**
 * RoP CommitLogOffset encoder and decoder.
 */
@Slf4j
public class CommitLogOffsetUtils {

    // use 1 bits to describe whether the topic is retry-topic,
    // 11 bits for pulsar real partitionId,
    // 52 bits for message offset.
    public static final int RETRY_TOPIC_TAG_BITS = 1;
    public static final int PULSAR_PARTITION_ID_BITS = 11;
    public static final int OFFSET_BITS = Long.SIZE - (RETRY_TOPIC_TAG_BITS + PULSAR_PARTITION_ID_BITS);

    public static final long setRetryTopicTag(long commitLogOffset, boolean isRetryTopic) {
        return isRetryTopic ? (commitLogOffset | 0x8000000000000000L) : (commitLogOffset & 0x7FFFFFFFFFFFFFFFL);
    }

    public static final boolean isRetryTopic(long commitLogOffset) {
        return (commitLogOffset >>> (PULSAR_PARTITION_ID_BITS + OFFSET_BITS) & 0x01L) == 0x01L;
    }

    public static final long setPartitionId(long commitLogOffset, int partitionId) {
        Preconditions.checkArgument(partitionId >= 0 && partitionId < (1 << PULSAR_PARTITION_ID_BITS),
                "partitionId must be between 0 and 2048.");
        return (((commitLogOffset >>> (Long.SIZE - RETRY_TOPIC_TAG_BITS)) << PULSAR_PARTITION_ID_BITS) | partitionId)
                << OFFSET_BITS;
    }

    public static final int getPartitionId(long commitLogOffset) {
        return (int) ((commitLogOffset >>> OFFSET_BITS) & 0x7FFL);
    }

    public static final long setQueueOffset(long commitLogOffset, long queueOffset) {
        Preconditions.checkArgument(queueOffset >= 0);
        return ((commitLogOffset >>> OFFSET_BITS) << OFFSET_BITS) | queueOffset;
    }

    public static final long getQueueOffset(long commitLogOffset) {
        return commitLogOffset & ((1L << OFFSET_BITS) - 1);
    }

}
