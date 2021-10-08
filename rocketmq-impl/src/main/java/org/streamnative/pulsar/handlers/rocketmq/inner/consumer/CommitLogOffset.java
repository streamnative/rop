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

package org.streamnative.pulsar.handlers.rocketmq.inner.consumer;

import com.google.common.base.Preconditions;
import lombok.Getter;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommitLogOffsetUtils;

/**
 * commit log offset is used to lookup message in retry logic.
 * it's made up by 3 components, isRetryTopic + partitionId + offset
 */
@Getter
public final class CommitLogOffset {

    private final boolean isRetryTopic;
    private final int partitionId;
    private final long queueOffset;

    public CommitLogOffset(boolean isRetryTopic, int partitionId, long queueOffset) {
        Preconditions
                .checkArgument(partitionId >= 0 && queueOffset >= 0, "partition id and queue offset must be >= 0.");
        this.isRetryTopic = isRetryTopic;
        this.partitionId = partitionId;
        this.queueOffset = queueOffset;
    }

    public CommitLogOffset(long commitLogOffset) {
        this.isRetryTopic = CommitLogOffsetUtils.isRetryTopic(commitLogOffset);
        this.partitionId = CommitLogOffsetUtils.getPartitionId(commitLogOffset);
        this.queueOffset = CommitLogOffsetUtils.getQueueOffset(commitLogOffset);
    }

    public long getCommitLogOffset() {
        long commitLogOffset = CommitLogOffsetUtils.setRetryTopicTag(queueOffset, isRetryTopic);
        commitLogOffset = CommitLogOffsetUtils.setPartitionId(commitLogOffset, partitionId);
        return commitLogOffset;
    }
}
