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

import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_KEY_TOTAL_HEAD_LEN;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopDecodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;
import org.testng.util.Strings;

/**
 * Group offset key.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class GroupOffsetKey extends GroupMetaKey<GroupOffsetKey> {

    private int partition;
    private String topicName;

    public GroupOffsetKey() {
        this.type = GroupKeyType.GROUP_OFFSET;
    }

    @Override
    public ByteBuffer encode() throws RopEncodeException {
        if (Strings.isNullOrEmpty(topicName)) {
            throw new RopEncodeException("GroupOffsetKey subTopic can't be null or empty");
        }

        try {
            byte[] topicBytes = topicName.getBytes(StandardCharsets.UTF_8);
            ByteBuffer byteBuffer = ByteBuffer.allocate(estimateSize());
            super.encode(byteBuffer);
            byteBuffer.putInt(partition);
            byteBuffer.putInt(topicBytes.length);
            byteBuffer.put(topicBytes);
            return byteBuffer;
        } catch (Exception e) {
            throw new RopEncodeException("GroupOffsetKey encode error: " + e.getMessage());
        }
    }

    @Override
    public GroupOffsetKey decode(ByteBuffer buffer) throws RopDecodeException {
        try {
            this.partition = buffer.getInt();
            int topicNameLen = buffer.getInt();
            byte[] topicNameBytes = new byte[topicNameLen];
            buffer.get(topicNameBytes);
            this.topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
            return this;
        } catch (Exception e) {
            throw new RopDecodeException("GroupOffsetKey decode error", e);
        }
    }

    @Override
    public int estimateSize() {
        return super.estimateSize() + GROUP_OFFSET_KEY_TOTAL_HEAD_LEN + topicName
                .getBytes(StandardCharsets.UTF_8).length;
    }
}
