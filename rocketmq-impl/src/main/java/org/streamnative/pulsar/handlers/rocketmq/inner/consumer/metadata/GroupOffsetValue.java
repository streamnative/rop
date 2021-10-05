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

import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_VALUE_COMMIT_TIMESTAMP_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_VALUE_EXPIRE_TIMESTAMP_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_VALUE_OFFSET_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_VALUE_TOTAL_LEN;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_OFFSET_VALUE_VERSION_POS;

import java.nio.ByteBuffer;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopDecodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;

/**
 * Group offset value.
 */
@Data
@ToString
@EqualsAndHashCode
public class GroupOffsetValue implements Deserializer<GroupOffsetValue> {

    private short version = 1;
    private long offset;
    private long commitTimestamp;
    private long expireTimestamp;
    transient private volatile boolean isUpdated = true;

    public GroupOffsetValue() {
    }

    public GroupOffsetValue(long offset, long commitTimestamp, long expireTimestamp) {
        this.offset = offset;
        this.commitTimestamp = commitTimestamp;
        this.expireTimestamp = expireTimestamp;
    }

    @Override
    public ByteBuffer encode() throws RopEncodeException {
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocate(estimateSize());
            byteBuffer.putShort(version);
            byteBuffer.putLong(offset);
            byteBuffer.putLong(commitTimestamp);
            byteBuffer.putLong(expireTimestamp);
            return byteBuffer;
        } catch (Exception e) {
            throw new RopEncodeException("GroupOffsetValue encode error: " + e.getMessage());
        }
    }

    @Override
    public GroupOffsetValue decode(ByteBuffer buffer) throws RopDecodeException {
        try {
            this.version = buffer.getShort(GROUP_OFFSET_VALUE_VERSION_POS);
            this.offset = buffer.getLong(GROUP_OFFSET_VALUE_OFFSET_POS);
            this.commitTimestamp = buffer.getLong(GROUP_OFFSET_VALUE_COMMIT_TIMESTAMP_POS);
            this.expireTimestamp = buffer.getLong(GROUP_OFFSET_VALUE_EXPIRE_TIMESTAMP_POS);
            return this;
        } catch (Exception e) {
            throw new RopDecodeException("GroupOffsetValue decode error: " + e.getMessage());
        }
    }

    @Override
    public int estimateSize() {
        return GROUP_OFFSET_VALUE_TOTAL_LEN;
    }

    public static GroupOffsetValue decodeGroupOffset(ByteBuffer buffer) throws RopDecodeException {
        GroupOffsetValue offsetValue = new GroupOffsetValue();
        offsetValue.decode(buffer);
        return offsetValue;
    }

    public void refresh(long offset, long commitTimestamp, long expireTimestamp) {
        if (this.offset != offset) {
            this.offset = offset;
            this.commitTimestamp = commitTimestamp;
        }
        this.expireTimestamp = expireTimestamp;
        this.isUpdated = true;
    }
}
