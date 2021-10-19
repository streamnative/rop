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

import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_META_KEY_GRP_NAME_LEN_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_META_KEY_TOTAL_HEAD_LEN;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_META_KEY_TYPE_POS;
import static org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata.GroupOffsetConstant.GROUP_META_VERSION_POS;

import com.google.common.base.Objects;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import lombok.Data;
import lombok.ToString;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopDecodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;

/**
 * Group meta key.
 *
 * @param <T>
 */
@Data
@ToString
public abstract class GroupMetaKey<T extends GroupMetaKey> implements Deserializer<T> {

    protected short version = 1;
    protected GroupKeyType type;
    protected String groupName;

    public GroupMetaKey() {
    }

    public GroupMetaKey(GroupKeyType type, String groupName) {
        this.type = type;
        this.groupName = groupName;
    }

    /**
     * Group key type.
     */
    public enum GroupKeyType {
        GROUP_SUBSCRIPTION,
        GROUP_OFFSET;

        public static GroupKeyType parseFromOrdinal(int ordinal) {
            return Arrays.stream(GroupKeyType.values()).filter(type -> type.ordinal() == ordinal).findFirst().get();
        }
    }

    protected ByteBuffer encode(ByteBuffer buffer) throws RopEncodeException {
        try {
            buffer.rewind();
            byte[] groupBytes = groupName.getBytes(StandardCharsets.UTF_8);
            buffer.putShort(GROUP_META_VERSION_POS, version);
            buffer.putInt(GROUP_META_KEY_TYPE_POS, type.ordinal());
            buffer.putInt(GROUP_META_KEY_GRP_NAME_LEN_POS, groupBytes.length);
            buffer.position(GROUP_META_KEY_TOTAL_HEAD_LEN);
            buffer.put(groupBytes);
            return buffer;
        } catch (Exception e) {
            throw new RopEncodeException("GroupMetaKey encode error:" + e.getMessage());
        }
    }

    protected static <T extends GroupMetaKey> T decodeKey(ByteBuffer buffer) throws RopDecodeException {
        try {
            int version = buffer.getShort(GROUP_META_VERSION_POS);
            GroupKeyType type = GroupKeyType.parseFromOrdinal(buffer.getInt(GROUP_META_KEY_TYPE_POS));
            int grpNameLen = buffer.getInt(GROUP_META_KEY_GRP_NAME_LEN_POS);
            byte[] grpNameBytes = new byte[grpNameLen];
            buffer.position(GROUP_META_KEY_TOTAL_HEAD_LEN);
            buffer.get(grpNameBytes);
            String groupName = new String(grpNameBytes, StandardCharsets.UTF_8);

            GroupMetaKey metaKey = new GroupOffsetKey();
            metaKey.setVersion((short) version);
            metaKey.setType(type);
            metaKey.setGroupName(groupName);
            metaKey.decode(buffer);
            return (T) metaKey;
        } catch (Exception e) {
            throw new RopDecodeException("GroupMetaKey decodeGroupMeta error:" + e.getMessage());
        }
    }

    @Override
    public int estimateSize() {
        return GROUP_META_KEY_TOTAL_HEAD_LEN + groupName.getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupMetaKey<?> metaKey = (GroupMetaKey<?>) o;
        return Objects.equal(groupName, metaKey.groupName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(groupName);
    }
}
