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

import java.nio.ByteBuffer;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopDecodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;

@Data
@EqualsAndHashCode
public class GroupSubscriptionKey extends GroupMetaKey<GroupSubscriptionKey> {
    public GroupSubscriptionKey() {
        this.type = GroupKeyType.GROUP_SUBSCRIPTION;
    }

    @Override
    public ByteBuffer encode() throws RopEncodeException {
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocate(estimateSize());
            return super.encode(byteBuffer);
        } catch (Exception e) {
            throw new RopEncodeException("GroupOffsetKey encode error: " + e.getMessage());
        }
    }

    @Override
    public GroupSubscriptionKey decode(ByteBuffer buffer) throws RopDecodeException {
        return this;
    }

    @Override
    public int estimateSize() {
        return super.estimateSize();
    }
}
