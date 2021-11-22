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

package org.streamnative.pulsar.handlers.rocketmq.inner;

import io.netty.buffer.ByteBuf;
import lombok.Data;

@Data
public class RopMessage {

    private String msgId;
    private int partitionId;
    private long offset;
    private ByteBuf payload;

    private String msgKey;
    private String msgTag;
    private byte[] body;

    public RopMessage() {
    }

    public RopMessage(String msgId, int partitionId, long offset, ByteBuf payload) {
        this.msgId = msgId;
        this.partitionId = partitionId;
        this.offset = offset;
        this.payload = payload;
    }

    public RopMessage(String msgId, String msgKey, String msgTag, byte[] body) {
        this.msgId = msgId;
        this.msgKey = msgKey;
        this.msgTag = msgTag;
        this.body = body;
    }

}
