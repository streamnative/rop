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

import java.nio.ByteBuffer;
import java.util.List;
import lombok.Data;
import org.apache.rocketmq.store.GetMessageStatus;

/**
 * Rop get message result.
 */
@Data
public class RopGetMessageResult {

    private List<ByteBuffer> messageBufferList;
    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private int bufferTotalSize = 0;
    private boolean suggestPullingFromSlave = false;
    private int msgCount4Commercial = 0;

    public int getMessageCount() {
        if (messageBufferList != null) {
            return messageBufferList.size();
        }
        return 0;
    }

    public int getBufferTotalSize() {
        if (messageBufferList != null) {
            return messageBufferList.stream().reduce(0, (r, item) ->
                            r += item.limit()
                    , Integer::sum);
        }
        return 0;
    }

}
