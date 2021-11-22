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

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.ToString;
import org.apache.rocketmq.store.GetMessageStatus;
import org.streamnative.pulsar.handlers.rocketmq.inner.RopMessage;

/**
 * Rop get message result.
 */
@Data
@ToString(exclude = "messageBufferList")
public class RopGetMessageResult {

    private final List<RopMessage> messageBufferList = new ArrayList<>(100);
    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private boolean suggestPullingFromSlave = false;
    private int msgCount4Commercial = 0;

    private String pulsarTopic;
    private int partitionId;
    private String instanceName;

    public int getMessageCount() {
        return messageBufferList.size();
    }

    public int getBufferTotalSize() {
        return messageBufferList.stream().reduce(0, (r, item) ->
                        r += item.getPayload().readableBytes()
                , Integer::sum);
    }

    public void addMessage(final RopMessage ropMessage) {
        messageBufferList.add(ropMessage);
    }

    public int size(){
        return messageBufferList.size();
    }

    public void release() {
        for (RopMessage ropMessage : this.messageBufferList) {
            ropMessage.getPayload().release();
        }
    }

}
