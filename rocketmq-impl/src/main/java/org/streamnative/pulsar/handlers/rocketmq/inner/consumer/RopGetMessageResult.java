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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.store.GetMessageStatus;

/**
 * Rop get message result.
 */
@Data
@ToString(exclude = "messageBufferList")
@Slf4j
public class RopGetMessageResult {

    private static final AtomicLong addMsgCount = new AtomicLong();
    private static final AtomicLong delMsgCount = new AtomicLong();

    static {
        Thread t = new Thread(() -> {
            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(10000);
                } catch (Exception e) {
                    return;
                }
                log.info("Show rop message unRelease count: {}, addMsgCount: {}, delMsgCount: {}.",
                        addMsgCount.get() - delMsgCount.get(), addMsgCount.get(), delMsgCount.get());
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private final List<ByteBuf> messageBufferList = new ArrayList<>(100);
    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private boolean suggestPullingFromSlave = false;
    private int msgCount4Commercial = 0;

    public int getMessageCount() {
        return messageBufferList.size();
    }

    public int getBufferTotalSize() {
        return messageBufferList.stream().reduce(0, (r, item) ->
                        r += item.readableBytes()
                , Integer::sum);
    }

    public void addMessage(final ByteBuf msgBuf) {
        messageBufferList.add(msgBuf);
        addMsgCount.incrementAndGet();
    }

    public int size() {
        return messageBufferList.size();
    }

    public void release() {
        for (ByteBuf msgByteBuf : this.messageBufferList) {
            try {
                msgByteBuf.release();
                delMsgCount.incrementAndGet();
            } catch (Exception e) {
                log.warn("RoP release get message failed.", e);
            }
        }
    }

}
