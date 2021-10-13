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
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * write channel using non-heap buffer.
 */
public class BatchMessageTransfer extends AbstractReferenceCounted implements FileRegion, Closeable {

    private final ByteBuffer byteBufferHeader;
    private final RopGetMessageResult getMessageResult;
    private long transferred;

    public BatchMessageTransfer(ByteBuffer byteBufferHeader,
            RopGetMessageResult getMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.getMessageResult = getMessageResult;
    }

    @Override
    public long position() {
        int pos = byteBufferHeader.position();
        List<ByteBuf> messageBufferList = this.getMessageResult.getMessageBufferList();
        for (ByteBuf bb : messageBufferList) {
            pos += bb.writerIndex();
        }
        return pos;
    }

    @Override
    public long transfered() {
        return transferred();
    }

    @Override
    public long transferred() {
        return transferred;
    }

    @Override
    public long count() {
        return byteBufferHeader.limit() + this.getMessageResult.getBufferTotalSize();
    }

    @Override
    public long transferTo(WritableByteChannel channel, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += channel.write(this.byteBufferHeader);
            return transferred;
        } else {
            List<ByteBuf> messageBufferList = this.getMessageResult.getMessageBufferList();
            for (ByteBuf bb : messageBufferList) {
                if (bb.readableBytes() > 0) {
                    transferred += channel.write(bb.nioBuffer());
                    return transferred;
                }
            }
        }

        return 0;
    }

    @Override
    protected void deallocate() {
        getMessageResult.release();
    }

    @Override
    public void close() throws IOException {
        this.deallocate();
    }

    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        return this;
    }
}
