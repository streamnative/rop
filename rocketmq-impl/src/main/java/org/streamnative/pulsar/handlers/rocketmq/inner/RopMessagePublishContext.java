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

import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Implementation for PublishContext.
 */
@Slf4j
public final class RopMessagePublishContext implements PublishContext {

    private CompletableFuture<PutMessageResult> offsetFuture;
    private Topic topic;
    private int partition;
    private long startTimeNs;
    private long baseOffset = -1L;

    private String msgId;
    private String msgKey;
    private String msgTag;

    @Override
    public void setMetadataFromEntryData(ByteBuf entryData) {
        try {
            final BrokerEntryMetadata brokerEntryMetadata = Commands.peekBrokerEntryMetadataIfExist(entryData);
            if (brokerEntryMetadata == null) {
                throw new IllegalStateException("There's no BrokerEntryData, "
                        + "check if your broker has configured brokerEntryMetadataInterceptors");
            }
            if (!brokerEntryMetadata.hasIndex()) {
                throw new IllegalStateException("The BrokerEntryData has no 'index' field, check if "
                        + "your broker configured AppendIndexMetadataInterceptor");
            }
            baseOffset = brokerEntryMetadata.getIndex();
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to set metadata from entry", e);
        }
    }

    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {
        if (exception != null) {
            log.error("Failed write entry: ledgerId: {}, entryId: {}. triggered send callback.",
                    ledgerId, entryId);
            offsetFuture.completeExceptionally(exception);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Success write topic: {}, ledgerId: {}, entryId: {}"
                                + " And triggered send callback.",
                        topic.getName(), ledgerId, entryId);
            }
            long cost = System.nanoTime() - startTimeNs;
            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.MICROSECONDS);
            if (cost >= 1000000000) {
                log.warn("RoP publishMessage timeout. Cost = [{}ms], topic: [{}]", cost / 1000000, topic.getName());
            }
            // setMetadataFromEntryData() was called before completed() is called so that baseOffset could be set
            if (baseOffset < 0) {
                log.error("Failed to get offset for ({}, {})", ledgerId, entryId);
            }

            offsetFuture.complete(
                    new PutMessageResult(msgId, Joiner.on(":").join(ledgerId, entryId, partition), baseOffset, msgKey,
                            msgTag));
        }

        recycle();
    }

    // recycler
    public static RopMessagePublishContext get(CompletableFuture<PutMessageResult> offsetFuture, Topic topic,
            int partition, long startTimeNs, String msgId, String msgKey, String msgTag) {
        RopMessagePublishContext callback = RECYCLER.get();
        callback.offsetFuture = offsetFuture;
        callback.topic = topic;
        callback.partition = partition;
        callback.startTimeNs = startTimeNs;

        callback.msgId = msgId;
        callback.msgKey = msgKey;
        callback.msgTag = msgTag;
        return callback;
    }

    private final Handle<RopMessagePublishContext> recyclerHandle;

    private RopMessagePublishContext(Handle<RopMessagePublishContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<RopMessagePublishContext> RECYCLER = new Recycler<RopMessagePublishContext>() {
        protected RopMessagePublishContext newObject(
                Handle<RopMessagePublishContext> handle) {
            return new RopMessagePublishContext(handle);
        }
    };

    public void recycle() {
        offsetFuture = null;
        topic = null;
        startTimeNs = -1;
        recyclerHandle.recycle(this);
    }
}
