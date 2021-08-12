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

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * Implementation for PublishContext.
 */
@Slf4j
public final class RopMessagePublishContext implements PublishContext {

    private CompletableFuture<MessageId> messageIdFuture;
    private Topic topic;
    private long startTimeNs;
    private long partitionId;

    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {

        if (exception != null) {
            log.error("Failed write entry: ledgerId: {}, entryId: {}. triggered send callback.",
                    ledgerId, entryId);
            messageIdFuture.completeExceptionally(exception);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Success write topic: {}, ledgerId: {}, entryId: {}"
                                + " And triggered send callback.",
                        topic.getName(), ledgerId, entryId);
            }

            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.MICROSECONDS);

            messageIdFuture.complete(new MessageIdImpl(ledgerId, entryId, (int) partitionId));
        }

        recycle();
    }

    // recycler
    public static RopMessagePublishContext get(CompletableFuture<MessageId> messageIdFuture,
            Topic topic,
            long startTimeNs,
            long partitionId) {
        RopMessagePublishContext callback = RECYCLER.get();
        callback.messageIdFuture = messageIdFuture;
        callback.topic = topic;
        callback.startTimeNs = startTimeNs;
        callback.partitionId = partitionId;
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
        messageIdFuture = null;
        topic = null;
        startTimeNs = -1;
        recyclerHandle.recycle(this);
    }
}
