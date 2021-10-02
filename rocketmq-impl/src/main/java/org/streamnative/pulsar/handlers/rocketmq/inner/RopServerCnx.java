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

import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.SLASH_CHAR;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQProtocolHandler;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.RopGetMessageResult;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopEntryFormatter;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopMessageFilter;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.pulsar.PulsarMessageStore;
import org.streamnative.pulsar.handlers.rocketmq.inner.request.PullRequestFilterKey;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Rop server cnx.
 */
@Slf4j
@Getter
public class RopServerCnx extends ChannelInboundHandlerAdapter implements PulsarMessageStore {

    private static final int sendTimeoutInSec = 30;
    private static final int maxPendingMessages = 1000;
    private static final int fetchTimeoutInMs = 3000; // 3 sec
    private static final String ropHandlerName = "RopServerCnxHandler";
    private final BrokerService service;
    private final ConcurrentLongHashMap<Producer<byte[]>> producers;
    private final ConcurrentLongHashMap<MessageIdImpl> nextBeginOffsets;
    private final ConcurrentHashMap<String, ManagedCursor> cursors;
    private final HashMap<Long, Reader<byte[]>> lookMsgReaders;
    private final RopEntryFormatter entryFormatter = new RopEntryFormatter();
    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock lookMsgLock = new ReentrantLock();
    private final SystemClock systemClock = new SystemClock();
    private final RocketMQBrokerController brokerController;
    private final ChannelHandlerContext ctx;
    private final SocketAddress remoteAddress;
    private final int localListenPort;
    private final Cache<PullRequestFilterKey, Object> requestFilterCache = CacheBuilder
            .newBuilder()
            .initialCapacity(1024)
            .maximumSize(4096)
            .build();
    private final Object pullRequestFilterValue = new Object();
    private State state;
    private final String cursorName = "rop-cursor";

    public RopServerCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        this.brokerController = brokerController;
        this.localListenPort =
                RocketMQProtocolHandler.getListenerPort(brokerController.getServerConfig().getRocketmqListeners());
        this.service = brokerController.getBrokerService();
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        this.state = State.Connected;
        this.producers = new ConcurrentLongHashMap<>(2, 1);
        this.nextBeginOffsets = new ConcurrentLongHashMap<>(2, 1);
        this.lookMsgReaders = new HashMap<>();
        this.cursors = new ConcurrentHashMap<>(4);
        synchronized (this.ctx) {
            if (ctx.pipeline().get(ropHandlerName) == null) {
                ctx.pipeline().addLast(ropHandlerName, this);
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("Closed connection from {}", remoteAddress);
        // Connection is gone, close the resources immediately
        producers.values().forEach(Producer::closeAsync);
        cursors.forEach((s, managedCursor) -> closeCursor(s));
        producers.clear();
        nextBeginOffsets.clear();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (log.isDebugEnabled()) {
            log.debug("Channel writable has changed to: {}", ctx.channel().isWritable());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (this.state != State.Failed) {
            log.warn("[{}] Got exception {}", this.remoteAddress,
                    ClientCnx.isKnownException(cause) ? cause : ExceptionUtils.getStackTrace(cause));
            this.state = State.Failed;
        } else if (log.isDebugEnabled()) {
            log.debug("[{}] Got exception: {}", this.remoteAddress, cause);
        }
        this.ctx.close();
    }

    @Override
    public void putMessage(int partitionId, MessageExtBrokerInner messageInner, String producerGroup,
            PutMessageCallback callback)
            throws Exception {
        Preconditions.checkNotNull(messageInner);
        Preconditions.checkNotNull(producerGroup);
        RocketMQTopic rmqTopic = new RocketMQTopic(messageInner.getTopic());
        // pTopic = persistent://rocketmq/default/topicTest2-partition-1
        String pTopic = rmqTopic.getPartitionName(partitionId);
        long deliverAtTime = getDeliverAtTime(messageInner);
        int queueId = messageInner.getQueueId();

        if (deliverAtTime - System.currentTimeMillis() > brokerController.getServerConfig().getRopMaxDelayTime()) {
            throw new RuntimeException("DELAY TIME IS TOO LONG");
        }

        final int tranType = MessageSysFlag.getTransactionValue(messageInner.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            if (isNotDelayMessage(deliverAtTime) && messageInner.getDelayTimeLevel() > 0 && !rmqTopic.isDLQTopic()) {
                if (messageInner.getDelayTimeLevel() > this.brokerController.getServerConfig().getMaxDelayLevelNum()) {
                    messageInner.setDelayTimeLevel(this.brokerController.getServerConfig().getMaxDelayLevelNum());
                }

                int totalQueueNum = this.brokerController.getServerConfig().getRmqScheduleTopicPartitionNum();
                queueId = queueId % totalQueueNum;
                pTopic = this.brokerController.getDelayedMessageService()
                        .getDelayedTopicName(messageInner.getDelayTimeLevel(), partitionId);

                MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_TOPIC, messageInner.getTopic());
                MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
                        String.valueOf(messageInner.getQueueId()));
                messageInner.setPropertiesString(MessageDecoder.messageProperties2String(messageInner.getProperties()));
                messageInner.setTopic(pTopic);
                messageInner.setQueueId(queueId);

            }
        }

        try {
            List<byte[]> body = this.entryFormatter.encode(messageInner, 1);
            CompletableFuture<Long> offsetFuture = null;

            /*
             * Optimize the production performance of publish messages.
             * If the broker is the owner of the current partitioned topic, directly use the PersistentTopic interface
             * for publish message.
             */
            if (isNotDelayMessage(deliverAtTime) && this.brokerController.getTopicConfigManager()
                    .isPartitionTopicOwner(rmqTopic.getPulsarTopicName(), partitionId)) {
                PersistentTopic persistentTopic = this.brokerController.getTopicConfigManager()
                        .getPulsarPersistentTopic(pTopic);
                // if persistentTopic is null, throw SYSTEM_ERROR to rop proxy and retry send request.
                if (persistentTopic != null) {
                    offsetFuture = publishMessage(body.get(0), persistentTopic, pTopic);
                } else {
                    AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                    PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, temp);
                    callback.callback(putMessageResult);
                }
            }

            /*
             * Use pulsar producer send delay message.
             */
            long producerId = buildPulsarProducerId(producerGroup, pTopic,
                    ctx.channel().remoteAddress().toString());
            Producer<byte[]> producer = this.producers.get(producerId);
            if (producer == null) {
                synchronized (this.producers) {
                    log.info("[{}] PutMessage creating producer[id={}] and channel=[{}].",
                            rmqTopic.getPulsarFullName(), producerId, ctx.channel());
                    if (this.producers.get(producerId) == null) {
                        producer = createNewProducer(pTopic, producerGroup, producerId);
                        Producer<byte[]> oldProducer = this.producers.put(producerId, producer);
                        if (oldProducer != null) {
                            oldProducer.closeAsync();
                        }
                    }
                }
            }
            // TODO: handle delay message
//            if (!isNotDelayMessage(deliverAtTime)) {
//                messageIdFuture = this.producers.get(producerId).newMessage()
//                        .value((body.get(0)))
//                        .deliverAt(deliverAtTime)
//                        .sendAsync();
//            }

            /*
             * Handle future async by brokerController.getSendCallbackExecutor().
             */
            Preconditions.checkNotNull(offsetFuture);
            offsetFuture.whenCompleteAsync((offset, t) -> {
                if (t != null) {
                    log.warn("[{}] PutMessage error.", rmqTopic.getPulsarFullName(), t);

                    PutMessageStatus status = PutMessageStatus.FLUSH_DISK_TIMEOUT;
                    AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                    PutMessageResult putMessageResult = new PutMessageResult(status, temp);
                    callback.callback(putMessageResult);
                    return;
                }

                AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
                appendMessageResult.setMsgNum(1);
                appendMessageResult.setWroteBytes(body.get(0).length);
                appendMessageResult.setMsgId(
                        CommonUtils.createMessageId(this.ctx.channel().localAddress(), localListenPort, offset));
                appendMessageResult.setLogicsOffset(offset);
                appendMessageResult.setWroteOffset(offset);
                PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);

                callback.callback(putMessageResult);

                brokerController.getMessageArrivingListener()
                        .arriving(messageInner.getTopic(), messageInner.getQueueId(), offset, 0, 0,
                                null, null);
            }, brokerController.getSendCallbackExecutor());

        } catch (RopEncodeException e) {
            log.warn("PutMessage encode error.", e);
            throw e;
        } catch (Exception e) {
            log.warn("PutMessage error.", e);
            throw e;
        }
    }

    @Override
    public void putMessages(int realPartitionID, MessageExtBatch batchMessage, String producerGroup,
            PutMessageCallback callback)
            throws Exception {

        Preconditions.checkNotNull(batchMessage);
        Preconditions.checkNotNull(producerGroup);
        RocketMQTopic rmqTopic = new RocketMQTopic(batchMessage.getTopic());
        String pTopic = rmqTopic.getPartitionName(realPartitionID);
        int queueId = batchMessage.getQueueId();

        try {
            StringBuilder sb = new StringBuilder();
            int totalBytesSize = 0;
            int messageNum = 0;

            List<CompletableFuture<Long>> batchMessageFutures = new ArrayList<>();
            List<byte[]> bodies = this.entryFormatter.encode(batchMessage, 1);

            /*
             * Optimize the production performance of batch publish messages.
             * If the broker is the owner of the current partitioned topic, directly use the PersistentTopic interface
             * for publish message.
             */
            if (this.brokerController.getTopicConfigManager()
                    .isPartitionTopicOwner(rmqTopic.getPulsarTopicName(), realPartitionID)) {
                PersistentTopic persistentTopic = this.brokerController.getTopicConfigManager()
                        .getPulsarPersistentTopic(pTopic);
                if (persistentTopic != null) {
                    for (byte[] body : bodies) {
                        CompletableFuture<Long> offsetFuture = publishMessage(body, persistentTopic, pTopic);
                        batchMessageFutures.add(offsetFuture);
                        messageNum++;
                        totalBytesSize += body.length;
                    }
                }
            }

            /*
             * Use pulsar producer send message.
             * We uniformly use a single message to send messages, so as to avoid the problem of batch message parsing.
             */
            if (batchMessageFutures.isEmpty()) {
                long producerId = buildPulsarProducerId(producerGroup, pTopic, this.remoteAddress.toString());
                Producer<byte[]> putMsgProducer = this.producers.get(producerId);
                if (putMsgProducer == null) {
                    synchronized (this.producers) {
                        if (this.producers.get(producerId) == null) {
                            log.info("[{}] putMessages creating producer[id={}].", pTopic, producerId);
                            putMsgProducer = createNewProducer(pTopic, producerGroup, producerId);
                            Producer<byte[]> oldProducer = this.producers.put(producerId, putMsgProducer);
                            if (oldProducer != null) {
                                oldProducer.closeAsync();
                            }
                        }
                    }
                }

                log.info("The producer [{}] putMessages begin to send message.", producerId);
            }

            /*
             * Handle future list async by brokerController.getSendCallbackExecutor().
             */
            final int messageNumFinal = messageNum;
            final int totalBytesSizeFinal = totalBytesSize;
            FutureUtil.waitForAll(batchMessageFutures).whenCompleteAsync((aVoid, throwable) -> {
                if (throwable != null) {
                    PutMessageStatus status = PutMessageStatus.FLUSH_DISK_TIMEOUT;
                    AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                    PutMessageResult result = new PutMessageResult(status, temp);
                    callback.callback(result);
                    return;
                }

                for (CompletableFuture<Long> f : batchMessageFutures) {
                    Long offset = f.getNow(null);
                    if (offset == null) {
                        PutMessageStatus status = PutMessageStatus.FLUSH_DISK_TIMEOUT;
                        AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                        PutMessageResult result = new PutMessageResult(status, temp);
                        callback.callback(result);
                        return;
                    }
                    String msgId = CommonUtils.createMessageId(ctx.channel().localAddress(), localListenPort, offset);
                    sb.append(msgId).append(",");
                }

                AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
                appendMessageResult.setMsgNum(messageNumFinal);
                appendMessageResult.setWroteBytes(totalBytesSizeFinal);
                appendMessageResult.setMsgId(sb.toString());

                PutMessageResult result = new PutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);
                callback.callback(result);

                brokerController.getMessageArrivingListener()
                        .arriving(batchMessage.getTopic(), queueId, Long.MAX_VALUE, 0, 0,
                                null, null);
            }, brokerController.getSendCallbackExecutor());

        } catch (RopEncodeException e) {
            log.warn("putMessages batchMessage encode error.", e);
            throw e;
        } catch (PulsarServerException e) {
            log.warn("putMessages batchMessage send error.", e);
            throw e;
        } catch (Exception e) {
            log.warn("putMessages batchMessage error.", e);
            throw e;
        }
    }

    private Producer<byte[]> createNewProducer(String pTopic, String producerGroup, long producerId)
            throws PulsarServerException, PulsarClientException {
        return this.service.pulsar().getClient().newProducer()
                .topic(pTopic)
                .maxPendingMessages(maxPendingMessages)
                .producerName(producerGroup + CommonUtils.UNDERSCORE_CHAR + producerId)
                .sendTimeout(sendTimeoutInSec, TimeUnit.MILLISECONDS)
                .enableBatching(false)
                .blockIfQueueFull(false)
                .create();
    }

    private CompletableFuture<Long> publishMessage(byte[] body, PersistentTopic persistentTopic, String pTopic) {
        ByteBuf headersAndPayload = null;
        try {
            headersAndPayload = this.entryFormatter.encode(body);

            // collect metrics
            org.apache.pulsar.broker.service.Producer producer = this.brokerController.getTopicConfigManager()
                    .getReferenceProducer(pTopic, persistentTopic, this);
            if (producer != null) {
                producer.updateRates(1, headersAndPayload.readableBytes());
                producer.getTopic().incrementPublishCount(1, headersAndPayload.readableBytes());
            }

            // publish message
            CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
            persistentTopic.publishMessage(headersAndPayload, RopMessagePublishContext
                    .get(offsetFuture, persistentTopic, System.nanoTime()));

            return offsetFuture;
        } finally {
            if (headersAndPayload != null) {
                headersAndPayload.release();
            }
        }
    }

    @Override
    public MessageExt lookMessageByMessageId(String partitionedTopic, String msgId) {
        return null;
    }

    @Override
    public MessageExt lookMessageByMessageId(String originTopic, long offset) {
        Preconditions.checkNotNull(originTopic, "topic mustn't be null");
        MessageIdImpl messageId = MessageIdUtils.getMessageId(offset);

        RocketMQTopic rocketMQTopic = new RocketMQTopic(originTopic);
        TopicName pTopic = rocketMQTopic.getPulsarTopicName().getPartition(messageId.getPartitionIndex());

        Message<byte[]> message;
        long readerId = pTopic.toString().hashCode();

        try {
            lookMsgLock.lock();
            Reader<byte[]> topicReader = this.lookMsgReaders.get(readerId);
            if (topicReader == null) {
                topicReader = service.pulsar().getClient()
                        .newReader()
                        .startMessageId(messageId)
                        .startMessageIdInclusive()
                        .topic(pTopic.toString())
                        .create();
                this.lookMsgReaders.put(readerId, topicReader);
            }

            message = topicReader.readNext(fetchTimeoutInMs, TimeUnit.MILLISECONDS);
            if (message != null && MessageIdUtils.isMessageEquals(messageId, message.getMessageId())) {
                return this.entryFormatter.decodePulsarMessage(Collections.singletonList(message), null).get(0);
            } else {
                topicReader.seek(messageId);
                message = topicReader.readNext(fetchTimeoutInMs, TimeUnit.MILLISECONDS);
                if (message != null && MessageIdUtils.isMessageEquals(messageId, message.getMessageId())) {
                    return this.entryFormatter.decodePulsarMessage(Collections.singletonList(message), null).get(0);
                }
            }
        } catch (Exception ex) {
            log.warn("lookMessageByMessageId message[topic={}, msgId={}] error.", originTopic, messageId);
        } finally {
            lookMsgLock.unlock();
        }
        return null;
    }

    @Override
    public MessageExt lookMessageByTimestamp(String partitionedTopic, long timestamp) {
        try {
            lookMsgLock.lock();
            Long readerKey = (long) partitionedTopic.hashCode();
            Reader<byte[]> topicReader = this.lookMsgReaders.get(readerKey);
            if (topicReader == null) {
                topicReader = service.pulsar().getClient().newReader()
                        .topic(partitionedTopic)
                        .create();
                this.lookMsgReaders.put(readerKey, topicReader);
            }
            Preconditions.checkNotNull(topicReader);
            Message<byte[]> message;
            topicReader.seek(timestamp);
            message = topicReader.readNext();
            if (message != null) {
                return this.entryFormatter.decodePulsarMessage(Collections.singletonList(message), null).get(0);
            }
        } catch (Exception ex) {
            log.warn("lookMessageByMessageId message[topic={}, timestamp={}] error.", partitionedTopic, timestamp);
        } finally {
            lookMsgLock.unlock();
        }
        return null;
    }

    @Override
    public long now() {
        return systemClock.now();
    }

    @Override
    public RopGetMessageResult getMessage(int realPartitionId, RemotingCommand request,
            PullMessageRequestHeader requestHeader,
            RopMessageFilter messageFilter) {
        RopGetMessageResult getResult = new RopGetMessageResult();

        String consumerGroupName = requestHeader.getConsumerGroup();
        String topicName = requestHeader.getTopic();
        int queueId = requestHeader.getQueueId();

        // hang pull request if this broker not owner for the request partitionId topicName
        RocketMQTopic rmqTopic = new RocketMQTopic(topicName);
        if (!this.brokerController.getTopicConfigManager()
                .isPartitionTopicOwner(rmqTopic.getPulsarTopicName(), realPartitionId)) {
            getResult.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
            // set suspend flag
            requestHeader.setSysFlag(requestHeader.getSysFlag() | 2);
            return getResult;
        }

        if (requestFilterCache.getIfPresent(new PullRequestFilterKey(consumerGroupName, topicName, realPartitionId))
                != null) {
            getResult.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
            requestHeader.setSysFlag(requestHeader.getSysFlag() | 2);
            return getResult;
        }

        long queueOffset = requestHeader.getQueueOffset();
        int maxMsgNums = requestHeader.getMaxMsgNums();
        if (maxMsgNums < 1) {
            getResult.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            getResult.setNextBeginOffset(queueOffset);
            return getResult;
        }

        long maxOffset;
        long minOffset;
        try {
            maxOffset = this.brokerController.getConsumerOffsetManager()
                    .getMaxOffsetInQueue(new ClientTopicName(topicName), realPartitionId);
            minOffset = this.brokerController.getConsumerOffsetManager()
                    .getMinOffsetInQueue(new ClientTopicName(topicName), realPartitionId);
        } catch (RopPersistentTopicException e) {
            requestFilterCache
                    .put(new PullRequestFilterKey(consumerGroupName, topicName, realPartitionId),
                            pullRequestFilterValue);
            getResult.setStatus(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE);
            getResult.setNextBeginOffset(0L);
            return getResult;
        }

        MessageIdImpl startOffset;
        GetMessageStatus status;
        if (queueOffset <= MessageIdUtils.MIN_ROP_OFFSET) {
            startOffset = (MessageIdImpl) MessageId.earliest;
        } else if (queueOffset >= MessageIdUtils.MAX_ROP_OFFSET) {
            startOffset = (MessageIdImpl) MessageId.latest;
        } else {
            startOffset = MessageIdUtils.getMessageId(queueOffset);
        }
        long nextBeginOffset = queueOffset;
        String pTopic = rmqTopic.getPartitionName(realPartitionId);
        long readerId = buildPulsarReaderId(consumerGroupName, pTopic, this.ctx.channel().id().asLongText());

        List<ByteBuf> messagesBufferList = Lists.newArrayList();
        final PositionImpl startPosition = MessageIdUtils.getPosition(MessageIdUtils.getOffset(startOffset));
        ManagedCursor managedCursor = getOrCreateCursor(pTopic, rmqTopic, realPartitionId, startPosition);

        if (managedCursor != null) {
            Position position = startPosition;
            // seek offset if startOffset not equals lastNextBeginOffset
            MessageIdImpl lastNextBeginOffset = nextBeginOffsets.get(readerId);
            if (lastNextBeginOffset != null && !lastNextBeginOffset.equals(startOffset)) {
                log.info("[{}] [{}] Seek offset to [{}]", consumerGroupName, pTopic, startOffset);
                try {
                    PersistentTopic persistentTopic = brokerController.getConsumerOffsetManager()
                            .getPulsarPersistentTopic(new ClientTopicName(rmqTopic.getPulsarTopicName()), queueId);
                    persistentTopic.getManagedLedger().deleteCursor("Rop-cursor-" + readerId);
                    cursors.remove(pTopic);
                } catch (Exception e) {
                    log.warn("Delete cursor error", e);
                }

                managedCursor = cursors.computeIfAbsent(pTopic, (Function<String, ManagedCursor>) s -> {
                    try {
                        PositionImpl cursorStartPosition = startPosition;
                        if (startPosition.getEntryId() > -1) {
                            cursorStartPosition = new PositionImpl(startPosition.getLedgerId(),
                                    startPosition.getEntryId() - 1);
                        }

                        PersistentTopic persistentTopic = brokerController.getConsumerOffsetManager()
                                .getPulsarPersistentTopic(new ClientTopicName(rmqTopic.getPulsarTopicName()), queueId);
                        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
                        return managedLedger.newNonDurableCursor(cursorStartPosition, "Rop-cursor-" + readerId);
                    } catch (Exception e) {
                        log.warn("Topic [{}] create managedLedger failed", pTopic, e);
                    }
                    return null;
                });
            }

            if (managedCursor != null) {
                try {
                    List<Entry> entries = managedCursor.readEntries(maxMsgNums);
                    for (Entry entry : entries) {
                        try {
                            nextBeginOffset = MessageIdUtils
                                    .getOffset(entry.getLedgerId(), entry.getEntryId(), queueId);
                            ByteBuf byteBuffer = this.entryFormatter
                                    .decodePulsarMessage(entry.getDataBuffer(), nextBeginOffset, messageFilter);
                            if (byteBuffer != null) {
                                messagesBufferList.add(byteBuffer);
                            }
                            position = entry.getPosition();
                        } finally {
                            entry.release();
                        }
                    }
                } catch (ManagedLedgerException | InterruptedException e) {
                    log.warn("Fetch message failed, seek to startPosition [{}]", startPosition, e);
                    managedCursor.seek(position);
                } catch (Exception e) {
                    log.warn("Fetch message error, seek to startPosition [{}]", startPosition, e);
                    managedCursor.seek(position);
                }
            }
        }

        if (!messagesBufferList.isEmpty()) {
            status = GetMessageStatus.FOUND;
            getResult.setMessageBufferList(messagesBufferList);
        } else {
            status = GetMessageStatus.OFFSET_FOUND_NULL;
        }
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        nextBeginOffsets.put(readerId, MessageIdUtils.getMessageId(nextBeginOffset));
        return getResult;
    }

    private ManagedCursor getOrCreateCursor(String pTopic, RocketMQTopic rmqTopic, int partitionId,
            PositionImpl startPosition) {
        return cursors.computeIfAbsent(pTopic, (Function<String, ManagedCursor>) s -> {
            try {
                PersistentTopic persistentTopic = brokerController.getConsumerOffsetManager()
                        .getPulsarPersistentTopic(new ClientTopicName(rmqTopic.getPulsarTopicName()),
                                partitionId);
                try {
                    persistentTopic.getManagedLedger().deleteCursor(cursorName);
                } catch (ManagedLedgerException.CursorNotFoundException e) {
                    //
                }

                PositionImpl cursorStartPosition = startPosition;
                if (startPosition.getEntryId() > -1) {
                    cursorStartPosition = new PositionImpl(startPosition.getLedgerId(),
                            startPosition.getEntryId() - 1);
                }
                ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
                return managedLedger.newNonDurableCursor(cursorStartPosition, cursorName);
            } catch (Exception e) {
                log.warn("Topic [{}] create nonDurableCursor failed", pTopic, e);
            }
            return null;
        });
    }

    private void closeCursor(String pTopic) {
        try {
            cursors.remove(pTopic);
            TopicName topicName = TopicName.get(pTopic);
            PersistentTopic persistentTopic = brokerController.getConsumerOffsetManager()
                    .getPulsarPersistentTopic(new ClientTopicName(topicName), topicName.getPartitionIndex());
            persistentTopic.getManagedLedger().deleteCursor(cursorName);
        } catch (Exception e) {
            log.error("delete cursor error of pTopic[{}]", pTopic, e);
        }
    }

    private long buildPulsarReaderId(String... tags) {
        return (Joiner.on(SLASH_CHAR).join(tags)).hashCode();
    }

    private long buildPulsarProducerId(String... tags) {
        return (Joiner.on(SLASH_CHAR).join(tags)).hashCode();
    }

    enum State {
        Start,
        Connected,
        Failed,
        Connecting
    }

    /**
     * Get deliver at time from message properties.
     *
     * @param messageInner messageExtBrokerInner
     * @return deliver at time
     */
    private long getDeliverAtTime(MessageExtBrokerInner messageInner) {
        return NumberUtils.toLong(messageInner.getProperties().getOrDefault("__STARTDELIVERTIME", "0"));
    }

    private boolean isNotDelayMessage(long deliverAtTime) {
        return deliverAtTime <= System.currentTimeMillis();
    }
}
