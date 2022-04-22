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

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TAGS;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_MESSAGE_ID;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.SLASH_CHAR;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.NonDurableCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageStatus;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQProtocolHandler;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.CommitLogOffset;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.RopGetMessageResult;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopEncodeException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopEntryFormatter;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopMessageFilter;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.pulsar.PulsarMessageStore;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Rop server cnx.
 */
@Slf4j
@Getter
public class RopServerCnx extends ChannelInboundHandlerAdapter implements PulsarMessageStore {

    public static final AtomicLong ADD_CURSOR_COUNT = new AtomicLong();
    public static final AtomicLong DEL_CURSOR_COUNT = new AtomicLong();
    public static final AtomicLong DELAY_SEND_COUNT = new AtomicLong();
    public static final AtomicLong TIMING_SEND_COUNT = new AtomicLong();

    private static final Cache<ImmutablePair<String, Integer>, Long> minOffsetCaches = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .initialCapacity(1024)
            .concurrencyLevel(64)
            .maximumSize(1000 * 1000)
            .build();

    private static final int sendTimeoutInSec = 3;
    private static final int maxPendingMessages = 30000;
    private static final int fetchTimeoutInMs = 3000; // 3 sec
    private static final String ropHandlerName = "RopServerCnxHandler";
    private static final Cache<String, Producer<byte[]>> producers = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .initialCapacity(1024)
            .removalListener((RemovalListener<String, Producer<byte[]>>) listener -> {
                log.info("remove internal producer from caches [name={}].", listener.getKey());
                Producer<byte[]> producer = listener.getValue();
                if (producer != null) {
                    producer.closeAsync();
                }
            })
            .build();
    private final BrokerService service;
    private final ConcurrentLongHashMap<Long> nextBeginOffsets;
    private final ConcurrentHashMap<Triple<Long, String, String>, ManagedCursor> cursors;
    private final HashMap<Long, Reader<byte[]>> lookMsgReaders;
    private final RopEntryFormatter entryFormatter = new RopEntryFormatter();

    private final SystemClock systemClock = new SystemClock();
    private final RocketMQBrokerController brokerController;
    private final ChannelHandlerContext ctx;
    private final SocketAddress remoteAddress;
    private final int localListenPort;
    private State state;
    private final String cursorBaseName = "rop-cursor_%d_%s";
    private volatile boolean isInactive = false;

    public RopServerCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        this.brokerController = brokerController;
        this.localListenPort =
                RocketMQProtocolHandler.getListenerPort(brokerController.getServerConfig().getRocketmqListeners());
        this.service = brokerController.getBrokerService();
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        this.state = State.Connected;
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
        this.isInactive = true;
        log.info("Closed connection from {}", remoteAddress);
        // Connection is gone, close the resources immediately
        cursors.forEach((triple, managedCursor) -> closeCursor(triple));
        nextBeginOffsets.clear();
        lookMsgReaders.clear();
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
        log.warn("RoP server cnx occur an exception, message: {}.", cause.getMessage());
        cursors.forEach((triple, managedCursor) -> closeCursor(triple));
        this.ctx.close();
    }

    @Override
    public void putMessage(int partitionId, MessageExtBrokerInner messageInner, String producerGroup,
            PutMessageCallback callback)
            throws Exception {
        Preconditions.checkNotNull(messageInner);
        Preconditions.checkNotNull(producerGroup);
        RocketMQTopic rmqTopic = new RocketMQTopic(messageInner.getTopic());
        String pTopic = rmqTopic.getPartitionName(partitionId);
        long deliverAtTime = getDeliverAtTime(messageInner);

        if (deliverAtTime - System.currentTimeMillis() > brokerController.getServerConfig().getRopMaxDelayTime()) {
            throw new RuntimeException("DELAY TIME IS TOO LONG");
        }

        final String msgId = messageInner.getProperties().get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        final String msgKey = messageInner.getProperties().get(PROPERTY_KEYS);
        final String msgTag = messageInner.getProperties().get(PROPERTY_TAGS);

        final int tranType = MessageSysFlag.getTransactionValue(messageInner.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            if (!isDelayMessage(deliverAtTime)) {
                pTopic = handleReconsumeDelayedMessage(messageInner);
            } else {
                // implements by delay message
                pTopic = handleTimingMessage(messageInner);
            }
        }

        try {
            List<byte[]> body = this.entryFormatter.encode(messageInner, 1);
            CompletableFuture<PutMessageResult> offsetFuture = null;
            /*
             * Optimize the production performance of publish messages.
             * If the broker is the owner of the current partitioned topic, directly use the PersistentTopic interface
             * for publish message.
             */
            if (!isDelayMessage(deliverAtTime)) {
                ClientTopicName clientTopicName = new ClientTopicName(messageInner.getTopic());
                // For SCHEDULE_TOPIC messages, we still use producer to send msg, the
                // `messageInner.getDelayTimeLevel() > 0` check will process RECONSUME_LATER failed case.
                // In here, we don't need to process the return value(MessageID)
                if (messageInner.getDelayTimeLevel() > 0) {
                    log.trace("Send delay level message topic: {}, messageInner: {}", messageInner.getTopic(),
                            messageInner);
                    DELAY_SEND_COUNT.incrementAndGet();
                    long startTimeMs = System.currentTimeMillis();
                    final String finalTopic= pTopic;
                    CompletableFuture<MessageId> messageIdFuture = getProducerFromCache(pTopic, producerGroup)
                            .newMessage()
                            .value((body.get(0)))
                            .property(ROP_MESSAGE_ID, msgId)
                            .sendAsync();
                    offsetFuture = messageIdFuture.thenApply((Function<MessageId, PutMessageResult>) messageId -> {
                        try {
                            long cost = System.currentTimeMillis() - startTimeMs;
                            if (cost >= 1000) {
                                log.warn("RoP sendMessage timeout. Cost = [{}ms], topic: [{}]", cost, finalTopic);
                            }
                            if (messageId == null) {
                                log.warn("Rop send delay level message error, messageId is null.");
                                return null;
                            }
                            return new PutMessageResult(msgId, messageId.toString(),
                                    MessageIdUtils.getOffset((MessageIdImpl) messageId), msgKey, msgTag);
                        } catch (Exception e) {
                            log.warn("Rop send delay level message error.", e);
                            return null;
                        }
                    });
                } else {
                    PersistentTopic persistentTopic = this.brokerController.getConsumerOffsetManager()
                            .getPulsarPersistentTopic(clientTopicName, partitionId);
                    // if persistentTopic is null, throw SYSTEM_ERROR to rop proxy and retry send request.
                    if (persistentTopic != null) {
                        offsetFuture = publishMessage(new RopMessage(msgId, msgKey, msgTag, body.get(0)),
                                persistentTopic, pTopic, partitionId);
                    } else {
                        throw new RopPersistentTopicException("PersistentTopic isn't on current broker.");
                    }
                }
            } else {
                TIMING_SEND_COUNT.incrementAndGet();
                long startTimeMs = System.currentTimeMillis();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Publish timing message to topic {}.",
                            messageInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC), pTopic);
                }

                final String finalTopic= pTopic;
                CompletableFuture<MessageId> messageIdFuture = getProducerFromCache(pTopic, producerGroup)
                        .newMessage()
                        .value((body.get(0)))
//                        .deliverAt(deliverAtTime)
                        .property(ROP_MESSAGE_ID, msgId)
                        .sendAsync();
                offsetFuture = messageIdFuture.thenApply((Function<MessageId, PutMessageResult>) messageId -> {
                    try {
                        long cost = System.currentTimeMillis() - startTimeMs;
                        if (cost >= 1000) {
                            log.warn("RoP sendMessage timeout. Cost = [{}ms], topic: [{}]", cost, finalTopic);
                        }
                        if (messageId == null) {
                            log.warn("Rop send delay message error, messageId is null.");
                            return null;
                        }
                        return new PutMessageResult(msgId, messageId.toString(),
                                MessageIdUtils.getOffset((MessageIdImpl) messageId), msgKey, msgTag);
                    } catch (Exception e) {
                        log.warn("Rop send delay message error.", e);
                        return null;
                    }
                });
            }

            /*
             * Handle future async by brokerController.getSendCallbackExecutor().
             */
            Preconditions.checkNotNull(offsetFuture);
            offsetFuture.whenCompleteAsync((putMessageResult, t) -> {
                if (t != null || putMessageResult == null) {
                    log.warn("[{}] PutMessage error.", rmqTopic.getPulsarFullName(), t);
                    // remove pulsar topic from cache if send error
                    this.brokerController.getConsumerOffsetManager()
                            .removePulsarTopic(new ClientTopicName(messageInner.getTopic()), partitionId);

                    PutMessageStatus status = PutMessageStatus.UNKNOWN_ERROR;
                    AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                    RopPutMessageResult ropPutMessageResult = new RopPutMessageResult(status, temp);
                    callback.callback(ropPutMessageResult);
                    return;
                }

                AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
                appendMessageResult.setMsgNum(1);
                appendMessageResult.setWroteBytes(body.get(0).length);
                CommitLogOffset commitLogOffset = new CommitLogOffset(false, partitionId, putMessageResult.getOffset());
                String offsetMsgId = CommonUtils.createMessageId(this.ctx.channel().localAddress(), localListenPort,
                        commitLogOffset.getCommitLogOffset());
                appendMessageResult.setMsgId(offsetMsgId);
                appendMessageResult.setLogicsOffset(putMessageResult.getOffset());
                appendMessageResult.setWroteOffset(commitLogOffset.getCommitLogOffset());
                RopPutMessageResult ropPutMessageResult = new RopPutMessageResult(PutMessageStatus.PUT_OK,
                        appendMessageResult);

                putMessageResult.setOffsetMsgId(offsetMsgId);
                putMessageResult.setPartition(partitionId);
                ropPutMessageResult.addPutMessageId(putMessageResult);

                callback.callback(ropPutMessageResult);

                brokerController.getMessageArrivingListener()
                        .arriving(messageInner.getTopic(), partitionId, putMessageResult.getOffset(), 0, 0,
                                null, null);
            }, brokerController.getSendCallbackExecutor());

        } catch (RopPersistentTopicException e) {
            log.warn("PersistentTopic[{}] not found.", pTopic);
            throw e;
        } catch (RopEncodeException e) {
            log.warn("PutMessage encode error.", e);
            throw e;
        } catch (Exception e) {
            log.warn("PutMessage error.", e);
            throw e;
        }
    }

    @Override
    public void putSendBackMsg(MessageExtBrokerInner messageInner, String producerGroup,
            RemotingCommand response, CompletableFuture<RemotingCommand> cmdFuture) {
        try {
            String pTopic = handleReconsumeDelayedMessage(messageInner);
            Producer<byte[]> producer = getProducerFromCache(pTopic, producerGroup);
            List<byte[]> body = this.entryFormatter.encode(messageInner, 1);
            CompletableFuture<MessageId> messageIdFuture = producer.newMessage()
                    .value((body.get(0)))
                    .sendAsync();
            messageIdFuture.whenCompleteAsync((msgId, ex) -> {
                if (ex == null) {
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                } else {
                    log.warn("putSendBackMsg error. exception:{}", ex.getMessage());
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("putSendBackMsg error:" + ex.getMessage());
                }
                cmdFuture.complete(response);
            }, brokerController.getSendCallbackExecutor());
        } catch (RopEncodeException e) {
            log.warn("putSendBackMsg error.", e);
            cmdFuture.completeExceptionally(e);
        }
    }

    @Override
    public void putMessages(int realPartitionID, MessageExtBatch batchMessage, String producerGroup,
            PutMessageCallback callback, boolean traceEnable) throws Exception {

        Preconditions.checkNotNull(batchMessage);
        Preconditions.checkNotNull(producerGroup);
        RocketMQTopic rmqTopic = new RocketMQTopic(batchMessage.getTopic());
        String pTopic = rmqTopic.getPartitionName(realPartitionID);

        try {
            StringBuilder sb = new StringBuilder();
            int totalBytesSize = 0;
            int messageNum = 0;

            List<CompletableFuture<PutMessageResult>> batchMessageFutures = new ArrayList<>();
            List<RopMessage> bodies = this.entryFormatter.encodeBatch(batchMessage, traceEnable);

            /*
             * Optimize the production performance of batch publish messages.
             * If the broker is the owner of the current partitioned topic, directly use the PersistentTopic interface
             * for publish message.
             */
            if (this.brokerController.getTopicConfigManager()
                    .isPartitionTopicOwner(rmqTopic.getPulsarTopicName(), realPartitionID)) {
                ClientTopicName clientTopicName = new ClientTopicName(batchMessage.getTopic());
                PersistentTopic persistentTopic = this.brokerController.getConsumerOffsetManager()
                        .getPulsarPersistentTopic(clientTopicName, realPartitionID);
                if (persistentTopic != null) {
                    for (RopMessage ropMessage : bodies) {
                        CompletableFuture<PutMessageResult> offsetFuture = publishMessage(ropMessage, persistentTopic,
                                pTopic, realPartitionID);
                        batchMessageFutures.add(offsetFuture);
                        messageNum++;
                        totalBytesSize += ropMessage.getMsgBody().length;
                    }
                } else {
                    throw new RopPersistentTopicException("PersistentTopic isn't on current broker.");
                }
            }

            /*
             * Handle future list async by brokerController.getSendCallbackExecutor().
             */
            final int messageNumFinal = messageNum;
            final int totalBytesSizeFinal = totalBytesSize;
            FutureUtil.waitForAll(batchMessageFutures).whenCompleteAsync((aVoid, throwable) -> {
                if (throwable != null) {
                    log.warn("[{}] PutMessages error.", rmqTopic.getPulsarFullName(), throwable);
                    // remove pulsar topic from cache if send error
                    this.brokerController.getConsumerOffsetManager()
                            .removePulsarTopic(new ClientTopicName(batchMessage.getTopic()), realPartitionID);

                    PutMessageStatus status = PutMessageStatus.UNKNOWN_ERROR;
                    AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                    org.apache.rocketmq.store.PutMessageResult result = new org.apache.rocketmq.store.PutMessageResult(
                            status, temp);
                    callback.callback(result);
                    return;
                }

                List<PutMessageResult> putMessageResults = new ArrayList<>(batchMessageFutures.size());
                for (CompletableFuture<PutMessageResult> f : batchMessageFutures) {
                    PutMessageResult putMessageResult = f.getNow(null);
                    if (putMessageResult == null) {
                        PutMessageStatus status = PutMessageStatus.UNKNOWN_ERROR;
                        AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
                        org.apache.rocketmq.store.PutMessageResult result =
                                new org.apache.rocketmq.store.PutMessageResult(status, temp);
                        callback.callback(result);
                        return;
                    }
                    String offsetMsgId = CommonUtils
                            .createMessageId(ctx.channel().localAddress(), localListenPort,
                                    putMessageResult.getOffset());
                    sb.append(offsetMsgId).append(",");

                    putMessageResult.setOffsetMsgId(offsetMsgId);
                    putMessageResult.setPartition(realPartitionID);
                    putMessageResults.add(putMessageResult);
                }

                AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
                appendMessageResult.setMsgNum(messageNumFinal);
                appendMessageResult.setWroteBytes(totalBytesSizeFinal);
                appendMessageResult.setMsgId(sb.toString());

                RopPutMessageResult result = new RopPutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);
                result.addPutMessageIds(putMessageResults);
                callback.callback(result);

                brokerController.getMessageArrivingListener()
                        .arriving(batchMessage.getTopic(), realPartitionID, Long.MAX_VALUE, 0, 0,
                                null, null);
            }, brokerController.getSendCallbackExecutor());

        } catch (RopPersistentTopicException e) {
            log.warn("putMessages PersistentTopic[{}] not found.", pTopic);
            throw e;
        } catch (RopEncodeException e) {
            log.warn("putMessages batchMessage encode error.", e);
            throw e;
        } catch (Exception e) {
            log.warn("putMessages batchMessage error.", e);
            throw e;
        }
    }

    private Producer<byte[]> createNewProducer(String pTopic, String producerGroup)
            throws PulsarClientException {
        ProducerBuilder<byte[]> producerBuilder = brokerController.getRopBrokerProxy().getPulsarClient()
                .newProducer()
                .maxPendingMessages(maxPendingMessages);

        return producerBuilder.clone()
                .topic(pTopic)
                .producerName(producerGroup + CommonUtils.UNDERSCORE_CHAR + System.currentTimeMillis())
                .sendTimeout(sendTimeoutInSec, TimeUnit.SECONDS)
                .enableBatching(false)
                .blockIfQueueFull(false)
                .create();
    }

    private CompletableFuture<PutMessageResult> publishMessage(RopMessage ropMessage, PersistentTopic persistentTopic,
            String pTopic, int partition) {
        ByteBuf headersAndPayload = null;
        try {
            headersAndPayload = this.entryFormatter.encode(ropMessage.getMsgBody(), ropMessage.getMsgId());

            // collect metrics
            org.apache.pulsar.broker.service.Producer producer = this.brokerController.getTopicConfigManager()
                    .getReferenceProducer(pTopic, persistentTopic, this);
            if (producer != null) {
                producer.updateRates(1, headersAndPayload.readableBytes());
                producer.getTopic().incrementPublishCount(1, headersAndPayload.readableBytes());
            }

            // publish message
            CompletableFuture<PutMessageResult> offsetFuture = new CompletableFuture<>();
            persistentTopic.publishMessage(headersAndPayload, RopMessagePublishContext
                    .get(offsetFuture, persistentTopic, partition, System.nanoTime(), ropMessage.getMsgId(),
                            ropMessage.getMsgKey(), ropMessage.getMsgTag()));

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
        /*Preconditions.checkNotNull(originTopic, "topic mustn't be null");
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
        }*/
        return null;
    }

    @Override
    public MessageExt lookMessageByCommitLogOffset(ConsumerSendMsgBackRequestHeader requestHeader) {
        CommitLogOffset commitLogOffset = new CommitLogOffset(requestHeader.getOffset());
        String topic = commitLogOffset.isRetryTopic() ? MixAll.getRetryTopic(requestHeader.getGroup())
                : requestHeader.getOriginTopic();
        ClientTopicName clientTopicName = new ClientTopicName(topic);
        try {
            PersistentTopic persistentTopic = brokerController.getGroupMetaManager()
                    .getPulsarPersistentTopic(clientTopicName, commitLogOffset.getPartitionId());
            PositionImpl positionForOffset = MessageIdUtils
                    .getPositionForOffset(persistentTopic.getManagedLedger(), commitLogOffset.getQueueOffset());
            Preconditions.checkNotNull(positionForOffset,
                    String.format("lookMessageByCommitLogOffset [topic:%s, partitionId:%d, offset:%d] not found.",
                            topic, commitLogOffset.getPartitionId(), commitLogOffset.getQueueOffset()));
            CompletableFuture<MessageExt> messageFuture = new CompletableFuture<>();
            persistentTopic.asyncReadEntry(positionForOffset,
                    new ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            try {
                                MessageExt messageExt = RopServerCnx.this.entryFormatter
                                        .decodeMessageByPulsarEntry(clientTopicName.toPulsarTopicName()
                                                .getPartition(commitLogOffset.getPartitionId()), entry);
                                messageFuture.complete(messageExt);
                            } catch (Exception e) {
                                log.warn("RoP lookMessageByCommitLogOffset [topic = {}] and [offset={}].", topic,
                                        commitLogOffset, e);
                            } finally {
                                entry.release();
                            }
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            messageFuture.completeExceptionally(exception);
                        }
                    },
                    null);
            return messageFuture.join();
        } catch (RopPersistentTopicException e) {
            log.warn("lookMessageByCommitLogOffset[request={}] error.", requestHeader, e);
        }
        return null;
    }

    @Override
    public MessageExt lookMessageByTimestamp(String partitionedTopic, long timestamp) {
        // TODO: will impl in next rc version
        return null;
    }

    @Override
    public long now() {
        return systemClock.now();
    }

    @Override
    public RopGetMessageResult getMessage(int pulsarPartitionId, RemotingCommand request,
            PullMessageRequestHeader requestHeader, RopMessageFilter messageFilter) {
        RopGetMessageResult getResult = new RopGetMessageResult();

        String consumerGroupName = requestHeader.getConsumerGroup();
        String topicName = requestHeader.getTopic();
        long queueOffset = requestHeader.getQueueOffset();

        // hang pull request if this broker not owner for the request partitionId topicName
        RocketMQTopic rmqTopic = new RocketMQTopic(topicName);
        if (!this.brokerController.getTopicConfigManager()
                .isPartitionTopicOwner(rmqTopic.getPulsarTopicName(), pulsarPartitionId)) {
            log.info("RoP group [{}] topic [{}] getMessage on incorrect owner.", consumerGroupName, topicName);
            getResult.setStatus(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE);
            getResult.setNextBeginOffset(queueOffset);
            // set suspend flag
            requestHeader.setSysFlag(requestHeader.getSysFlag() | 2);
            return getResult;
        }

        int maxMsgNums = requestHeader.getMaxMsgNums();
        if (maxMsgNums < 1) {
            log.info("RoP group [{}] topic [{}] getMessage maxMsgNums < 1.", consumerGroupName, topicName);
            getResult.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            getResult.setNextBeginOffset(queueOffset);
            return getResult;
        }

        getResult.setPulsarTopic(rmqTopic.getOrigNoDomainTopicName());
        getResult.setPartitionId(pulsarPartitionId);
        getResult.setInstanceName(remoteAddress.toString());

        long maxOffset = Long.MAX_VALUE;
        long minOffset = 0L;

        PersistentTopic persistentTopic;
        try {
            persistentTopic = brokerController.getConsumerOffsetManager()
                    .getPulsarPersistentTopic(new ClientTopicName(topicName), pulsarPartitionId);
            maxOffset = this.brokerController.getConsumerOffsetManager()
                    .getMaxOffsetInPulsarPartition(new ClientTopicName(topicName), pulsarPartitionId);
//            minOffset = queueOffset > 0 ? this.brokerController.getConsumerOffsetManager()
//                    .getMinOffsetInQueue(new ClientTopicName(topicName), pulsarPartitionId) : queueOffset;
            minOffset = minOffsetCaches.get(ImmutablePair.of(topicName, pulsarPartitionId),
                    () -> {
                        try {
                            return brokerController.getConsumerOffsetManager()
                                    .getMinOffsetInQueue(new ClientTopicName(topicName), pulsarPartitionId);
                        } catch (Exception e) {
                            return 0L;
                        }
                    });
            if (minOffset > 0) {
                queueOffset = Math.max(minOffset, queueOffset);
            }
        } catch (Exception e) {
            throw new RuntimeException();
        }
        ManagedLedger managedLedger = persistentTopic.getManagedLedger();

        long nextBeginOffset = queueOffset;
        String pTopic = rmqTopic.getPartitionName(pulsarPartitionId);
        long readerId = buildPulsarReaderId(consumerGroupName, pTopic, this.ctx.channel().id().asLongText());

        Triple<Long, String, String> triple = Triple
                .of(readerId, new RocketMQTopic(consumerGroupName).getPulsarTopicName().getLocalName(), pTopic);
        Long lastNextBeginOffset = nextBeginOffsets.get(readerId);
        if (lastNextBeginOffset != null && lastNextBeginOffset != queueOffset) {
            log.info("[{}] Seek offset from [{}] to [{}].", triple, lastNextBeginOffset, queueOffset);
            DEL_CURSOR_COUNT.incrementAndGet();
            closeCursor(triple, managedLedger);
        }

        ManagedCursor managedCursor = cursors.get(triple);
        if (managedCursor == null) {
            PositionImpl startPosition;
            if (queueOffset <= MessageIdUtils.MIN_ROP_OFFSET) {
                startPosition = PositionImpl.earliest;
            } else if (queueOffset == Long.MAX_VALUE || queueOffset > maxOffset) {
                startPosition = PositionImpl.latest;
            } else {
                startPosition = MessageIdUtils.getPositionForOffset(managedLedger, queueOffset);
            }
            managedCursor = getOrCreateCursor(triple, managedLedger, startPosition);
        }

        if (managedCursor != null) {
            try {
                List<Entry> entries = managedCursor.readEntries(maxMsgNums);

                // commit the offset, so backlog not affect by this cursor.
                if (!entries.isEmpty()) {
                    final Entry lastEntry = entries.get(entries.size() - 1);
                    final PositionImpl currentPosition = PositionImpl.get(
                            lastEntry.getLedgerId(), lastEntry.getEntryId());
                    commitOffset((NonDurableCursorImpl) managedCursor, currentPosition);
                }

                for (Entry entry : entries) {
                    if (entry == null) {
                        continue;
                    }
                    try {
                        if (entry.getDataBuffer().isReadable()) {
                            long currentOffset = MessageIdUtils.peekOffsetFromEntry(entry);
                            try {
                                RopMessage byteBuffer = this.entryFormatter
                                        .decodePulsarMessage(
                                                rmqTopic.getPulsarTopicName().getPartition(pulsarPartitionId),
                                                entry.getDataBuffer(), messageFilter);
                                if (byteBuffer != null) {
                                    getResult.addMessage(byteBuffer);
                                }
                            } catch (Exception e) {
                                log.warn("[{}] [{}] RoP pulsar entry parse failed, msgId: [{}:{}]", pTopic,
                                        consumerGroupName, entry.getLedgerId(), entry.getEntryId(), e);
                            }
                            nextBeginOffset = currentOffset + 1;
                        }
                    } finally {
                        entry.release();
                    }
                }
            } catch (Exception e) {
                log.warn("[{}] [{}] Fetch message error.", pTopic, consumerGroupName, e);
                closeCursor(triple, managedLedger);
            }
        }

        GetMessageStatus status = getResult.size() > 0 ? GetMessageStatus.FOUND : GetMessageStatus.OFFSET_FOUND_NULL;
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        nextBeginOffsets.put(readerId, nextBeginOffset);
        return getResult;
    }

    private ManagedCursor getOrCreateCursor(Triple<Long, String, String> triple, ManagedLedger managedLedger,
            PositionImpl startPosition) {
        if (this.isInactive) {
            return null;
        }
        return cursors.computeIfAbsent(triple, (Function<Triple<Long, String, String>, ManagedCursor>) t -> {
            try {
                ADD_CURSOR_COUNT.incrementAndGet();
                log.info("RoP create cursor for [{}], startPosition: [{}]", t, startPosition);
                try {
                    managedLedger.deleteCursor(getFullCursorName(t));
                } catch (ManagedLedgerException.CursorNotFoundException ignore) {

                }

                PositionImpl cursorStartPosition = startPosition;
                if (startPosition.getEntryId() > -1) {
                    cursorStartPosition = new PositionImpl(startPosition.getLedgerId(), startPosition.getEntryId() - 1);
                }
                return managedLedger.newNonDurableCursor(cursorStartPosition, getFullCursorName(t));
            } catch (Exception e) {
                log.warn("RoP create cursor for [{}] failed.", t, e);
            }
            return null;
        });
    }

    private void closeCursor(Triple<Long, String, String> triple, ManagedLedger managedLedger) {
        try {
            DEL_CURSOR_COUNT.incrementAndGet();
            log.info("RoP close cursor for [{}].", triple);
            cursors.remove(triple);
            managedLedger.deleteCursor(getFullCursorName(triple));
        } catch (ManagedLedgerException.CursorNotFoundException ignore) {

        } catch (Exception e) {
            log.error("RoP close cursor for [{}] failed.", triple, e);
        }
    }

    private void closeCursor(Triple<Long, String, String> triple) {
        try {
            DEL_CURSOR_COUNT.incrementAndGet();
            log.info("RoP close cursor for [{}].", triple);
            cursors.remove(triple);
            TopicName topicName = TopicName.get(triple.getRight());
            PersistentTopic persistentTopic = brokerController.getConsumerOffsetManager()
                    .getPulsarPersistentTopic(new ClientTopicName(topicName), topicName.getPartitionIndex());
            persistentTopic.getManagedLedger().deleteCursor(getFullCursorName(triple));
        } catch (ManagedLedgerException.CursorNotFoundException ignore) {

        } catch (Exception e) {
            log.error("RoP close cursor for [{}] failed.", triple, e);
        }
    }

    private long buildPulsarReaderId(String... tags) {
        return (Joiner.on(SLASH_CHAR).join(tags)).hashCode();
    }

    private String buildPulsarProducerName(String... tags) {
        return Joiner.on(SLASH_CHAR).join(tags);
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
        return NumberUtils.toLong(messageInner.getProperties().getOrDefault(CommonUtils.DELIVER_AT_TIME_PROPERTY_NAME, "0"));
    }

    private boolean isDelayMessage(long deliverAtTime) {
        return deliverAtTime > System.currentTimeMillis();
    }

    private Producer<byte[]> getProducerFromCache(String pulsarTopic, String producerGroup) {
        try {
            String pulsarProducerName = buildPulsarProducerName(pulsarTopic, producerGroup, "SendProcessor");
            Producer<byte[]> producer = producers
                    .get(pulsarProducerName, () -> createNewProducer(pulsarTopic, pulsarProducerName));
            return producer;
        } catch (Exception e) {
            log.warn("getProducerFromCache[topic={},producerGroup={}] error.", pulsarTopic, producerGroup, e);
        }
        return null;
    }

    private String handleReconsumeDelayedMessage(MessageExtBrokerInner messageInner) {
        ClientTopicName clientTopicName = new ClientTopicName(messageInner.getTopic());
        String sendTopicName = clientTopicName.getPulsarTopicName();
        if (messageInner.getDelayTimeLevel() > 0 && !clientTopicName.isDLQTopic()) {
            if (messageInner.getDelayTimeLevel() > this.brokerController.getServerConfig().getMaxDelayLevelNum()) {
                messageInner.setDelayTimeLevel(this.brokerController.getServerConfig().getMaxDelayLevelNum());
            }
            sendTopicName = this.brokerController.getDelayedMessageService()
                    .getDelayedTopicName(messageInner.getDelayTimeLevel());

            MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_TOPIC, messageInner.getTopic());
            MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
                    String.valueOf(messageInner.getQueueId()));
            messageInner.setPropertiesString(MessageDecoder.messageProperties2String(messageInner.getProperties()));
            messageInner.setTopic(sendTopicName);
        }
        return sendTopicName;
    }

    private String handleTimingMessage(MessageExtBrokerInner messageInner) {
        ClientTopicName clientTopicName = new ClientTopicName(messageInner.getTopic());
        String sendTopicName = clientTopicName.getPulsarTopicName();
        if (!clientTopicName.isDLQTopic()) {
            sendTopicName = this.brokerController.getDelayedMessageService()
                    .getTimingTopicName();

            MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_TOPIC, messageInner.getTopic());
            MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
                    String.valueOf(messageInner.getQueueId()));
            messageInner.setPropertiesString(MessageDecoder.messageProperties2String(messageInner.getProperties()));
            messageInner.setTopic(sendTopicName);
        }
        return sendTopicName;
    }

    private String getFullCursorName(Triple<Long, String, String> triple) {
        return String.format(cursorBaseName, triple.getLeft(), triple.getMiddle());
    }

    // commit the offset, so backlog not affect by this cursor.
    private static void commitOffset(NonDurableCursorImpl cursor, PositionImpl currentPosition) {
        cursor.asyncMarkDelete(currentPosition, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("Mark delete success for position: {}", currentPosition);
                }
            }

            // this is OK, since this is kind of cumulative ack, following commit will come.
            @Override
            public void markDeleteFailed(ManagedLedgerException e, Object ctx) {
                log.warn("Mark delete success for position: {} with error:", currentPosition, e);
            }
        }, null);
    }
}
