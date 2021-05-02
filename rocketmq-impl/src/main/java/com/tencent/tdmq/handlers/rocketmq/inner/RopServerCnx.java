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

package com.tencent.tdmq.handlers.rocketmq.inner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.tencent.tdmq.handlers.rocketmq.RocketMQProtocolHandler;
import com.tencent.tdmq.handlers.rocketmq.inner.consumer.RopGetMessageResult;
import com.tencent.tdmq.handlers.rocketmq.inner.exception.RopPullMessageException;
import com.tencent.tdmq.handlers.rocketmq.inner.format.RopEntryFormatter;
import com.tencent.tdmq.handlers.rocketmq.inner.format.RopMessageFilter;
import com.tencent.tdmq.handlers.rocketmq.inner.pulsar.PulsarMessageStore;
import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
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

/**
 * Rop server cnx.
 */
@Slf4j
@Getter
public class RopServerCnx extends ChannelInboundHandlerAdapter implements PulsarMessageStore {

    private static final int sendTimeoutInSec = 300;
    private static final int maxBatchMessageNum = 20;
    private static final int fetchTimeoutInMs = 200;
    public static String ropHandlerName = "RopServerCnxHandler";
    private final BrokerService service;
    private final ConcurrentLongHashMap<Producer<byte[]>> producers;
    private final ConcurrentLongHashMap<Reader<byte[]>> readers;
    private final ConcurrentLongHashMap<Reader<byte[]>> lookupIdReaders;
    private final RopEntryFormatter entryFormatter = new RopEntryFormatter();
    private final ReentrantLock readLock = new ReentrantLock();
    private final SystemClock systemClock = new SystemClock();
    private RocketMQBrokerController brokerController;
    private ChannelHandlerContext ctx;
    private SocketAddress remoteAddress;
    private State state;
    private int localListenPort = 9876;

    public RopServerCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        this.brokerController = brokerController;
        this.localListenPort =
                RocketMQProtocolHandler.getListenerPort(brokerController.getServerConfig().getRocketmqListeners());
        this.service = brokerController.getBrokerService();
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        this.state = State.Connected;
        this.producers = new ConcurrentLongHashMap(2, 1);
        this.readers = new ConcurrentLongHashMap(2, 1);
        this.lookupIdReaders = new ConcurrentLongHashMap(2, 1);
        synchronized (ctx) {
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
        readers.values().forEach(Reader::closeAsync);
        producers.clear();
        readers.clear();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (this.state != State.Failed) {
            log.warn("[{}] Got exception {}", this.remoteAddress,
                    ClientCnx.isKnownException(cause) ? cause : ExceptionUtils
                            .getStackTrace(cause));
            this.state = State.Failed;
        } else if (log.isDebugEnabled()) {
            log.debug("[{}] Got exception: {}", this.remoteAddress, cause);
        }
        this.ctx.close();
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner messageInner, String producerGroup) {
        Preconditions.checkNotNull(messageInner);
        Preconditions.checkNotNull(producerGroup);
        RocketMQTopic rmqTopic = new RocketMQTopic(messageInner.getTopic());
        int partitionId = messageInner.getQueueId();
        String pTopic = rmqTopic.getPartitionName(partitionId);

        final int tranType = MessageSysFlag.getTransactionValue(messageInner.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            if (messageInner.getDelayTimeLevel() > 0 && !rmqTopic.isDLQTopic()) {
                if (messageInner.getDelayTimeLevel() > this.brokerController.getServerConfig().getMaxDelayLevelNum()) {
                    messageInner.setDelayTimeLevel(this.brokerController.getServerConfig().getMaxDelayLevelNum());
                }

                int totalQueueNum = this.brokerController.getServerConfig().getRmqScheduleTopicPartitionNum();
                partitionId = partitionId % totalQueueNum;
                pTopic = this.brokerController.getDelayedMessageService()
                        .getDelayedTopicName(messageInner.getDelayTimeLevel(), partitionId);

                MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_TOPIC, messageInner.getTopic());
                MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
                        String.valueOf(messageInner.getQueueId()));
                messageInner.setPropertiesString(MessageDecoder.messageProperties2String(messageInner.getProperties()));
                messageInner.setTopic(pTopic);
                messageInner.setQueueId(partitionId);

            }
        }

        long producerId = buildPulsarProducerId(producerGroup, pTopic, ctx.channel().remoteAddress().toString());
        try {
            Producer<byte[]> producer = this.producers.get(producerId);
            if (producer == null) {
                log.info("putMessage creating producer[id={}] and channl=[{}].", producerId, ctx.channel());
                synchronized (this.producers) {
                    if (this.producers.get(producerId) == null) {
                        producer = this.service.pulsar().getClient()
                                .newProducer()
                                .topic(pTopic)
                                .maxPendingMessages(500)
                                .producerName(producerGroup + CommonUtils.UNDERSCORE_CHAR + producerId)
                                .sendTimeout(sendTimeoutInSec, TimeUnit.MILLISECONDS)
                                .enableBatching(false)
                                .create();
                        Producer<byte[]> oldProducer = this.producers.put(producerId, producer);
                        if (oldProducer != null) {
                            oldProducer.closeAsync();
                        }
                    }
                }
            }
            List<byte[]> body = this.entryFormatter.encode(messageInner, 1);
            MessageIdImpl messageId = (MessageIdImpl) this.producers.get(producerId).send(body.get(0));
            AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
            appendMessageResult.setMsgNum(1);
            appendMessageResult.setWroteBytes(body.get(0).length);
            appendMessageResult.setMsgId(CommonUtils.createMessageId(this.ctx.channel().localAddress(), localListenPort,
                    MessageIdUtils.getOffset(messageId.getLedgerId(), messageId.getEntryId(), partitionId)));
            return new PutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);
        } catch (Exception ex) {
            PutMessageStatus status = PutMessageStatus.FLUSH_DISK_TIMEOUT;
            AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            log.warn("Put message error", ex);
            return new PutMessageResult(status, temp);
        }
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch batchMessage, String producerGroup) {
        RocketMQTopic rmqTopic = new RocketMQTopic(batchMessage.getTopic());
        int partitionId = batchMessage.getQueueId();
        String pTopic = rmqTopic.getPartitionName(partitionId);
        long producerId = buildPulsarProducerId(producerGroup, pTopic, this.remoteAddress.toString());
        try {
            Producer<byte[]> putMsgProducer = this.producers.get(producerId);
            if (putMsgProducer == null) {
                synchronized (this.producers) {
                    if (this.producers.get(producerId) == null) {
                        log.info("putMessages create producer[id={}] successfully.", producerId);
                        putMsgProducer = this.service.pulsar().getClient().newProducer()
                                .topic(pTopic)
                                .producerName(producerGroup + producerId)
                                .batchingMaxPublishDelay(fetchTimeoutInMs, TimeUnit.MILLISECONDS)
                                .sendTimeout(sendTimeoutInSec, TimeUnit.MILLISECONDS)
                                .batchingMaxMessages(maxBatchMessageNum)
                                .enableBatching(true)
                                .create();
                        Producer<byte[]> oldProducer = this.producers.put(producerId, putMsgProducer);
                        if (oldProducer != null) {
                            oldProducer.closeAsync();
                        }
                    }
                }
            }

            List<CompletableFuture<MessageId>> batchMessageFutures = new ArrayList<>();
            List<byte[]> body = this.entryFormatter.encode(batchMessage, 1);
            AtomicInteger totalBytesSize = new AtomicInteger(0);
            body.forEach(item -> {
                batchMessageFutures.add(this.producers.get(producerId).sendAsync(item));
                totalBytesSize.getAndAdd(item.length);
            });
            FutureUtil.waitForAll(batchMessageFutures).get(sendTimeoutInSec, TimeUnit.MILLISECONDS);
            StringBuilder sb = new StringBuilder();
            for (CompletableFuture<MessageId> f : batchMessageFutures) {
                MessageIdImpl messageId = (MessageIdImpl) f.get();
                long ledgerId = messageId.getLedgerId();
                long entryId = messageId.getEntryId();
                String msgId = CommonUtils.createMessageId(this.ctx.channel().localAddress(), localListenPort,
                        MessageIdUtils.getOffset(ledgerId, entryId, partitionId));
                sb.append(msgId).append(",");
            }
            AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
            appendMessageResult.setMsgNum(batchMessageFutures.size());
            appendMessageResult.setWroteBytes(totalBytesSize.get());
            PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);
            appendMessageResult.setMsgId(sb.toString());
            return putMessageResult;
        } catch (Exception ex) {
            log.warn("putMessages batchMessage fail.", ex);
            PutMessageStatus status = PutMessageStatus.FLUSH_DISK_TIMEOUT;
            AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            return new PutMessageResult(status, temp);
        }
    }

    @Override
    public MessageExt lookMessageByMessageId(String partitionedTopic, String msgId) {
        try {
            MessageIdImpl messageId = CommonUtils.decodeMessageId(msgId);
            Reader<byte[]> topicReader = this.readers.get(partitionedTopic.hashCode());
            if (topicReader == null) {
                synchronized (this.readers) {
                    topicReader = this.readers.get(partitionedTopic.hashCode());
                    if (topicReader == null) {
                        topicReader = service.pulsar().getClient().newReader()
                                .topic(partitionedTopic)
                                .create();
                        this.readers.put(partitionedTopic.hashCode(), topicReader);
                    }
                }
            }
            Preconditions.checkNotNull(topicReader);
            Message<byte[]> message = null;
            synchronized (topicReader) {
                topicReader.seek(messageId);
                message = topicReader.readNext();
            }
            return this.entryFormatter.decodePulsarMessage(Collections.singletonList(message), null).get(0);
        } catch (Exception ex) {
            log.warn("lookMessageByMessageId message[topic={}, msgId={}] error.", partitionedTopic, msgId);
        }
        return null;
    }

    @Override
    public MessageExt lookMessageByMessageId(String originTopic, long offset) {
        Preconditions.checkNotNull(originTopic, "topic mustn't be null");
        MessageIdImpl messageId = MessageIdUtils.getMessageId(offset);

        RocketMQTopic rocketMQTopic = new RocketMQTopic(originTopic);
        TopicName pTopic = rocketMQTopic.getPulsarTopicName().getPartition(messageId.getPartitionIndex());

        Message<byte[]> message = null;
        int readerId = pTopic.toString().hashCode();
        Reader<byte[]> topicReader = this.lookupIdReaders.get(readerId);
        try {
            if (topicReader == null) {
                synchronized (this.lookupIdReaders) {
                    if (this.lookupIdReaders.get(readerId) == null) {
                        topicReader = service.pulsar().getClient()
                                .newReader()
                                .startMessageId(messageId)
                                .startMessageIdInclusive()
                                .topic(pTopic.toString())
                                .create();
                        this.lookupIdReaders.put(readerId, topicReader);
                    }
                }
            }
            topicReader = this.lookupIdReaders.get(readerId);
            synchronized (topicReader) {
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
            }
        } catch (Exception ex) {
            log.warn("lookMessageByMessageId message[topic={}, msgId={}] error.", originTopic, messageId);
        }
        return null;
    }

    @Override
    public MessageExt lookMessageByTimestamp(String partitionedTopic, long timestamp) {
        try {
            Reader<byte[]> topicReader = this.readers.get(partitionedTopic.hashCode());
            if (topicReader == null) {
                synchronized (this.readers) {
                    topicReader = this.readers.get(partitionedTopic.hashCode());
                    if (topicReader == null) {
                        topicReader = service.pulsar().getClient().newReader()
                                .topic(partitionedTopic)
                                .create();
                        this.readers.put(partitionedTopic.hashCode(), topicReader);
                    }
                }
            }
            Preconditions.checkNotNull(topicReader);
            Message<byte[]> message = null;
            synchronized (topicReader) {
                topicReader.seek(timestamp);
                message = topicReader.readNext();
            }
            return this.entryFormatter.decodePulsarMessage(Collections.singletonList(message), null).get(0);
        } catch (Exception ex) {
            log.warn("lookMessageByMessageId message[topic={}, timestamp={}] error.", partitionedTopic, timestamp);
        }
        return null;
    }

    @Override
    public long now() {
        return systemClock.now();
    }

    @Override
    public RopGetMessageResult getMessage(RemotingCommand request, PullMessageRequestHeader requestHeader,
            RopMessageFilter messageFilter) {
        RopGetMessageResult getResult = new RopGetMessageResult();
        String consumerGroup = requestHeader.getConsumerGroup();
        String topic = requestHeader.getTopic();
        int partitionId = requestHeader.getQueueId();
        // queueOffset 是要拉取消息的起始位置
        long queueOffset = requestHeader.getQueueOffset();
        int maxMsgNums = requestHeader.getMaxMsgNums();
        if (maxMsgNums < 1) {
            getResult.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            getResult.setNextBeginOffset(queueOffset);
            return getResult;
        }

        MessageIdImpl startOffset;
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        if (queueOffset <= MessageIdUtils.MIN_ROP_OFFSET) {
            startOffset = (MessageIdImpl) MessageId.earliest;
        } else if (queueOffset >= MessageIdUtils.MAX_ROP_OFFSET) {
            startOffset = (MessageIdImpl) MessageId.latest;
        } else {
            startOffset = MessageIdUtils.getMessageId(queueOffset);
        }
        long nextBeginOffset = queueOffset;

        String ctxId = this.ctx.channel().id().asLongText();
        long readerId = buildPulsarReaderId(consumerGroup, topic, ctxId);
        RocketMQTopic rmqTopic = new RocketMQTopic(requestHeader.getTopic());
        String pTopic = rmqTopic.getPartitionName(partitionId);
        // 通过offset来取出要开始消费的messageId的位置
        List<Message<byte[]>> messageList = new ArrayList<>();
        try {
            readLock.lock();
            if (!this.readers.containsKey(readerId)) {
                Reader<byte[]> reader = this.service.pulsar().getClient().newReader()
                        .topic(pTopic)
                        .receiverQueueSize(maxMsgNums)
                        .startMessageId(startOffset)
                        .startMessageIdInclusive()
                        .readerName(consumerGroup + readerId)
                        .create();
                Reader<byte[]> oldReader = this.readers.put(readerId, reader);
                if (oldReader != null) {
                    oldReader.closeAsync();
                }
            }

            Reader<byte[]> reader = this.readers.get(readerId);
            if (startOffset != MessageId.earliest && startOffset != MessageId.latest) {
                Message<byte[]> message = reader.readNext(fetchTimeoutInMs, TimeUnit.MILLISECONDS);
                if (message != null && !MessageIdUtils.isMessageEquals(startOffset, message.getMessageId())) {
                    reader.seek(startOffset);
                    message = reader.readNext(fetchTimeoutInMs, TimeUnit.MILLISECONDS);
                    if (message != null && !MessageIdUtils.isMessageEquals(startOffset, message.getMessageId())) {
                        if (message != null) {
                            log.error("getMessage error, for seekOffset=[{}] isn't matched readOffset=[{}].",
                                    startOffset, message.getMessageId());
                            throw new RopPullMessageException("couldn't find start-offset matched message");
                        } else {
                            log.error("getMessage null message for seekOffset=[{}].", startOffset);
                        }
                    }
                }
            }

            for (int i = 0; i < maxMsgNums; i++) {
                Message<byte[]> message = reader.readNext(fetchTimeoutInMs, TimeUnit.MILLISECONDS);
                if (message == null) {
                    break;
                }
                messageList.add(message);
                nextBeginOffset = MessageIdUtils.getOffset((MessageIdImpl) message.getMessageId());
            }
        } catch (Exception e) {
            log.warn("retrieve message error, group = [{}], topic = [{}], startOffset=[{}].",
                    new Object[]{consumerGroup, topic, startOffset});
            e.printStackTrace();
        } finally {
            readLock.unlock();
        }

        List<ByteBuffer> messagesBufferList = this.entryFormatter
                .decodePulsarMessageResBuffer(messageList, messageFilter);
        if (null != messagesBufferList && !messagesBufferList.isEmpty()) {
            status = GetMessageStatus.FOUND;
            getResult.setMessageBufferList(messagesBufferList);
        } else {
            status = GetMessageStatus.OFFSET_FOUND_NULL;
        }
        getResult.setMaxOffset(MessageIdUtils.MAX_ROP_OFFSET);
        getResult.setMinOffset(requestHeader.getCommitOffset());
        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        return getResult;
    }

    private long buildPulsarReaderId(String... tags) {
        return (Joiner.on("/").join(tags)).hashCode();
    }

    private long buildPulsarProducerId(String... tags) {
        return (Joiner.on("/").join(tags)).hashCode();
    }

    enum State {
        Start,
        Connected,
        Failed,
        Connecting;
    }
}
