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
import com.tencent.tdmq.handlers.rocketmq.inner.format.RopEntryFormatter;
import com.tencent.tdmq.handlers.rocketmq.inner.format.RopMessageFilter;
import com.tencent.tdmq.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import com.tencent.tdmq.handlers.rocketmq.inner.pulsar.PulsarMessageStore;
import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

    public static String ropHandlerName = "RopServerCnxHandler";
    private final int sendTimeoutInSec = 3;
    private final int maxBatchMessageNum = 20;
    private final BrokerService service;
    private final ConcurrentLongHashMap<Producer<byte[]>> producers;
    private final ConcurrentLongHashMap<Reader<byte[]>> readers;
    private final RopEntryFormatter entryFormatter = new RopEntryFormatter();
    private final ReentrantLock readLock = new ReentrantLock();
    private RocketMQBrokerController brokerController;
    private ChannelHandlerContext ctx;
    private SocketAddress remoteAddress;
    private State state;
    private SystemClock systemClock = new SystemClock();
    private int localListenPort = 9876;

    public RopServerCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        this.brokerController = brokerController;
        this.localListenPort =
                RocketMQProtocolHandler.getListenerPort(brokerController.getServerConfig().getRocketmqListeners());
        this.service = brokerController.getBrokerService();
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        this.state = State.Connected;
        this.producers = new ConcurrentLongHashMap(8, 1);
        this.readers = new ConcurrentLongHashMap(8, 1);
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
        producers.values().forEach((producer) -> producer.closeAsync());
        readers.values().forEach((reader) -> reader.closeAsync());
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
            if (messageInner.getDelayTimeLevel() > 0) {
                if (messageInner.getDelayTimeLevel() > this.brokerController.getServerConfig().getMaxDelayLevelNum()) {
                    messageInner.setDelayTimeLevel(this.brokerController.getServerConfig().getMaxDelayLevelNum());
                }

                int totalQueueNum = this.brokerController.getServerConfig().getRmqScheduleTopicPartitionNum();
                partitionId = partitionId % totalQueueNum;
                pTopic = this.brokerController.getDelayedMessageService()
                        .getDelayedTopicName(messageInner.getDelayTimeLevel());
                pTopic = pTopic + "-partition-" + partitionId;

                MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_TOPIC, messageInner.getTopic());
                MessageAccessor.putProperty(messageInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
                        String.valueOf(messageInner.getQueueId()));
                messageInner.setPropertiesString(MessageDecoder.messageProperties2String(messageInner.getProperties()));
                messageInner.setTopic(pTopic);
                messageInner.setQueueId(partitionId);

            }
        }

        long producerId = buildPulsarProducerId(producerGroup, pTopic, this.remoteAddress.toString());
        try {
            Producer<byte[]> producer = this.producers.get(producerId);
            if (producer == null) {
                log.info("putMessage creating producer[id={}] and channl=[{}].", producerId, ctx.channel());
                synchronized (this.producers) {
                    if (producer == null) {
                        producer = this.service.pulsar().getClient()
                                .newProducer()
                                .topic(pTopic)
                                .producerName(producerGroup + CommonUtils.UNDERSCORE_CHAR + producerId)
                                .sendTimeout(sendTimeoutInSec, TimeUnit.SECONDS)
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
            MessageIdImpl messageId = (MessageIdImpl) producer.send(body.get(0));
            AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
            appendMessageResult.setMsgNum(1);
            appendMessageResult.setWroteBytes(body.get(0).length);
            appendMessageResult.setMsgId(CommonUtils.createMessageId(this.ctx.channel().localAddress(), localListenPort,
                    MessageIdUtils.getOffset(messageId.getLedgerId(), messageId.getEntryId())));
            return new PutMessageResult(PutMessageStatus.PUT_OK, appendMessageResult);
        } catch (Exception ex) {
            PutMessageStatus status = PutMessageStatus.FLUSH_DISK_TIMEOUT;
            AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
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
            if (putMsgProducer == null || !putMsgProducer.isConnected()) {
                synchronized (this.producers) {
                    if (putMsgProducer == null || !putMsgProducer.isConnected()) {
                        log.info("putMessages create producer[id={}] successfully.", producerId);
                        putMsgProducer = this.service.pulsar().getClient().newProducer()
                                .topic(pTopic)
                                .producerName(producerGroup + producerId)
                                .batchingMaxPublishDelay(200, TimeUnit.MILLISECONDS)
                                .sendTimeout(sendTimeoutInSec, TimeUnit.SECONDS)
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
            AtomicInteger totoalBytesSize = new AtomicInteger(0);
            final Producer<byte[]> producer = putMsgProducer;
            body.forEach(item -> {
                batchMessageFutures.add(producer.sendAsync(item));
                totoalBytesSize.getAndAdd(item.length);
            });
            FutureUtil.waitForAll(batchMessageFutures).get(sendTimeoutInSec, TimeUnit.SECONDS);
            StringBuilder sb = new StringBuilder();
            for (CompletableFuture<MessageId> f : batchMessageFutures) {
                MessageIdImpl messageId = (MessageIdImpl) f.get();
                long ledgerId = messageId.getLedgerId();
                long entryId = messageId.getEntryId();
                String msgId = CommonUtils.createMessageId(this.ctx.channel().localAddress(), localListenPort,
                        MessageIdUtils.getOffset(ledgerId, entryId));
                sb.append(msgId).append(",");
            }
            AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
            appendMessageResult.setMsgNum(batchMessageFutures.size());
            appendMessageResult.setWroteBytes(totoalBytesSize.get());
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
    public MessageExt lookMessageByMessageId(String partitionedTopic, long offset) {
        Preconditions.checkNotNull(partitionedTopic, "topic mustn't be null");
        MessageIdImpl messageId = MessageIdUtils.getMessageId(offset);
        try {
            RocketMQTopic rocketMQTopic = new RocketMQTopic(partitionedTopic);
            TopicName tempTopic = rocketMQTopic.getPulsarTopicName();
            Map<Integer, InetSocketAddress> brokerAddr = this.brokerController.getTopicConfigManager()
                    .getTopicBrokerAddr(tempTopic);

            AtomicReference<Message<byte[]>> reference = new AtomicReference<>(null);

            for (Integer partitionId : brokerAddr.keySet()) {
                TopicName pTopic = tempTopic.getPartition(partitionId);
                Reader<byte[]> topicReader = this.readers.get(pTopic.toString().hashCode());
                try {
                    if (topicReader == null) {
                        synchronized (this.readers) {
                            if (topicReader == null) {
                                topicReader = service.pulsar().getClient()
                                        .newReader()
                                        .startMessageId(MessageId.latest)
                                        .topic(pTopic.toString())
                                        .create();
                                this.readers.put(pTopic.toString().hashCode(), topicReader);
                            }
                        }
                    }
                    Preconditions.checkNotNull(topicReader);
                    synchronized (topicReader) {
                        topicReader.seek(messageId);
                        Message<byte[]> message = topicReader.readNext(1000, TimeUnit.MILLISECONDS);
                        if (message != null) {
                            reference.set(message);
                            break;
                        }
                    }
                } catch (Exception ex) {
                    log.warn("lookMessageByMessageId create topicReader error.");
                }
            }
            if (reference.get() != null) {
                return this.entryFormatter.decodePulsarMessage(Collections.singletonList(reference.get()), null).get(0);
            }
        } catch (Exception ex) {
            log.warn("lookMessageByMessageId message[topic={}, msgId={}] error.", partitionedTopic, messageId);
        }
        return null;
    }

    @Override
    public MessageExt lookMessageByTimestamp(String partitionedTopic, long timestamp) {
        try {
            Reader<byte[]> topicReader = this.readers.get(partitionedTopic.hashCode());
            if (topicReader == null) {
                synchronized (this.readers) {
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
        // 当前已经消费掉的消息的offset的位置
        long commitOffset = requestHeader.getCommitOffset();
        // queueOffset 是要拉取消息的起始位置
        long queueOffset = requestHeader.getQueueOffset();
        int maxMsgNums = requestHeader.getMaxMsgNums();
        if (maxMsgNums < 1) {
            getResult.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            getResult.setNextBeginOffset(queueOffset);
            return getResult;
        }

        ClientGroupAndTopicName clientGroupName = new ClientGroupAndTopicName(consumerGroup, topic);
        long minOffsetInQueue = this.brokerController.getConsumerOffsetManager()
                .getMinOffsetInQueue(clientGroupName, partitionId);
        long maxOffsetInQueue = this.brokerController.getConsumerOffsetManager()
                .getMaxOffsetInQueue(clientGroupName, partitionId);
        long nextBeginOffset = queueOffset;

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        if (minOffsetInQueue == 0) {
            status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
            nextBeginOffset = queueOffset;
        } else if (queueOffset < minOffsetInQueue) {
            status = GetMessageStatus.OFFSET_TOO_SMALL;
            nextBeginOffset = minOffsetInQueue;
        } else if (queueOffset > maxOffsetInQueue) {
            status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
            nextBeginOffset = maxOffsetInQueue;
        } else {
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
                            .receiverQueueSize(100)
                            .startMessageId(MessageId.latest)
                            .readerName(consumerGroup + readerId)
                            .create();
                    Reader<byte[]> oldReader = this.readers.put(readerId, reader);
                    if (oldReader != null) {
                        oldReader.closeAsync();
                    }
                }

                Reader<byte[]> reader = this.readers.get(readerId);
                MessageId messageId = MessageIdUtils.getMessageId(queueOffset);
                reader.seek(messageId);
                Message<byte[]> message = null;
                for (int i = 0; i < maxMsgNums; i++) {
                    message = reader.readNext(200, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        messageList.add(message);
                    } else {
                        break;
                    }
                }
                if (!messageList.isEmpty()) {
                    MessageIdImpl lastMsgId = (MessageIdImpl) messageList.get(messageList.size() - 1).getMessageId();
                    nextBeginOffset = MessageIdUtils.getOffset(lastMsgId.getLedgerId(), lastMsgId.getEntryId() + 1);
                }
            } catch (Exception e) {
                log.warn("retrieve message error, group = [{}], topic = [{}].", consumerGroup, topic);
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
        }

        getResult.setMaxOffset(maxOffsetInQueue);
        getResult.setMinOffset(minOffsetInQueue);
        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);

        return getResult;
    }

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        return nextOffset;
    }

    private long buildPulsarReaderId(String... tags) {
        return Math.abs(Joiner.on("/").join(tags).hashCode());
    }

    private long buildPulsarProducerId(String... tags) {
        return Math.abs(Joiner.on("/").join(tags).hashCode());
    }

    enum State {
        Start,
        Connected,
        Failed,
        Connecting;
    }
}
