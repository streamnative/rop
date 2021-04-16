package com.tencent.tdmq.handlers.rocketmq.inner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.tencent.tdmq.handlers.rocketmq.inner.format.RopEntryFormatter;
import com.tencent.tdmq.handlers.rocketmq.inner.pulsar.PulsarMessageStore;
import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.PulsarUtil;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

@Slf4j
@Getter
public class RopServerCnx extends ChannelInboundHandlerAdapter implements PulsarMessageStore {

    public static String ROP_HANDLER_NAME = "RopServerCnxHandler";
    private final int SEND_TIMEOUT_IN_SEC = 3;
    private final int MAX_BATCH_MESSAGE_NUM = 20;
    private final BrokerService service;
    private final ConcurrentLongHashMap<Producer> producers;
    private final ConcurrentLongHashMap<Consumer> consumers;
    private final ConcurrentLongHashMap<Reader> readers;
    private final RopEntryFormatter entryFormatter = new RopEntryFormatter();
    private final AtomicLong seqGenerator = new AtomicLong();
    private RocketMQBrokerController brokerController;
    private ChannelHandlerContext ctx;
    private SocketAddress remoteAddress;
    private State state;
    private SystemClock systemClock = new SystemClock();

    public RopServerCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        this.brokerController = brokerController;
        this.service = brokerController.getBrokerService();
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        this.state = State.Connected;
        this.producers = new ConcurrentLongHashMap(8, 1);
        this.consumers = new ConcurrentLongHashMap(8, 1);
        this.readers = new ConcurrentLongHashMap(8, 1);
        synchronized (ctx) {
            if (ctx.pipeline().get(ROP_HANDLER_NAME) == null) {
                ctx.pipeline().addLast(ROP_HANDLER_NAME, this);
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
        consumers.values().forEach((consumer) -> consumer.closeAsync());
        readers.values().forEach((reader) -> reader.closeAsync());
        producers.clear();
        consumers.clear();
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
        long producerId = buildPulsarProducerId(producerGroup, pTopic);
        try {
            if (!this.producers.containsKey(producerId)) {
                Producer<byte[]> producer = this.service.pulsar().getClient().newProducer()
                        .topic(pTopic)
                        .producerName(producerGroup + producerId)
                        .sendTimeout(SEND_TIMEOUT_IN_SEC, TimeUnit.SECONDS)
                        .enableBatching(false)
                        .create();
                this.producers.putIfAbsent(producerId, producer);
            }
            List<ByteBuf> body = this.entryFormatter.encode(messageInner, 1);
            MessageIdImpl messageId = (MessageIdImpl) producers.get(producerId).send(body.get(0));

            AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
            appendMessageResult.setMsgId(CommonUtils.createMessageId(this.ctx.channel().localAddress(),
                    MessageIdUtils.getOffset(messageId.getLedgerId(), messageId.getLedgerId(),
                            messageId.getPartitionIndex())));
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
        long producerId = buildPulsarProducerId(producerGroup, pTopic);
        try {
            if (!this.producers.containsKey(producerId)) {
                Producer<byte[]> producer = this.service.pulsar().getClient().newProducer()
                        .topic(pTopic)
                        .producerName(producerGroup + producerId)
                        .sendTimeout(SEND_TIMEOUT_IN_SEC, TimeUnit.SECONDS)
                        .batchingMaxMessages(MAX_BATCH_MESSAGE_NUM)
                        .enableBatching(true)
                        .create();
                this.producers.putIfAbsent(producerId, producer);
            }

            List<CompletableFuture<MessageId>> batchMessageFutures = new ArrayList<>();
            List<ByteBuf> body = this.entryFormatter.encode(batchMessage, 1);
            body.forEach(
                    (item) -> {
                        CompletableFuture<PutMessageResult> putMessageFuture = new CompletableFuture<>();
                        CompletableFuture<MessageId> sendFuture = this.producers.get(producerId).sendAsync(item);
                        batchMessageFutures.add(sendFuture);
                    }
            );
            FutureUtil.waitForAll(batchMessageFutures).get(SEND_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
            StringBuilder sb = new StringBuilder();
            for (CompletableFuture<MessageId> f : batchMessageFutures) {
                MessageIdImpl messageId = (MessageIdImpl) f.get();
                long ledgerId = messageId.getLedgerId();
                long entryId = messageId.getEntryId();
                int partitionIndex = messageId.getPartitionIndex();
                String msgId = CommonUtils.createMessageId(this.ctx.channel().localAddress(),
                        MessageIdUtils.getOffset(ledgerId, entryId, partitionIndex));
                sb.append(msgId).append(",");
            }
            AppendMessageResult appendMessageResult = new AppendMessageResult(AppendMessageStatus.PUT_OK);
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
    public MessageExt lookMessageByMessageId(String topic, String msgId) {
        try {
            MessageIdImpl messageId = MessageIdUtils.getMessageId(CommonUtils.decodeMessageId(msgId).getOffset());
            Reader<byte[]> topicReader = this.readers.get(topic.hashCode());
            if (topicReader == null) {
                synchronized (this.readers) {
                    if (topicReader == null) {
                        topicReader = service.pulsar().getClient().newReader().topic(topic)
                                .startMessageId(messageId).create();
                        this.readers.put(topic.hashCode(), topicReader);
                    }
                }
            }
            Preconditions.checkNotNull(topicReader);
            Message<byte[]> message = null;
            synchronized (topicReader) {
                topicReader.seek(messageId);
                message = topicReader.readNext();
            }
            return this.entryFormatter.decodePulsarMessage(Collections.singletonList(message)).get(0);
        } catch (Exception ex) {
            log.warn("lookMessageByMessageId message[topic={}, msgId={}] error.", topic, msgId);
        }
        return null;
    }

    @Override
    public long now() {
        return systemClock.now();
    }

    @Override
    public GetMessageResult getMessage(RemotingCommand request, PullMessageRequestHeader requestHeader) {

        byte[] body = request.getBody();
        ByteBuffer buffer = ByteBuffer.wrap(body);
        ByteBuf headersAndPayload = Unpooled.wrappedBuffer(buffer);

        String consumerGroup = requestHeader.getConsumerGroup();
        String topic = requestHeader.getTopic();
        int partitionID = requestHeader.getQueueId();
        long commitOffset = requestHeader.getCommitOffset();
        long queueOffset = requestHeader.getQueueOffset();
        int maxMsgNums = requestHeader.getMaxMsgNums();
        // 集群和广播这两种消费模型
        String subscription = requestHeader.getSubscription();
        int sysFlag = requestHeader.getSysFlag();
        String expressionType = requestHeader.getExpressionType();
        Map<String, String> properties = request.getExtFields();
        String ctxId = this.ctx.channel().id().asLongText();

        long consumerId = buildPulsarConsumerId(consumerGroup, topic, ctxId);
        long seqId = seqGenerator.incrementAndGet();
        GetMessageResult getResult = new GetMessageResult();

        if (!this.consumers.containsKey(consumerId)) {
            CommandSubscribe.Builder builder = CommandSubscribe.newBuilder();
            CommandSubscribe commandSubscribe = builder
                    .setTopic(topic)
                    .setSubscription(consumerGroup)
                    .setSubType(PulsarUtil.parseSubType(SubscriptionType.Exclusive))
                    .setConsumerId(consumerId)
                    .setRequestId(seqId)
                    .setConsumerName(consumerGroup + consumerId)
                    .setDurable(true) // 代表 cursor 是否需要持久化
//                .setMetadata() // 代表properties对象，是一个map[string]string
                    // TODO: 这里的逻辑与 rocketMQ 本身有出入，rocketMQ 在启动的时候，会先查询要消费的offset的位置，然后将这个offset的位置
                    //       设置进来。queryConsumerOffset
                    .setInitialPosition(PulsarUtil.parseSubPosition(SubscriptionInitialPosition.Latest))
                    .setReadCompacted(false)
                    .setReplicateSubscriptionState(false)
                    .build();

            //this.handleSubscribe(commandSubscribe);
        }

        MessageIdImpl messageId = (MessageIdImpl) MessageIdUtils.getMessageId(commitOffset);
        MessageIdData messageIdData = MessageIdData.newBuilder()
                .setLedgerId(messageId.getLedgerId())
                .setEntryId(messageId.getEntryId())
                .setPartition(messageId.getPartitionIndex())
                .build();
        BaseCommand baseCommand = Commands.newMessageCommand(consumerId, messageIdData, 0, null);
        ByteBufPair res = serializeCommandMessageWithSize(baseCommand, headersAndPayload);
        this.ctx.write(res, ctx.voidPromise());

//        getResult.setBufferTotalSize();
//        getResult.setMaxOffset();
//        getResult.setMinOffset();
//        getResult.setStatus();
//        getResult.setNextBeginOffset();
//        getResult.setSuggestPullingFromSlave();
//        getResult.setMsgCount4Commercial();

        return getResult;
    }

    public static ByteBufPair serializeCommandMessageWithSize(BaseCommand cmd, ByteBuf metadataAndPayload) {
        // / Wire format
        // [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
        //
        // metadataAndPayload contains from magic-number to the payload included

        int cmdSize = cmd.getSerializedSize();
        int totalSize = 4 + cmdSize + metadataAndPayload.readableBytes();
        int headersSize = 4 + 4 + cmdSize;

        ByteBuf headers = PulsarByteBufAllocator.DEFAULT.buffer(headersSize);
        headers.writeInt(totalSize); // External frame

        // Write cmd
        headers.writeInt(cmdSize);
        try {
            cmd.writeTo(ByteBufCodedOutputStream.get(headers));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ByteBufPair.get(headers, metadataAndPayload);
    }

    private long buildPulsarConsumerId(String... tags) {
        return Math.abs(Joiner.on("/").join(tags).hashCode());
    }

    private long buildPulsarProducerId(String... tags) {
        return Math.abs(Joiner.on("/").join(tags).hashCode());
    }

    static enum State {
        Start,
        Connected,
        Failed,
        Connecting;
    }
}
