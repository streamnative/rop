package com.tencent.tdmq.handlers.rocketmq.inner;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.BrokerServiceException.SubscriptionNotFoundException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicNotFoundException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandFlow;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetLastMessageId;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandRedeliverUnacknowledgedMessages;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSeek;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSend;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandUnsubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.FeatureFlags;
import org.apache.pulsar.common.api.proto.PulsarApi.KeySharedMeta;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.naming.Metadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.protocol.CommandUtils;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.shaded.com.google.protobuf.v241.GeneratedMessageLite;

@Slf4j
@Getter
public class RopServerCnx extends ChannelInboundHandlerAdapter implements TransportCnx {

    private RocketMQBrokerController brokerController;
    private ChannelHandlerContext ctx;
    private SocketAddress remoteAddress;
    private final BrokerService service;
    private final ConcurrentLongHashMap<Producer> producers;
    private final ConcurrentLongHashMap<Consumer> consumers;
    private State state;
    private volatile boolean isActive = true;
    private final int maxPendingSendRequests;
    private final int resumeReadsThreshold;
    private int pendingSendRequest = 0;
    private final String replicatorPrefix;
    private int nonPersistentPendingMessages = 0;
    private final int MaxNonPersistentPendingMessages;
    private Set<String> proxyRoles;
    private final int maxMessageSize;
    private boolean preciseDispatcherFlowControl;
    private boolean preciseTopicPublishRateLimitingEnable;
    private boolean encryptionRequireOnProducer;
    private volatile boolean autoReadDisabledRateLimiting = false;
    private FeatureFlags features;
    private volatile boolean autoReadDisabledPublishBufferLimiting = false;
    private static final AtomicLongFieldUpdater<RopServerCnx> MSG_PUBLISH_BUFFER_SIZE_UPDATER = AtomicLongFieldUpdater
            .newUpdater(RopServerCnx.class, "messagePublishBufferSize");
    private volatile long messagePublishBufferSize = 0L;
    private AuthenticationDataSource originalAuthData;
    private AuthenticationDataSource authenticationData;
    private String authRole = null;
    private String clientVersion = null;
    private String originalPrincipal = null;

    public RopServerCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        this.brokerController = brokerController;
        this.service = brokerController.getBrokerService();
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        this.state = State.Start;
        this.producers = new ConcurrentLongHashMap(8, 1);
        this.consumers = new ConcurrentLongHashMap(8, 1);
        this.replicatorPrefix = this.service.pulsar().getConfiguration().getReplicatorPrefix();
        this.MaxNonPersistentPendingMessages = this.service.pulsar().getConfiguration()
                .getMaxConcurrentNonPersistentMessagePerConnection();
        this.proxyRoles = this.service.pulsar().getConfiguration().getProxyRoles();
        this.maxMessageSize = this.service.pulsar().getConfiguration().getMaxMessageSize();
        this.maxPendingSendRequests = this.service.pulsar().getConfiguration()
                .getMaxPendingPublishRequestsPerConnection();
        this.resumeReadsThreshold = this.maxPendingSendRequests / 2;
        this.preciseDispatcherFlowControl = this.service.pulsar().getConfiguration().isPreciseDispatcherFlowControl();
        this.preciseTopicPublishRateLimitingEnable = this.service.pulsar().getConfiguration()
                .isPreciseTopicPublishRateLimiterEnable();
        this.encryptionRequireOnProducer = this.service.pulsar().getConfiguration().isEncryptionRequireOnProducer();
    }

    protected void handleSubscribe(CommandSubscribe subscribe) {
        Preconditions.checkArgument(this.state == State.Connected);
        long requestId = subscribe.getRequestId();
        long consumerId = subscribe.getConsumerId();
        TopicName topicName = this.validateTopicName(subscribe.getTopic(), requestId, subscribe);
        if (topicName != null) {
            String subscriptionName = subscribe.getSubscription();
            SubType subType = subscribe.getSubType();
            String consumerName = subscribe.getConsumerName();
            boolean isDurable = subscribe.getDurable();
            MessageIdImpl startMessageId = subscribe.hasStartMessageId() ? new BatchMessageIdImpl(
                    subscribe.getStartMessageId().getLedgerId(), subscribe.getStartMessageId().getEntryId(),
                    subscribe.getStartMessageId().getPartition(), subscribe.getStartMessageId().getBatchIndex())
                    : null;
            int priorityLevel = subscribe.hasPriorityLevel() ? subscribe.getPriorityLevel() : 0;
            boolean readCompacted = subscribe.getReadCompacted();
            Map<String, String> metadata = CommandUtils.metadataFromCommand(subscribe);
            InitialPosition initialPosition = subscribe.getInitialPosition();
            long startMessageRollbackDurationSec =
                    subscribe.hasStartMessageRollbackDurationSec() ? subscribe.getStartMessageRollbackDurationSec()
                            : -1L;
            SchemaData schema = null;
            boolean isReplicated =
                    subscribe.hasReplicateSubscriptionState() && subscribe.getReplicateSubscriptionState();
            boolean forceTopicCreation = subscribe.getForceTopicCreation();
            KeySharedMeta keySharedMeta = subscribe.hasKeySharedMeta() ? subscribe.getKeySharedMeta() : null;
            boolean isAuthorized = this.isTopicOperationAllowed(topicName, subscriptionName, TopicOperation.CONSUME);
            if (isAuthorized) {
                log.info("[{}] Subscribing on topic {} / {}",
                        new Object[]{this.remoteAddress, topicName, subscriptionName});
                try {
                    Metadata.validateMetadata(metadata);
                    Consumer existingConsumer = this.consumers.get(consumerId);
                    if (existingConsumer != null) {
                        log.info("[{}] Consumer with the same id is already created: consumerId={}, consumer={}",
                                new Object[]{this.remoteAddress, consumerId, existingConsumer});
                        return;
                    }

                    boolean createTopicIfDoesNotExist =
                            forceTopicCreation && this.service.isAllowAutoTopicCreation(topicName.toString());
                    existingConsumer = this.service.getTopic(topicName.toString(), createTopicIfDoesNotExist)
                            .thenCompose((optTopic) -> {
                                if (!optTopic.isPresent()) {
                                    return FutureUtil
                                            .failedFuture(new TopicNotFoundException("Topic does not exist"));
                                } else {
                                    Topic topic = optTopic.get();
                                    boolean rejectSubscriptionIfDoesNotExist = isDurable && !this.service
                                            .isAllowAutoSubscriptionCreation(topicName.toString()) && !topic
                                            .getSubscriptions().containsKey(subscriptionName);
                                    if (rejectSubscriptionIfDoesNotExist) {
                                        return FutureUtil.failedFuture(
                                                new SubscriptionNotFoundException("Subscription does not exist"));
                                    } else {
                                        return topic.subscribe(this, subscriptionName, consumerId, subType,
                                                priorityLevel, consumerName, isDurable, startMessageId, metadata,
                                                readCompacted, initialPosition, startMessageRollbackDurationSec,
                                                isReplicated, keySharedMeta);
                                    }
                                }
                            }).get();
                    this.consumers.putIfAbsent(consumerId, existingConsumer);
                } catch (Exception e) {
                    log.warn("handleSubscribe error {}: {}", consumerId, e);
                    if (this.consumers.containsKey(consumerId)) {
                        try {
                            this.consumers.get(consumerId).close();
                        } catch (BrokerServiceException brokerServiceException) {
                        }
                        this.consumers.remove(consumerId);
                    }
                }
            } else {
                String msgx = "Client is not authorized to subscribe";
                log.warn("[{}] {} with role {}", new Object[]{this.remoteAddress, msgx, this.getPrincipal()});
            }
        }
    }


    protected void handleProducer(CommandProducer cmdProducer) {
        Preconditions.checkArgument(this.state == State.Connected);
        long producerId = cmdProducer.getProducerId();
        long requestId = cmdProducer.getRequestId();
        String producerName = cmdProducer.hasProducerName() ? cmdProducer.getProducerName()
                : this.service.generateUniqueProducerName();
        long epoch = cmdProducer.getEpoch();
        boolean userProvidedProducerName = cmdProducer.getUserProvidedProducerName();
        boolean isEncrypted = cmdProducer.getEncrypted();
        Map<String, String> metadata = CommandUtils.metadataFromCommand(cmdProducer);

        TopicName topicName = this.validateTopicName(cmdProducer.getTopic(), requestId, cmdProducer);
        if (topicName != null) {
            boolean isAuthorized = isTopicOperationAllowed(topicName, TopicOperation.PRODUCE);
            Producer existingProducer = this.producers.get(producerId);
            if (existingProducer != null) {
                log.info("[{}] Producer with the same id is already created: producerId={}, producer={}",
                        new Object[]{this.remoteAddress, producerId, existingProducer});
                return;
            }

            log.info("[{}][{}] Creating producer. producerId={}",
                    new Object[]{this.remoteAddress, topicName, producerId});
            this.service.getOrCreateTopic(topicName.toString()).thenAccept((topic) -> {
                if (topic.isBacklogQuotaExceeded(producerName)) {
                    this.producers.remove(producerId);
                } else if ((topic.isEncryptionRequired() || this.encryptionRequireOnProducer)
                        && !isEncrypted) {
                    String msg = String.format("Encryption is required in %s", topicName);
                    log.warn("[{}] {}", this.remoteAddress, msg);
                    this.producers.remove(producerId);
                } else {
                    try {
                        this.disableTcpNoDelayIfNeeded(topicName.toString(), producerName);
                        Producer producer = new Producer(topic, this, producerId, producerName,
                                this.getPrincipal(), isEncrypted, metadata, null, epoch,
                                userProvidedProducerName);
                        topic.addProducer(producer);
                        if (!this.isActive()) {
                            producer.closeNow(true);
                            log.info("[{}] Cleared producer created after connection was closed: {}",
                                    this.remoteAddress, producer);
                        }
                    } catch (Exception ex) {
                        log.error("[{}] Failed to add producer to topic {}: {}",
                                new Object[]{this.remoteAddress, topicName, ex.getMessage()});
                        this.producers.remove(producerId);
                    }
                }
            }).exceptionally((exception) -> {
                Throwable cause = exception.getCause();
                if (cause instanceof NoSuchElementException) {
                    cause = new TopicNotFoundException("Topic Not Found.");
                }

                if (!(cause instanceof ServiceUnitNotReadyException)) {
                    log.error("[{}] Failed to create topic {}, producerId={}",
                            new Object[]{this.remoteAddress, topicName, producerId, exception});
                }
                this.producers.remove(producerId);
                return null;
            });
        } else {
            log.warn("Topic [{}] isn't exists.", topicName);
        }
    }

    protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
        Preconditions.checkArgument(this.state == State.Connected);
        Producer producer = this.producers.get(send.getProducerId());
        if (producer != null) {
            if (log.isDebugEnabled()) {
                this.printSendCommandDebug(send, headersAndPayload);
            }

            if (producer.isNonPersistentTopic()) {
                if (this.nonPersistentPendingMessages > this.MaxNonPersistentPendingMessages) {
                    producer.recordMessageDrop(send.getNumMessages());
                    return;
                }
                ++this.nonPersistentPendingMessages;
            }

            this.startSendOperation(producer, headersAndPayload.readableBytes(), send.getNumMessages());
            if (send.hasHighestSequenceId() && send.getSequenceId() <= send.getHighestSequenceId()) {
                producer.publishMessage(send.getProducerId(), send.getSequenceId(), send.getHighestSequenceId(),
                        headersAndPayload, (long) send.getNumMessages(), send.getIsChunk());
            } else {
                producer.publishMessage(send.getProducerId(), send.getSequenceId(), headersAndPayload,
                        send.getNumMessages(), send.getIsChunk());
            }
        } else {
            log.warn("[{}] Producer had already been closed: {}", this.remoteAddress, send.getProducerId());
        }
    }

    private void printSendCommandDebug(CommandSend send, ByteBuf headersAndPayload) {
        headersAndPayload.markReaderIndex();
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        headersAndPayload.resetReaderIndex();
        if (log.isDebugEnabled()) {
            log.debug(
                    "[{}] Received send message request. producer: {}:{} {}:{} size: {}, partition key is: {}, ordering key is {}",
                    new Object[]{this.remoteAddress, send.getProducerId(), send.getSequenceId(),
                            msgMetadata.getProducerName(), msgMetadata.getSequenceId(),
                            headersAndPayload.readableBytes(), msgMetadata.getPartitionKey(),
                            msgMetadata.getOrderingKey()});
        }

        msgMetadata.recycle();
    }

    protected void handleAck(CommandAck ack) {
        Preconditions.checkArgument(this.state == State.Connected);
        Consumer consumer = this.consumers.get(ack.getConsumerId());
        long requestId = ack.getRequestId();
        boolean hasRequestId = ack.hasRequestId();
        long consumerId = ack.getConsumerId();
        if (consumer != null) {
            consumer.messageAcked(ack).thenRun(() -> {
                if (hasRequestId) {
                    this.ctx.writeAndFlush(
                            Commands.newAckResponse(requestId, (ServerError) null, (String) null, consumerId));
                }
            }).exceptionally((e) -> {
                if (hasRequestId) {
                    this.ctx.writeAndFlush(
                            Commands.newAckResponse(requestId, BrokerServiceException.getClientErrorCode(e),
                                    e.getMessage(), consumerId));
                }
                return null;
            });
        }
    }

    protected void handleFlow(CommandFlow flow) {
        Preconditions.checkArgument(this.state == State.Connected);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received flow from consumer {} permits: {}",
                    new Object[]{this.remoteAddress, flow.getConsumerId(), flow.getMessagePermits()});
        }

        Consumer consumer = this.consumers.get(flow.getConsumerId());
        if (consumer != null) {
            consumer.flowPermits(flow.getMessagePermits());
        } else {
            log.info("[{}] Couldn't find consumer {}", this.remoteAddress, flow.getConsumerId());
        }
    }

    protected void handleRedeliverUnacknowledged(CommandRedeliverUnacknowledgedMessages redeliver) {
        Preconditions.checkArgument(this.state == State.Connected);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received Resend Command from consumer {} ", this.remoteAddress, redeliver.getConsumerId());
        }
        Consumer consumer = this.consumers.get(redeliver.getConsumerId());
        if (consumer != null) {
            if (redeliver.getMessageIdsCount() > 0 && Subscription.isIndividualAckMode(consumer.subType())) {
                consumer.redeliverUnacknowledgedMessages(redeliver.getMessageIdsList());
            } else {
                consumer.redeliverUnacknowledgedMessages();
            }
        }

    }

    protected void handleUnsubscribe(CommandUnsubscribe unsubscribe) {
        Preconditions.checkArgument(this.state == State.Connected);
        Consumer consumer = this.consumers.get(unsubscribe.getConsumerId());
        if (consumer != null) {
            consumer.doUnsubscribe(unsubscribe.getRequestId());
        } else {
            log.warn("handleUnsubscribe can't find consumer[{}].", unsubscribe.getConsumerId());
        }
    }

    protected void handleSeek(CommandSeek seek) {
        Preconditions.checkArgument(this.state == State.Connected);
        long requestId = seek.getRequestId();
        Consumer consumer = this.consumers.get(seek.getConsumerId());
        if (!seek.hasMessageId() && !seek.hasMessagePublishTime()) {
            log.warn("Message id and message publish time were not present for ConsumerId = [{}].",
                    seek.getConsumerId());
        } else {
            boolean consumerCreated = (consumer != null);
            Subscription subscription;
            if (consumerCreated && seek.hasMessageId()) {
                subscription = consumer.getSubscription();
                MessageIdData msgIdData = seek.getMessageId();
                long[] ackSet = null;
                if (msgIdData.getAckSetCount() > 0) {
                    ackSet = SafeCollectionUtils.longListToArray(msgIdData.getAckSetList());
                }

                Position position = new PositionImpl(msgIdData.getLedgerId(), msgIdData.getEntryId(), ackSet);
                subscription.resetCursor(position).thenRun(() -> {
                    log.info("[{}] [{}][{}] Reset subscription to message id {}",
                            new Object[]{this.remoteAddress, subscription.getTopic().getName(), subscription.getName(),
                                    position});
                }).exceptionally((ex) -> {
                    log.warn("[{}][{}] Failed to reset subscription: {}",
                            new Object[]{this.remoteAddress, subscription, ex.getMessage(), ex});
                    return null;
                });
            } else if (consumerCreated && seek.hasMessagePublishTime()) {
                subscription = consumer.getSubscription();
                long timestamp = seek.getMessagePublishTime();
                subscription.resetCursor(timestamp).thenRun(() -> {
                    log.info("[{}] [{}][{}] Reset subscription to publish time {}",
                            new Object[]{this.remoteAddress, subscription.getTopic().getName(), subscription.getName(),
                                    timestamp});
                }).exceptionally((ex) -> {
                    log.warn("[{}][{}] Failed to reset subscription: {}",
                            new Object[]{this.remoteAddress, subscription, ex.getMessage(), ex});
                    return null;
                });
            } else {
                log.warn("Failed to get consumer [{}].", seek.getConsumerId());
            }

        }
    }

    protected void handleCloseProducer(CommandCloseProducer closeProducer) {
        Preconditions.checkArgument(this.state == State.Connected);
        long producerId = closeProducer.getProducerId();
        Producer producer = this.producers.get(producerId);
        if (producer != null) {
            producer.close(true).thenAccept((v) -> {
                log.info("[{}][{}] Closed producer on cnx {}. producerId={}",
                        new Object[]{producer.getTopic(), producer.getProducerName(), this.remoteAddress, producerId});
                this.producers.remove(producerId, producer);
            });
        }
    }

    protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
        Preconditions.checkArgument(this.state == State.Connected);
        long consumerId = closeConsumer.getConsumerId();
        Consumer consumer = this.consumers.get(consumerId);
        if (consumer != null) {
            try {
                consumer.close();
                this.consumers.remove(consumerId, consumer);
                log.info("[{}] Closed consumer, consumerId={}", this.remoteAddress, consumerId);
            } catch (Exception ex) {
                log.warn("[{]] Error closing consumer {} : {}", new Object[]{this.remoteAddress, consumer, ex});
            }
        }
    }

    protected MessageIdData handleGetLastMessageId(CommandGetLastMessageId getLastMessageId) {
        Preconditions.checkArgument(this.state == State.Connected);
        Consumer consumer = this.consumers.get(getLastMessageId.getConsumerId());
        if (consumer != null) {
            long requestId = getLastMessageId.getRequestId();
            Topic topic = consumer.getSubscription().getTopic();
            Position position = topic.getLastPosition();
            int partitionIndex = TopicName.getPartitionIndex(topic.getName());
            Position markDeletePosition = null;
            if (consumer.getSubscription() instanceof PersistentSubscription) {
                markDeletePosition = ((PersistentSubscription) consumer.getSubscription()).getCursor()
                        .getMarkDeletedPosition();
            }
            return this
                    .getLargestBatchIndexWhenPossible(topic, (PositionImpl) position, (PositionImpl) markDeletePosition,
                            partitionIndex, requestId, consumer.getSubscription().getName());
        } else {
            return null;
        }
    }

    private MessageIdData getLargestBatchIndexWhenPossible(Topic topic, PositionImpl position,
            PositionImpl markDeletePosition,
            int partitionIndex, long requestId, String subscriptionName) {
        PersistentTopic persistentTopic = (PersistentTopic) topic;
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        MessageIdData markDeleteMessageId = null;
        MessageIdData messageId = null;
        if (position.getEntryId() == -1L) {
            messageId = MessageIdData.newBuilder().setLedgerId(position.getLedgerId())
                    .setEntryId(position.getEntryId()).setPartition(partitionIndex).build();
            if (null != markDeletePosition) {
                markDeleteMessageId = MessageIdData.newBuilder().setLedgerId(markDeletePosition.getLedgerId())
                        .setEntryId(markDeletePosition.getEntryId()).build();
            }
        } else {
            final CompletableFuture<Entry> entryFuture = new CompletableFuture();
            ml.asyncReadEntry(position, new ReadEntryCallback() {
                public void readEntryComplete(Entry entry, Object ctx) {
                    entryFuture.complete(entry);
                }

                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                    entryFuture.completeExceptionally(exception);
                }
            }, null);
            CompletableFuture<Integer> batchSizeFuture = entryFuture.thenApply((entry) -> {
                MessageMetadata metadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                int batchSize = metadata.getNumMessagesInBatch();
                entry.release();
                return metadata.hasNumMessagesInBatch() ? batchSize : -1;
            });

            try {
                int batchSize = batchSizeFuture.get();
                int largestBatchIndex = batchSize > 0 ? batchSize - 1 : -1;
                messageId = MessageIdData.newBuilder().setLedgerId(position.getLedgerId())
                        .setEntryId(position.getEntryId()).setPartition(partitionIndex)
                        .setBatchIndex(largestBatchIndex).build();

                if (null != markDeletePosition) {
                    markDeleteMessageId = MessageIdData.newBuilder().setLedgerId(markDeletePosition.getLedgerId())
                            .setEntryId(markDeletePosition.getEntryId()).build();
                }
            } catch (Exception e) {
                log.error("Failed to get batch size for entry[subscriber=[{}],requestId={}].", subscriptionName,
                        requestId);
            }
        }
        return markDeleteMessageId;
    }

    protected List<String> handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace) {
        long requestId = commandGetTopicsOfNamespace.getRequestId();
        String namespace = commandGetTopicsOfNamespace.getNamespace();
        Mode mode = commandGetTopicsOfNamespace.getMode();
        NamespaceName namespaceName = NamespaceName.get(namespace);
        try {
            List<String> topicResult = this.service.pulsar().getNamespaceService().getListOfTopics(namespaceName, mode)
                    .get();
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received CommandGetTopicsOfNamespace for namespace [//{}] by {}, size:{}",
                        new Object[]{this.remoteAddress, namespace, requestId, topicResult.size()});
            }
            return topicResult;
        } catch (Exception e) {
            log.warn("[{}] Error GetTopicsOfNamespace for namespace [//{}] by {}",
                    new Object[]{this.remoteAddress, namespace, requestId});
            return Collections.emptyList();
        }
    }

    public void closeProducer(Producer producer) {
        this.safelyRemoveProducer(producer);
        this.close();
    }

    public void closeConsumer(Consumer consumer) {
        this.safelyRemoveConsumer(consumer);
        this.close();
    }

    @Override
    public Promise<Void> newPromise() {
        return null;
    }

    @Override
    public boolean hasHAProxyMessage() {
        return false;
    }

    @Override
    public HAProxyMessage getHAProxyMessage() {
        return null;
    }

    public void removedConsumer(Consumer consumer) {
        this.safelyRemoveConsumer(consumer);
    }

    public void removedProducer(Producer producer) {
        this.safelyRemoveProducer(producer);
    }

    private void safelyRemoveProducer(Producer producer) {
        long producerId = producer.getProducerId();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed producer: producerId={}, producer={}",
                    new Object[]{this.remoteAddress, producerId, producer});
        }

        Producer tmpProducer = this.producers.get(producerId);
        if (tmpProducer != null && tmpProducer == producer) {
            this.consumers.remove(producerId, producer);
        }

    }

    private void safelyRemoveConsumer(Consumer consumer) {
        long consumerId = consumer.consumerId();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed consumer: consumerId={}, consumer={}",
                    new Object[]{this.remoteAddress, consumerId, consumer});
        }

        Consumer tmpCunsumer = this.consumers.get(consumerId);
        if (tmpCunsumer != null && tmpCunsumer == consumer) {
            this.consumers.remove(consumerId, tmpCunsumer);
        }
    }

    public void startSendOperation(Producer producer, int msgSize, int numMessages) {
        MSG_PUBLISH_BUFFER_SIZE_UPDATER.getAndAdd(this, (long) msgSize);
        boolean isPublishRateExceeded = false;
        if (this.preciseTopicPublishRateLimitingEnable) {
            boolean isPreciseTopicPublishRateExceeded = producer.getTopic()
                    .isTopicPublishRateExceeded(numMessages, msgSize);
            if (isPreciseTopicPublishRateExceeded) {
                producer.getTopic().disableCnxAutoRead();
                return;
            }

            isPublishRateExceeded = producer.getTopic().isBrokerPublishRateExceeded();
        } else {
            isPublishRateExceeded = producer.getTopic().isPublishRateExceeded();
        }

        if (++this.pendingSendRequest == this.maxPendingSendRequests || isPublishRateExceeded) {
            this.ctx.channel().config().setAutoRead(false);
            this.autoReadDisabledRateLimiting = isPublishRateExceeded;
        }

        if (this.service.isReachMessagePublishBufferThreshold()) {
            this.ctx.channel().config().setAutoRead(false);
            this.autoReadDisabledPublishBufferLimiting = true;
        }

    }

    @Override
    public String getClientVersion() {
        return null;
    }

    @Override
    public SocketAddress clientAddress() {
        return remoteAddress;
    }

    @Override
    public BrokerService getBrokerService() {
        return service;
    }

    @Override
    public PulsarCommandSender getCommandSender() {
        return null;
    }

    @Override
    public boolean isBatchMessageCompatibleVersion() {
        return true;
    }

    @Override
    public String getAuthRole() {
        return authRole;
    }

    public String getPrincipal() {
        return originalPrincipal != null ? originalPrincipal : authRole;
    }

    @Override
    public AuthenticationDataSource getAuthenticationData() {
        return originalAuthData != null ? originalAuthData : authenticationData;
    }

    @Override
    public boolean isWritable() {
        return ctx.channel().isWritable();
    }

    public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
        MSG_PUBLISH_BUFFER_SIZE_UPDATER.getAndAdd(this, (long) (-msgSize));
        if (--this.pendingSendRequest == this.resumeReadsThreshold) {
            this.ctx.channel().config().setAutoRead(true);
            this.ctx.read();
        }

        if (isNonPersistentTopic) {
            --this.nonPersistentPendingMessages;
        }

    }

    public void cancelPublishRateLimiting() {
        if (this.autoReadDisabledRateLimiting) {
            this.autoReadDisabledRateLimiting = false;
        }

    }

    public void cancelPublishBufferLimiting() {
        if (this.autoReadDisabledPublishBufferLimiting) {
            this.autoReadDisabledPublishBufferLimiting = false;
        }

    }

    @Override
    public void disableCnxAutoRead() {

    }

    @Override
    public void enableCnxAutoRead() {

    }

    @Override
    public void execute(Runnable runnable) {

    }

    private void disableTcpNoDelayIfNeeded(String topic, String producerName) {
        if (producerName != null && producerName.startsWith(this.replicatorPrefix)) {
            try {
                if (this.ctx.channel().config().getOption(ChannelOption.TCP_NODELAY)) {
                    this.ctx.channel().config().setOption(ChannelOption.TCP_NODELAY, false);
                }
            } catch (Throwable ex) {
                log.warn("[{}] [{}] Failed to remove TCP no-delay property on client cnx {}",
                        new Object[]{topic, producerName, this.ctx.channel()});
            }
        }

    }

    private TopicName validateTopicName(String topic, long requestId, GeneratedMessageLite requestCommand) {
        try {
            return TopicName.get(topic);
        } catch (Throwable ex) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to parse topic name '{}'", new Object[]{this.remoteAddress, topic, ex});
            }
            return null;
        }
    }

    private boolean isTopicOperationAllowed(TopicName topicName, TopicOperation operation) {
        return true;
    }

    private boolean isTopicOperationAllowed(TopicName topicName, String subscriptionName,
            TopicOperation operation) {
        return true;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddress = ctx.channel().remoteAddress();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        isActive = false;
        log.info("Closed connection from {}", remoteAddress);

        // Connection is gone, close the producers immediately
        producers.values().forEach((producer) -> producer.closeNow(true));
        consumers.values().forEach((consumer) -> {
            try {
                consumer.close();
            } catch (BrokerServiceException e) {
                log.warn("Consumer {} was already closed: {}", consumer, e);
            }
        });
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
        ctx.close();
    }

    protected void close() {
        this.ctx.close();
    }

    static enum State {
        Start,
        Connected,
        Failed,
        Connecting;
    }
}
