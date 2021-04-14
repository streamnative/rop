package com.tencent.tdmq.handlers.rocketmq.inner.pulsar;

import com.tencent.tdmq.handlers.rocketmq.inner.RopServerCnx;
import io.netty.channel.ChannelPromise;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

@Slf4j
public class RopPulsarCommandSender implements PulsarCommandSender {

    private final RopServerCnx cnx;
    private final ConcurrentLongHashMap<CompletableFuture<PutMessageResult>> producerResultMap;
    private final SystemClock systemClock = new SystemClock();

    public RopPulsarCommandSender(RopServerCnx cnx) {
        this.cnx = cnx;
        producerResultMap = new ConcurrentLongHashMap<>(1024);
    }

    public void put(long seqId, CompletableFuture<PutMessageResult> putMessageFuture) {
        CompletableFuture<PutMessageResult> oldResult = producerResultMap.put(seqId, putMessageFuture);
        if (oldResult != null) {
            log.warn("exists duplicated message sequenceId[{}].", seqId);
            PutMessageStatus status = PutMessageStatus.UNKNOWN_ERROR;
            AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            PutMessageResult result = new PutMessageResult(status, temp);
            oldResult.complete(result);
        }
    }

    @Override
    public void sendPartitionMetadataResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
    }

    @Override
    public void sendPartitionMetadataResponse(int partitions, long requestId) {
    }

    @Override
    public void sendSuccessResponse(long requestId) {
    }

    @Override
    public void sendErrorResponse(long requestId, PulsarApi.ServerError error, String message) {
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion) {
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
            SchemaVersion schemaVersion) {
    }

    @Override
    public void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId,
            long entryId) {
        CompletableFuture<PutMessageResult> resultFuture = this.producerResultMap.remove(sequenceId);
        if (resultFuture != null) {
            PutMessageStatus status = PutMessageStatus.PUT_OK;
            AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.PUT_OK, highestId, 0,
                    null, systemClock.now(), ledgerId, entryId);
            PutMessageResult result = new PutMessageResult(status, temp);
            resultFuture.complete(result);
        }
    }

    @Override
    public void sendSendError(long producerId, long sequenceId, PulsarApi.ServerError error, String errorMsg) {
        CompletableFuture<PutMessageResult> resultFuture = this.producerResultMap.remove(sequenceId);
        if (resultFuture != null) {
            PutMessageStatus status = PutMessageStatus.UNKNOWN_ERROR;
            AppendMessageResult temp = new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            PutMessageResult result = new PutMessageResult(status, temp);
            resultFuture.complete(result);
        }
    }

    @Override
    public void sendGetTopicsOfNamespaceResponse(List<String> topics, long requestId) {
    }

    @Override
    public void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {
    }

    @Override
    public void sendGetSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage) {
    }

    @Override
    public void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {
    }

    @Override
    public void sendGetOrCreateSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage) {
    }

    @Override
    public void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize) {
    }

    @Override
    public void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
            PulsarApi.CommandLookupTopicResponse.LookupType response, long requestId, boolean proxyThroughServiceUrl) {
    }

    @Override
    public void sendLookupResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
    }

    @Override
    public void sendActiveConsumerChange(long consumerId, boolean isActive) {
    }

    @Override
    public void sendSuccess(long requestId) {
    }

    @Override
    public void sendError(long requestId, PulsarApi.ServerError error, String message) {
    }

    @Override
    public void sendReachedEndOfTopic(long consumerId) {

    }

    @Override
    public ChannelPromise sendMessagesToConsumer(long consumerId, String topicName, Subscription subscription,
            int partitionIdx, List<Entry> entries, EntryBatchSizes batchSizes, EntryBatchIndexesAcks batchIndexesAcks,
            RedeliveryTracker redeliveryTracker) {
        return null;
    }
}
