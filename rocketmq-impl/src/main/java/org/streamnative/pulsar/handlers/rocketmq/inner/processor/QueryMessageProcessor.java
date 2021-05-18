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

package org.streamnative.pulsar.handlers.rocketmq.inner.processor;

import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.CompressionType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopEntryFormatter;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.MessageIdUtils;

/**
 * Query message processor.
 */
public class QueryMessageProcessor implements NettyRequestProcessor {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected final AsyncLoadingCache<String, ManagedLedger> ledgerCache;
    private final RocketMQBrokerController brokerController;
    private final RopEntryFormatter entryFormatter = new RopEntryFormatter();

    private static final String queryMessageLedgerName = "queryMessageProcessor_ledger";

    public QueryMessageProcessor(final RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.ledgerCache = Caffeine.newBuilder()
                .recordStats()
                .removalListener((String key, ManagedLedger value, RemovalCause removalCause) -> {
                    if (value != null) {
                        try {
                            value.close();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ManagedLedgerException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .buildAsync((key, executor1) -> {
                    return getManageLedger(key);
                });
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.QUERY_MESSAGE:
                return this.queryMessage(ctx, request);
            case RequestCode.VIEW_MESSAGE_BY_ID:
                return this.viewMessageById(ctx, request);
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
//        final RemotingCommand response =
//                RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);
//        final QueryMessageResponseHeader responseHeader =
//                (QueryMessageResponseHeader) response.readCustomHeader();
//        final QueryMessageRequestHeader requestHeader =
//                (QueryMessageRequestHeader) request
//                        .decodeCommandCustomHeader(QueryMessageRequestHeader.class);
//
//        response.setOpaque(request.getOpaque());
//
//        String isUniqueKey = request.getExtFields().get(MixAll.UNIQUE_MSG_QUERY_FLAG);
//        if (isUniqueKey != null && isUniqueKey.equals("true")) {
//            requestHeader.setMaxNum(this.brokerController.getServerConfig().getDefaultQueryMaxNum());
//        }
//
//        final QueryMessageResult queryMessageResult = null;
//                /*TODO this.brokerController.getMessageStore().queryMessage(requestHeader.getTopic(),
//                        requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(),
//                        requestHeader.getEndTimestamp())*/
//
//        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
//        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());
//
//        if (queryMessageResult.getBufferTotalSize() > 0) {
//            response.setCode(ResponseCode.SUCCESS);
//            response.setRemark(null);
//
//            try {
//                FileRegion fileRegion =
//                        new QueryMessageTransfer(response.encodeHeader(queryMessageResult
//                                .getBufferTotalSize()), queryMessageResult);
//                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
//                    @Override
//                    public void operationComplete(ChannelFuture future) throws Exception {
//                        queryMessageResult.release();
//                        if (!future.isSuccess()) {
//                            log.error("transfer query message by page cache failed, ", future.cause());
//                        }
//                    }
//                });
//            } catch (Throwable e) {
//                log.error("", e);
//                queryMessageResult.release();
//            }
//            return null;
//        }
//        response.setCode(ResponseCode.QUERY_NOT_FOUND);
//        response.setRemark("can not find message, maybe time range not correct");
//        return response;
        return null;
    }

    /**
     * query msg by origin id.
     *
     * @param cxt ChannelHandlerContext
     * @param request request
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand viewMessageById(ChannelHandlerContext cxt, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ViewMessageRequestHeader requestHeader =
                (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);
        response.setOpaque(request.getOpaque());
        MessageIdImpl messageId = MessageIdUtils.getMessageId(requestHeader.getOffset());
        log.info("viewMessageById {}, {} ,{}", request.getOpaque(),
                messageId.getLedgerId(), messageId.getEntryId());
        try {
            CompletableFuture<Entry> future = new CompletableFuture();
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) ledgerCache.get(queryMessageLedgerName).get(10,
                    TimeUnit.SECONDS);
            if (managedLedger != null) {
                // 通过offset来取出要开始消费的messageId的位置
                managedLedger.asyncReadEntry(new PositionImpl(messageId.getLedgerId(),
                        messageId.getEntryId()), new AsyncCallbacks.ReadEntryCallback() {
                    @Override
                    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }

                    @Override
                    public void readEntryComplete(Entry entry, Object ctx) {
                        future.complete(entry);
                    }
                }, null);
            }
            future.thenAccept((entry) -> {
                ByteBuffer msgBuf = getMessage(messageId, entry.getDataBuffer());
                if (msgBuf != null) {
                    try {
                        MessageExt messageExt = CommonUtils.decode(msgBuf,
                                messageId, true, false);
                        response.setBody(MessageDecoder.encode(messageExt, false));
                        response.setCode(ResponseCode.SUCCESS);
                        response.setRemark(null);
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Decode or encode msg has error! {}", e);
                        response.setCode(ResponseCode.SYSTEM_ERROR);
                        response.setRemark("Decode or encode msg has error :" + e.toString());
                    }
                }
                entry.release();
            }).exceptionally((ex) -> {
                log.error("query message from bookie or send message to client by id from bookie "
                        + "has exception e = {}", ex);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                if (ex instanceof ManagedLedgerException) {
                    response.setRemark("Read message error by ManagedLedger, " + requestHeader.getOffset());
                } else {
                    response.setRemark("Send message to client error, " + requestHeader.getOffset());
                }
                return null;
            }).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("query message by id has exception e = {}", e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("can not find message by the offset or query time out, " + requestHeader.getOffset());
        }
        return response;
    }

    private CompletableFuture<ManagedLedger> getManageLedger(String ledgerName) {

        CompletableFuture<ManagedLedger> mdFuture = new CompletableFuture<>();

        ManagedLedger managedLedger = null;
        try {
            ServiceConfiguration serviceConfig =
                    brokerController.getBrokerService().getPulsar().getConfig();
            ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
            managedLedgerConfig.setEnsembleSize(serviceConfig.getManagedLedgerDefaultEnsembleSize());
            managedLedgerConfig.setWriteQuorumSize(serviceConfig.getManagedLedgerDefaultWriteQuorum());
            managedLedgerConfig.setAckQuorumSize(serviceConfig.getManagedLedgerDefaultAckQuorum());

            managedLedgerConfig.setDigestType(serviceConfig.getManagedLedgerDigestType());
            managedLedgerConfig.setPassword(serviceConfig.getManagedLedgerPassword());
            managedLedgerConfig.setMaxUnackedRangesToPersist(serviceConfig.getManagedLedgerMaxUnackedRangesToPersist());
            managedLedgerConfig.setMaxUnackedRangesToPersistInZk(
                    serviceConfig.getManagedLedgerMaxUnackedRangesToPersistInZooKeeper());
            managedLedgerConfig.setMaxEntriesPerLedger(serviceConfig.getManagedLedgerMaxEntriesPerLedger());
            managedLedgerConfig.setMinimumRolloverTime(serviceConfig.getManagedLedgerMinLedgerRolloverTimeMinutes(),
                    TimeUnit.MINUTES);
            managedLedgerConfig.setMaximumRolloverTime(serviceConfig.getManagedLedgerMaxLedgerRolloverTimeMinutes(),
                    TimeUnit.MINUTES);
            managedLedgerConfig.setMaxSizePerLedgerMb(serviceConfig.getManagedLedgerMaxSizePerLedgerMbytes());
            managedLedgerConfig.setMetadataOperationsTimeoutSeconds(
                    serviceConfig.getManagedLedgerMetadataOperationsTimeoutSeconds());
            managedLedgerConfig.setReadEntryTimeoutSeconds(serviceConfig.getManagedLedgerReadEntryTimeoutSeconds());
            managedLedgerConfig.setAddEntryTimeoutSeconds(serviceConfig.getManagedLedgerAddEntryTimeoutSeconds());
            managedLedgerConfig.setMetadataEnsembleSize(serviceConfig.getManagedLedgerDefaultEnsembleSize());
            managedLedgerConfig.setUnackedRangesOpenCacheSetEnabled(
                    serviceConfig.isManagedLedgerUnackedRangesOpenCacheSetEnabled());
            managedLedgerConfig.setMetadataWriteQuorumSize(serviceConfig.getManagedLedgerDefaultWriteQuorum());
            managedLedgerConfig.setMetadataAckQuorumSize(serviceConfig.getManagedLedgerDefaultAckQuorum());
            managedLedgerConfig
                    .setMetadataMaxEntriesPerLedger(serviceConfig.getManagedLedgerCursorMaxEntriesPerLedger());
            managedLedgerConfig.setLedgerRolloverTimeout(serviceConfig.getManagedLedgerCursorRolloverTimeInSeconds());
            managedLedgerConfig.setAutoSkipNonRecoverableData(serviceConfig.isAutoSkipNonRecoverableData());
            managedLedgerConfig.setLazyCursorRecovery(serviceConfig.isLazyCursorRecovery());

            managedLedgerConfig
                    .setDeletionAtBatchIndexLevelEnabled(serviceConfig.isAcknowledgmentAtBatchIndexLevelEnabled());
            managedLedgerConfig
                    .setNewEntriesCheckDelayInMillis(serviceConfig.getManagedLedgerNewEntriesCheckDelayInMillis());
            managedLedger =
                    brokerController.getBrokerService().getManagedLedgerFactory().open(ledgerName
                            , managedLedgerConfig);
            mdFuture.complete(managedLedger);
        } catch (InterruptedException e) {
            e.printStackTrace();
            mdFuture.completeExceptionally(e);
        } catch (ManagedLedgerException e) {
            e.printStackTrace();
            mdFuture.completeExceptionally(e);
        }
        return mdFuture;
    }

    private ByteBuffer getMessage(MessageIdImpl messageId, ByteBuf headersAndPayload) {
        MessageMetadata msgMetadata = null;
        ByteBuf uncompressedPayload = null;
        if (!verifyChecksum(headersAndPayload, messageId)) {
            // discard message with checksum error
            return null;
        } else {
            try {
                msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
                if (msgMetadata != null) {
                    // uncompress decryptedPayload and release decryptedPayload-ByteBuf
                    uncompressedPayload = uncompressPayloadIfNeeded(messageId, msgMetadata,
                            headersAndPayload, true);
                }

                if (uncompressedPayload != null) {
                    return uncompressedPayload.nioBuffer();
                }
            } catch (Throwable t) {
                log.error("parseMessageMetadata has error e {}", t);
            }
        }
        return null;
    }

    private ByteBuf uncompressPayloadIfNeeded(MessageIdImpl messageId, MessageMetadata msgMetadata, ByteBuf payload,
            boolean checkMaxMessageSize) {
        CompressionType compressionType = msgMetadata.getCompression();
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        if (checkMaxMessageSize && payloadSize > ClientCnx.getMaxMessageSize()) {
            // payload size is itself corrupted since it cannot be bigger than the MaxMessageSize
            log.error("Got corrupted payload message size {} at {}", payloadSize,
                    messageId);
            return null;
        }
        try {
            ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
            payload.release();
            return uncompressedPayload;
        } catch (IOException e) {
            log.error("Failed to decompress message with {} at {}: {}", compressionType,
                    messageId, e.getMessage(), e);
            return null;
        }
    }

    private boolean verifyChecksum(ByteBuf headersAndPayload, MessageIdImpl messageId) {

        if (hasChecksum(headersAndPayload)) {
            int checksum = readChecksum(headersAndPayload);
            int computedChecksum = computeChecksum(headersAndPayload);
            if (checksum != computedChecksum) {
                log.error(
                        "Checksum mismatch for message at {}:{}. Received checksum: 0x{}, Computed checksum: 0x{}",
                        messageId.getLedgerId(), messageId.getEntryId(),
                        Long.toHexString(checksum), Integer.toHexString(computedChecksum));
                return false;
            }
        }

        return true;
    }
}
