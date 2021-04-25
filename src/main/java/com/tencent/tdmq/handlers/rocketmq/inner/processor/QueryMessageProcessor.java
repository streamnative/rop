/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tdmq.handlers.rocketmq.inner.processor;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.inner.format.RopEntryFormatter;
import com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.broker.pagecache.OneMessageTransfer;
import org.apache.rocketmq.broker.pagecache.QueryMessageTransfer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.zookeeper.data.Stat;

public class QueryMessageProcessor implements NettyRequestProcessor {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final RocketMQBrokerController brokerController;

    protected final AsyncLoadingCache<String, ManagedLedger> ledgerCache;

    private final RopEntryFormatter entryFormatter = new RopEntryFormatter();

    private final String QUERY_MESSAGE_LEDGER_NAME = "queryMessageProcessor_ledger";

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
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);
        final QueryMessageResponseHeader responseHeader =
                (QueryMessageResponseHeader) response.readCustomHeader();
        final QueryMessageRequestHeader requestHeader =
                (QueryMessageRequestHeader) request
                        .decodeCommandCustomHeader(QueryMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        String isUniqueKey = request.getExtFields().get(MixAll.UNIQUE_MSG_QUERY_FLAG);
        if (isUniqueKey != null && isUniqueKey.equals("true")) {
            requestHeader.setMaxNum(this.brokerController.getServerConfig().getDefaultQueryMaxNum());
        }

        final QueryMessageResult queryMessageResult = null
                /*TODO this.brokerController.getMessageStore().queryMessage(requestHeader.getTopic(),
                        requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(),
                        requestHeader.getEndTimestamp())*/;
        assert queryMessageResult != null;

        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());

        if (queryMessageResult.getBufferTotalSize() > 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            try {
                FileRegion fileRegion =
                        new QueryMessageTransfer(response.encodeHeader(queryMessageResult
                                .getBufferTotalSize()), queryMessageResult);
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        queryMessageResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer query message by page cache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                queryMessageResult.release();
            }
            return null;
        }
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("can not find message, maybe time range not correct");
        return response;
    }

    public RemotingCommand viewMessageById(ChannelHandlerContext channelHandlerContext, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ViewMessageRequestHeader requestHeader =
                (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);
        response.setOpaque(request.getOpaque());
        MessageIdImpl messageId = MessageIdUtils.getMessageId(requestHeader.getOffset());
        log.debug("viewMessageById {}, {} ,{}",request.getOpaque(),
                messageId.getLedgerId(), messageId.getEntryId());
        /*TODO this.brokerController.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset())*/;
        try {
            CompletableFuture future = new CompletableFuture();
            ManagedLedgerImpl  managedLedger = (ManagedLedgerImpl)ledgerCache.get(QUERY_MESSAGE_LEDGER_NAME).get(10,
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
                        try {
                            ByteBuffer byteBuffer = entryFormatter
                                        .decodePulsarMessageResBuffer(entry.getData());
                            channelHandlerContext.channel().writeAndFlush(byteBuffer).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture oCfuture) throws Exception {
                                    if (!oCfuture.isSuccess()) {
                                        log.error("Transfer one message from page cache failed, ", oCfuture.cause());
                                        future.complete(new Exception(oCfuture.cause()));
                                    } else {
                                        future.complete(null);
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("", e);
                            future.completeExceptionally(new Exception(String.valueOf(ResponseCode.SYSTEM_ERROR)));
                        } finally {
                            if (entry != null) {
                                entry.release();
                            }
                        }
                    }
                }, null);
            }
            future.thenAccept((v)->{
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
            }).exceptionally((ex)->{
                log.error("query or send message by id from bookie has exception e = {}",ex);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                if (ex instanceof ManagedLedgerException) {
                    response.setRemark("Read message error by ManagedLedger, " + requestHeader.getOffset());
                } else {
                    response.setRemark("Send message to client error, " + requestHeader.getOffset());
                }
                return null;
            }).get(30,TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("query message by id has exception e = {}",e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("can not find message by the offset or query time out, " + requestHeader.getOffset());
        }
        return response;
    }

    private CompletableFuture<ManagedLedger> getManageLedger(String ledgerName) {

        CompletableFuture<ManagedLedger> mdFuture = new CompletableFuture<>();

        ManagedLedger managedLedger = null;
        try {
            managedLedger =
                    brokerController.getBrokerService().getManagedLedgerFactory().open(ledgerName);
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
}
