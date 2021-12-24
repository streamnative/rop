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

import static org.apache.rocketmq.common.protocol.heartbeat.ConsumeType.CONSUME_PASSIVELY;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_INNER_CLIENT_ADDRESS;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_INNER_REMOTE_CLIENT_TAG;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RopClientChannelCnx;
import org.streamnative.pulsar.handlers.rocketmq.inner.RopMessage;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.BatchMessageTransfer;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.ConsumerGroupInfo;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.RopGetMessageResult;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopMessageFilter;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.inner.pulsar.PulsarMessageStore;
import org.streamnative.pulsar.handlers.rocketmq.inner.trace.TraceContext;
import org.streamnative.pulsar.handlers.rocketmq.inner.trace.TraceManager;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;

/**
 * Pull message processor.
 */
@Slf4j
public class PullMessageProcessor implements NettyRequestProcessor {

    private final RocketMQBrokerController brokerController;
    private List<ConsumeMessageHook> consumeMessageHookList;

    public PullMessageProcessor(final RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    private boolean isBrokerProxyRequest(RemotingCommand request) {
        return request.getExtFields() != null && request.getExtFields().containsKey(ROP_INNER_REMOTE_CLIENT_TAG);
    }

    protected PulsarMessageStore getServerCnxMsgStore(Channel channel, PullMessageRequestHeader requestHeader,
            RemotingCommand request) {
        try {
            String groupName = isBrokerProxyRequest(request) ? request.getExtFields().get(ROP_INNER_REMOTE_CLIENT_TAG)
                    : requestHeader.getConsumerGroup();
            ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager()
                    .getConsumerGroupInfo(groupName);
            RopClientChannelCnx channelCnx = (RopClientChannelCnx) consumerGroupInfo
                    .getChannelInfoTable().get(channel);
            return channelCnx.getServerCnx();
        } catch (Exception e) {
            log.info("PullMessageProcessor get client [request={}] channel context error,"
                            + " wait client register consumer info.",
                    request);
            return null;
        }
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
            RemotingCommand request) throws RemotingCommandException {
        final PullMessageRequestHeader requestHeader =
                (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        if (isBrokerProxyRequest(request)) {
            String brokerProxyGroupName = request.getExtFields().get(ROP_INNER_REMOTE_CLIENT_TAG);
            this.brokerController.getConsumerManager()
                    .registerProxyRequestConsumer(brokerProxyGroupName, requestHeader.getConsumerGroup(),
                            this.brokerController, ctx);
        } else {
            if (Objects.isNull(this.brokerController.getConsumerManager()
                    .getConsumerGroupInfo(requestHeader.getConsumerGroup())) && Objects.nonNull(ctx)) {
                RopClientChannelCnx clientChannelCnx = new RopClientChannelCnx(brokerController, ctx, Strings.EMPTY,
                        LanguageCode.JAVA, 0);
                this.brokerController.getConsumerManager()
                        .registerConsumer(requestHeader.getConsumerGroup(), clientChannelCnx, CONSUME_PASSIVELY,
                                CLUSTERING,
                                ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, Collections.emptySet(),
                                true);
            }
        }
        return this.processRequest(ctx.channel(), requestHeader, request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, PullMessageRequestHeader requestHeader,
            RemotingCommand request, boolean brokerAllowSuspend)
            throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        response.setOpaque(request.getOpaque());

        if (log.isDebugEnabled()) {
            log.debug("receive PullMessage request command, {}", request);
        }

        if (!PermName.isReadable(this.brokerController.getServerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[" //+ this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] pulling message is forbidden"));
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager()
                        .findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(
                    String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(),
                            FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager()
                .selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.error("the topic {} not exist, consumer: {}", requestHeader.getTopic(),
                    RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(),
                    FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return response;
        }

        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String
                    .format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                            requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(),
                            channel.remoteAddress());
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        boolean isBroadcasting = false;
        SubscriptionData subscriptionData = null;
        if (hasSubscriptionFlag) {
            try {
                subscriptionData = FilterAPI.build(
                        requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType()
                );
            } catch (Exception e) {
                log.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getSubscription(),
                        requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }

            ConsumerGroupInfo consumerGroupInfo =
                    this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
            isBroadcasting =
                    consumerGroupInfo != null && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING;
            if (consumerGroupInfo != null && !subscriptionGroupConfig.isConsumeBroadcastEnable()
                    && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup()
                        + "] can not consume by broadcast way");
                return response;
            }
        } else {
            ConsumerGroupInfo consumerGroupInfo =
                    this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
            if (null == consumerGroupInfo) {
                log.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark(
                        "the consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }
            isBroadcasting = consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING;

            if (!subscriptionGroupConfig.isConsumeBroadcastEnable()
                    && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup()
                        + "] can not consume by broadcast way");
                return response;
            }

            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                log.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(),
                        requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist" + FAQUrl
                        .suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                log.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(),
                        subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription not latest");
                return response;
            }
        }

        RopMessageFilter messageFilter = new RopMessageFilter(subscriptionData);
        // Obtain and process the received message data from the message store.
        PulsarMessageStore pulsarMessageStore = this
                .getServerCnxMsgStore(channel, requestHeader, request);

        // If obtaining the serverCnxMsgStore object fails, enter the retry phase
        // and wait for the heartbeat request to register.
        if (null == pulsarMessageStore) {
            response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
            responseHeader.setMaxOffset(requestHeader.getQueueOffset());
            responseHeader.setNextBeginOffset(requestHeader.getQueueOffset());
            responseHeader.setMinOffset(requestHeader.getQueueOffset());
            responseHeader.setSuggestWhichBrokerId(0L);
            response.setRemark("Get serverCnx failed.");
            return response;
        }

        //TODO:
        final RopGetMessageResult ropGetMessageResult = pulsarMessageStore
                .getMessage(CommonUtils.getPulsarPartitionIdByRequest(request), request, requestHeader, messageFilter);

        if (ropGetMessageResult != null) {
            response.setRemark(ropGetMessageResult.getStatus().name());
            responseHeader.setNextBeginOffset(ropGetMessageResult.getNextBeginOffset());
            responseHeader.setMinOffset(ropGetMessageResult.getMinOffset());
            responseHeader.setMaxOffset(ropGetMessageResult.getMaxOffset());
            responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);

            switch (ropGetMessageResult.getStatus()) {
                case FOUND:
                    response.setCode(ResponseCode.SUCCESS);
                    break;
                case MESSAGE_WAS_REMOVING:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case NO_MATCHED_LOGIC_QUEUE:
                case NO_MESSAGE_IN_QUEUE:
                    if (0 != requestHeader.getQueueOffset()) {
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                        log.info(
                                "the broker store no queue data, fix the request offset {} to {}, "
                                        + "Topic: {} QueueId: {} Consumer Group: {}",
                                requestHeader.getQueueOffset(),
                                ropGetMessageResult.getNextBeginOffset(),
                                requestHeader.getTopic(),
                                requestHeader.getQueueId(),
                                requestHeader.getConsumerGroup()
                        );
                    } else {
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                    }
                    break;
                case NO_MATCHED_MESSAGE:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case OFFSET_FOUND_NULL:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_OVERFLOW_BADLY:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    // TODO: warn and notify me
                    log.info("the request offset: {} over flow badly, broker max offset: {}, consumer: {}",
                            requestHeader.getQueueOffset(), ropGetMessageResult.getMaxOffset(),
                            channel.remoteAddress());
                    break;
                case OFFSET_OVERFLOW_ONE:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_TOO_SMALL:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    log.info(
                            "the request offset too small. group={}, topic={}, requestOffset={},"
                                    + " brokerMinOffset={}, clientIp={}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueOffset(),
                            ropGetMessageResult.getMinOffset(), channel.remoteAddress());
                    break;
                default:
                    assert false;
                    break;
            }

            if (this.hasConsumeMessageHook()) {
                ConsumeMessageContext context = new ConsumeMessageContext();
                context.setConsumerGroup(requestHeader.getConsumerGroup());
                context.setTopic(requestHeader.getTopic());
                context.setQueueId(requestHeader.getQueueId());

                String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);

                switch (response.getCode()) {
                    case ResponseCode.SUCCESS:
                        int commercialBaseCount = brokerController.getServerConfig().getCommercialBaseCount();
                        int incValue = ropGetMessageResult.getMsgCount4Commercial() * commercialBaseCount;

                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_SUCCESS);
                        context.setCommercialRcvTimes(incValue);
                        context.setCommercialRcvSize(ropGetMessageResult.getBufferTotalSize());
                        context.setCommercialOwner(owner);

                        break;
                    case ResponseCode.PULL_NOT_FOUND:
                        if (!brokerAllowSuspend) {
                            context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                            context.setCommercialRcvTimes(1);
                            context.setCommercialOwner(owner);

                        }
                        break;
                    case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    case ResponseCode.PULL_OFFSET_MOVED:
                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                        context.setCommercialRcvTimes(1);
                        context.setCommercialOwner(owner);
                        break;
                    default:
                        assert false;
                        break;
                }

                this.executeConsumeMessageHookBefore(context);
            }

            switch (response.getCode()) {
                case ResponseCode.SUCCESS:

                    this.brokerController.getBrokerStatsManager()
                            .incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                                    ropGetMessageResult.getMessageCount());
                    this.brokerController.getBrokerStatsManager()
                            .incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                                    ropGetMessageResult.getBufferTotalSize());
                    this.brokerController.getBrokerStatsManager()
                            .incBrokerGetNums(ropGetMessageResult.getMessageCount());

                    if (this.brokerController.getServerConfig().isTransferMsgByHeap()) {
                        try {
                            final long beginTimeMills = System.currentTimeMillis();
                            final byte[] r = this
                                    .readGetMessageResult(ropGetMessageResult, requestHeader.getConsumerGroup(),
                                            requestHeader.getTopic(), requestHeader.getQueueId());
                            this.brokerController.getBrokerStatsManager()
                                    .incGroupGetLatency(requestHeader.getConsumerGroup(),
                                            requestHeader.getTopic(), requestHeader.getQueueId(),
                                            (int) (System.currentTimeMillis() - beginTimeMills));
                            response.setBody(r);
                        } finally {
                            ropGetMessageResult.release();
                        }
                    } else {
                        try {
                            FileRegion fileRegion =
                                    new BatchMessageTransfer(
                                            response.encodeHeader(ropGetMessageResult.getBufferTotalSize()),
                                            ropGetMessageResult);
                            channel.writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                                if (!future.isSuccess()) {
                                    log.error("transfer many message by bytebuf failed, {}", channel.remoteAddress(),
                                            future.cause());
                                }
                            });
                        } catch (Throwable e) {
                            log.error("transfer many message by pagecache exception", e);
                            ropGetMessageResult.release();
                        }
                        response = null;
                    }

                    // Trace point:/rop/get
                    if (this.brokerController.isRopTraceEnable()) {
                        List<RopMessage> ropMessages = ropGetMessageResult.getMessageBufferList();
                        for (RopMessage ropMessage : ropMessages) {
                            TraceContext traceContext = new TraceContext();
                            traceContext.setTopic(ropGetMessageResult.getPulsarTopic());
                            traceContext.setGroup(
                                    new ClientGroupName(requestHeader.getConsumerGroup()).getPulsarGroupName());
                            traceContext.setPartitionId(ropGetMessageResult.getPartitionId());
                            traceContext.setOffset(ropMessage.getOffset());
                            traceContext.setMsgId(ropMessage.getMsgId());
                            if (request.getExtFields().containsKey(ROP_INNER_CLIENT_ADDRESS)) {
                                traceContext.setInstanceName(request.getExtFields().get(ROP_INNER_CLIENT_ADDRESS));
                            } else {
                                traceContext.setInstanceName(ropGetMessageResult.getInstanceName());
                            }
                            traceContext.setMessageModel(isBroadcasting ? MessageModel.BROADCASTING.getModeCN()
                                    : MessageModel.CLUSTERING.getModeCN());
                            TraceManager.get().traceGet(traceContext);
                        }
                    }
                    break;
                case ResponseCode.PULL_NOT_FOUND:
                    if (brokerAllowSuspend && hasSuspendFlag) {
                        long pollingTimeMills = suspendTimeoutMillisLong;
                        if (!this.brokerController.getServerConfig().isLongPollingEnable()) {
                            pollingTimeMills = this.brokerController.getServerConfig().getShortPollingTimeMills();
                        }

                        String topic = requestHeader.getTopic();
                        long offset = Math
                                .max(ropGetMessageResult.getNextBeginOffset(), requestHeader.getQueueOffset());
//                        int queueId = requestHeader.getQueueId();
                        int partitionId = CommonUtils.getPulsarPartitionIdByRequest(request);
                        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills,
                                System.currentTimeMillis(), offset, subscriptionData, null);
                        this.brokerController.getPullRequestHoldService()
                                .suspendPullRequest(topic, partitionId, pullRequest);
                        response = null;
                    }
                    break;
                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    break;
                case ResponseCode.PULL_OFFSET_MOVED:
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    log.warn(
                            "PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, "
                                    + "requestOffset={}, suggestBrokerId={}",
                            requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                            requestHeader.getQueueOffset(),
                            responseHeader.getSuggestWhichBrokerId());
                    break;
                default:
                    assert false;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store getMessage return null");
        }
        boolean storeOffsetEnable = brokerAllowSuspend && hasCommitOffsetFlag;
        if (storeOffsetEnable) {
            this.brokerController.getConsumerOffsetManager().commitOffsetByPartitionId(
                    RemotingHelper.parseChannelRemoteAddr(channel),
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                    CommonUtils.getPulsarPartitionIdByRequest(request), requestHeader.getCommitOffset());
        }
        return response;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookBefore(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    private byte[] readGetMessageResult(final RopGetMessageResult getMessageResult, final String group,
            final String topic,
            final int queueId) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());
        long storeTimestamp = 0;
        try {
            List<RopMessage> messageBufferList = getMessageResult.getMessageBufferList();
            for (RopMessage ropMessage : messageBufferList) {
                ByteBuf bb = ropMessage.getPayload();
                byteBuffer.put(bb.nioBuffer());
                int sysFlag = bb.getInt(MessageDecoder.SYSFLAG_POSITION);
                // bornhost has the IPv4 ip if the MessageSysFlag.BORNHOST_V6_FLAG bit of sysFlag is 0
                // IPv4 host = ip(4 byte) + port(4 byte); IPv6 host = ip(16 byte) + port(4 byte)
                int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                int msgStoreTimePos = 4 // 1 TOTALSIZE
                        + 4 // 2 MAGICCODE
                        + 4 // 3 BODYCRC
                        + 4 // 4 QUEUEID
                        + 4 // 5 FLAG
                        + 8 // 6 QUEUEOFFSET
                        + 8 // 7 PHYSICALOFFSET
                        + 4 // 8 SYSFLAG
                        + 8 // 9 BORNTIMESTAMP
                        + bornHostLength; // 10 BORNHOST
                storeTimestamp = bb.getLong(msgStoreTimePos);
            }

        } finally {
        }
        return byteBuffer.array();
    }

    public void executeRequestWhenWakeup(final Channel channel,
            final RemotingCommand request, final long queueOffset) throws RemotingCommandException {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {

                    final PullMessageRequestHeader requestHeader =
                            (PullMessageRequestHeader) request
                                    .decodeCommandCustomHeader(PullMessageRequestHeader.class);
                    requestHeader.setQueueOffset(queueOffset);
                    final RemotingCommand response = PullMessageProcessor.this
                            .processRequest(channel, requestHeader, request, false);

                    if (response != null) {
                        response.setOpaque(request.getOpaque());
                        response.markResponseType();
                        try {
                            channel.writeAndFlush(response).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        log.error("processRequestWrapper response to {} failed",
                                                future.channel().remoteAddress(), future.cause());
                                        log.error(request.toString());
                                        log.error(response.toString());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("processRequestWrapper process request over, but response failed", e);
                            log.error(request.toString());
                            log.error(response.toString());
                        }
                    }
                } catch (RemotingCommandException e1) {
                    log.error("excuteRequestWhenWakeup run", e1);
                }
            }
        };
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> sendMessageHookList) {
        this.consumeMessageHookList = sendMessageHookList;
    }
}

