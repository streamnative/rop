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

import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_INNER_CLIENT_ADDRESS;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_INNER_MESSAGE_ID;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_TRACE_START_TIME;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageCallback;
import org.streamnative.pulsar.handlers.rocketmq.inner.PutMessageResult;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RopPutMessageResult;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopPersistentTopicException;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.trace.TraceContext;
import org.streamnative.pulsar.handlers.rocketmq.inner.trace.TraceManager;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Send message processor.
 */
@Slf4j
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final RocketMQBrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
            RemotingCommand request) throws RemotingCommandException {
        SendMessageContext mqtraceContext;
        switch (request.getCode()) {
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.consumerSendMsgBack(ctx, request);
            default:
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    return null;
                }

                mqtraceContext = buildMsgContext(ctx, requestHeader);
                this.executeSendMessageHookBefore(ctx, request, mqtraceContext);

                TraceContext traceContext = null;
                if (this.brokerController.isRopTraceEnable()) {
                    traceContext = TraceContext.buildMsgContext(ctx, requestHeader);
                    traceContext.setPutStartTime(NumberUtils
                            .toLong(request.getExtFields().get(ROP_TRACE_START_TIME), System.currentTimeMillis()));
                    if (request.getExtFields().containsKey(ROP_INNER_CLIENT_ADDRESS)) {
                        traceContext.setInstanceName(request.getExtFields().get(ROP_INNER_CLIENT_ADDRESS));
                    }
                    if (!request.isOnewayRPC()) {
                        traceContext.setFromProxy(CommonUtils.isFromProxy(request));
                    }
                }

                RemotingCommand response;
                if (requestHeader.isBatch()) {
                    response = this.sendBatchMessage(ctx, request, mqtraceContext, requestHeader, traceContext);
                } else {
                    response = this.sendMessage(ctx, request, mqtraceContext, requestHeader, traceContext);
                }

                if (response != null) {
                    this.executeSendMessageHookAfter(response, mqtraceContext);
                }
                return response;
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ConsumerSendMsgBackRequestHeader requestHeader =
                (ConsumerSendMsgBackRequestHeader) request
                        .decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);
        String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getGroup());
        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setNamespace(namespace);
            context.setConsumerGroup(requestHeader.getGroup());
            context.setTopic(requestHeader.getOriginTopic());
            context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
            context.setCommercialRcvTimes(1);
            context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));

            this.executeConsumeMessageHookAfter(context);
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager()
                        .findSubscriptionGroupConfig(requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            log.warn(
                    "[SendBackMsg] lookMessageByCommitLogOffset getSubscriptionGroupManager error, "
                            + "request header: [{}].",
                    requestHeader);
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return response;
        }

        if (!PermName.isWriteable(this.brokerController.getServerConfig().getBrokerPermission())) {
            log.warn("[SendBackMsg] lookMessageByCommitLogOffset getBrokerPermission error, request header: [{}].",
                    requestHeader);
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" //+ this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] sending message is forbidden");
            return response;
        }

        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            log.warn("[SendBackMsg] lookMessageByCommitLogOffset getRetryQueueNums < 0 error, request header: [{}].",
                    requestHeader);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();

        int topicSysFlag = 0;
        if (requestHeader.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                newTopic,
                subscriptionGroupConfig.getRetryQueueNums(),
                PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        if (null == topicConfig) {
            log.warn("[SendBackMsg] lookMessageByCommitLogOffset get topicConfig error, request header: [{}].",
                    requestHeader);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return response;
        }

        if (!PermName.isWriteable(topicConfig.getPerm())) {
            log.warn("[SendBackMsg] lookMessageByCommitLogOffset get topicConfig Perm error, request header: [{}].",
                    requestHeader);
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return response;
        }

        MessageExt msgExt = this
                .getServerCnxMsgStore(ctx, CommonUtils.getInnerProducerGroupName(request, requestHeader.getGroup()))
                .lookMessageByCommitLogOffset(requestHeader);
        if (null == msgExt) {
            log.warn("[SendBackMsg] lookMessageByCommitLogOffset request header: [{}] error.", requestHeader);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return response;
        }

        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        msgExt.setWaitStoreMsgOK(false);

        int delayLevel = requestHeader.getDelayLevel();
        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            maxReconsumeTimes = requestHeader.getMaxReconsumeTimes() == null ? maxReconsumeTimes
                    : requestHeader.getMaxReconsumeTimes();
        }

        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes || delayLevel < 0) {
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            queueIdInt = 0;

            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                    DLQ_NUMS_PER_GROUP,
                    PermName.PERM_WRITE, 0
            );
            if (null == topicConfig) {
                log.warn("[SendBackMsg] lookMessageByCommitLogOffset get DLQ topicConfig error, request header: [{}].",
                        requestHeader);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return response;
            }
        } else {
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }

            msgExt.setDelayTimeLevel(delayLevel);
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(ctx.channel().localAddress());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);

        traceDlq(msgInner.getTopic(), msgInner.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                ctx, request);

        try {
            CompletableFuture<RemotingCommand> responseFuture = new CompletableFuture<>();
            this.getServerCnxMsgStore(ctx, CommonUtils.getInnerProducerGroupName(request, requestHeader.getGroup()))
                    .putSendBackMsg(msgInner, requestHeader.getGroup(), response, responseFuture);
            return responseFuture.get();
        } catch (Exception e) {
            log.warn("[{}] consumerSendMsgBack failed", requestHeader.getGroup(), e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
        }
        return response;
    }

    private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader, RemotingCommand response,
            RemotingCommand request,
            MessageExt msg, TopicConfig topicConfig) {
        String newTopic = requestHeader.getTopic();
        if (NamespaceUtil.isRetryTopic(newTopic)) {
            String groupName = newTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            RocketMQTopic pulsarGroupName = new RocketMQTopic(groupName);
            SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager()
                            .findSubscriptionGroupConfig(pulsarGroupName.getOrigNoDomainTopicName());
            if (null == subscriptionGroupConfig) {
                response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
                response.setRemark(
                        "subscription group not exist, " + pulsarGroupName.getOrigNoDomainTopicName() + " " + FAQUrl
                                .suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
                return false;
            }

            int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
            if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
                maxReconsumeTimes = requestHeader.getMaxReconsumeTimes() != null
                        ? requestHeader.getMaxReconsumeTimes() : maxReconsumeTimes;
            }
            int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes();
            if (reconsumeTimes >= maxReconsumeTimes) {
                newTopic = MixAll.getDLQTopic(groupName);
                int queueIdInt = 0;
                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                        DLQ_NUMS_PER_GROUP,
                        PermName.PERM_WRITE, 0
                );
                msg.setTopic(newTopic);
                msg.setQueueId(queueIdInt);
                if (null == topicConfig) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("topic[" + newTopic + "] not exist");
                    return false;
                }
            }
        }
        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        msg.setSysFlag(sysFlag);
        return true;
    }

    private RemotingCommand sendMessage(final ChannelHandlerContext ctx,
            final RemotingCommand request,
            final SendMessageContext sendMessageContext,
            final SendMessageRequestHeader requestHeader,
            final TraceContext traceContext) throws RemotingCommandException {

        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        response.setOpaque(request.getOpaque());

        log.debug("receive SendMessage request command, {}", request);

        final byte[] body = request.getBody();
        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager()
                .selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);

        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return response;
        }

        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        if (request.getExtFields().containsKey(ROP_INNER_CLIENT_ADDRESS)) {
            try {
                String[] arr = request.getExtFields().get(ROP_INNER_CLIENT_ADDRESS).split(":");
                msgInner.setBornHost(new InetSocketAddress(arr[0], Integer.parseInt(arr[1])));
            } catch (Exception e) {
                log.info("RoP parser inner client address [{}] failed.",
                        request.getExtFields().get(ROP_INNER_CLIENT_ADDRESS), e);
            }
        }
        msgInner.setStoreHost(ctx.channel().localAddress());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getServerConfig().getClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        org.apache.rocketmq.store.PutMessageResult putMessageResult;
        Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);

        traceDlq(msgInner.getTopic(), msgInner.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                ctx, request);

        if (Boolean.parseBoolean(traFlag)
                && !(msgInner.getReconsumeTimes() > 0
                && msgInner.getDelayTimeLevel() > 0)) { //For client under version 4.6.1
            putMessageResult = new org.apache.rocketmq.store.PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            return handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader,
                    sendMessageContext, queueIdInt);
        } else {
            try {
                if (traceContext != null) {
                    traceContext.setPersistStartTime(System.currentTimeMillis());
                }
                this.getServerCnxMsgStore(ctx,
                        CommonUtils.getInnerProducerGroupName(request, requestHeader.getProducerGroup()))
                        .putMessage(CommonUtils.getPulsarPartitionIdByRequest(request),
                                msgInner,
                                requestHeader.getProducerGroup(),
                                new SendMessageCallback(response, request, msgInner, responseHeader,
                                        sendMessageContext, ctx, queueIdInt, traceContext));
                return null;
            } catch (RopPersistentTopicException e) {
                log.warn("[{}] sendMessage failed, for Owned Topic isn't on this broker.", requestHeader.getTopic());
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("NotFoundTopic");
                return response;
            } catch (Exception e) {
                log.warn("[{}] sendMessage failed", requestHeader.getTopic(), e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(e.getMessage());
                return response;
            }
        }
    }

    private RemotingCommand handlePutMessageResult(org.apache.rocketmq.store.PutMessageResult putMessageResult,
            RemotingCommand response,
            RemotingCommand request, MessageExt msg, SendMessageResponseHeader responseHeader,
            SendMessageContext sendMessageContext, int queueIdInt) {
        if (putMessageResult == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;

        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(
                        "the message is illegal, maybe msg body or properties length not matched. msg body "
                                + "length limit 128k, msg properties length limit 32k.");
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                        "service not available now, maybe disk full, maybe your broker machine memory too small.");
                break;
            case OS_PAGECACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        if (sendOK) {
            this.brokerController.getBrokerStatsManager()
                    .incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                    putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager()
                    .incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());

            response.setRemark(null);
            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getServerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int incValue = (int) Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        } else {
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int) Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        }
        return response;
    }

    private RemotingCommand sendBatchMessage(final ChannelHandlerContext ctx,
            final RemotingCommand request,
            final SendMessageContext sendMessageContext,
            final SendMessageRequestHeader requestHeader,
            final TraceContext traceContext) throws RemotingCommandException {

        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

        response.setOpaque(request.getOpaque());

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager()
                .selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("message topic length too long " + requestHeader.getTopic().length());
            return response;
        }

        if (requestHeader.getTopic() != null && requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("batch request does not support retry group " + requestHeader.getTopic());
            return response;
        }
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(requestHeader.getTopic());
        messageExtBatch.setQueueId(queueIdInt);

        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        messageExtBatch.setSysFlag(sysFlag);

        messageExtBatch.setFlag(requestHeader.getFlag());
        MessageAccessor
                .setProperties(messageExtBatch, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        messageExtBatch.setBody(request.getBody());
        messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
        messageExtBatch.setBornHost(ctx.channel().remoteAddress());
        if (request.getExtFields().containsKey(ROP_INNER_CLIENT_ADDRESS)) {
            try {
                String[] arr = request.getExtFields().get(ROP_INNER_CLIENT_ADDRESS).split(":");
                messageExtBatch.setBornHost(new InetSocketAddress(arr[0], Integer.parseInt(arr[1])));
            } catch (Exception e) {
                log.info("RoP parser inner client address [{}] failed.",
                        request.getExtFields().get(ROP_INNER_CLIENT_ADDRESS), e);
            }
        }
        messageExtBatch.setStoreHost(ctx.channel().localAddress());
        messageExtBatch
                .setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getServerConfig().getClusterName();
        MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_CLUSTER, clusterName);

        try {
            if (traceContext != null) {
                traceContext.setPersistStartTime(System.currentTimeMillis());
            }
            this.getServerCnxMsgStore(ctx,
                    CommonUtils.getInnerProducerGroupName(request, requestHeader.getProducerGroup()))
                    .putMessages(CommonUtils.getPulsarPartitionIdByRequest(request),
                            messageExtBatch,
                            requestHeader.getProducerGroup(),
                            new SendMessageCallback(response, request, messageExtBatch, responseHeader,
                                    sendMessageContext,
                                    ctx, queueIdInt, traceContext), traceContext != null);
            return null;
        } catch (Exception e) {
            log.warn("[{}] sendBatchMessage failed", requestHeader.getTopic(), e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
            return response;
        }
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    private void traceDlq(String rmqTopic, String msgId, ChannelHandlerContext ctx, RemotingCommand request) {
        if (this.brokerController.isRopTraceEnable() && rmqTopic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
            TraceContext traceContext = new TraceContext();
            String dlqTopic = new ClientTopicName(rmqTopic).getPulsarTopicName();
            traceContext.setTopic(dlqTopic);
            traceContext.setGroup(dlqTopic.replace(MixAll.DLQ_GROUP_TOPIC_PREFIX, ""));
            traceContext.setMsgId(msgId);
            if (request.getExtFields().containsKey(ROP_INNER_CLIENT_ADDRESS)) {
                traceContext.setInstanceName(request.getExtFields().get(ROP_INNER_CLIENT_ADDRESS));
            } else {
                traceContext.setInstanceName(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
            TraceManager.get().traceQlq(traceContext);
        }
    }

    /**
     * Send message callback.
     */
    public class SendMessageCallback implements PutMessageCallback {

        private final RemotingCommand response;
        private final RemotingCommand request;
        private final MessageExt msg;
        private final SendMessageResponseHeader responseHeader;
        private final SendMessageContext sendMessageContext;
        private final ChannelHandlerContext ctx;
        private final int queueIdInt;
        private final TraceContext traceContext;

        public SendMessageCallback(RemotingCommand response, RemotingCommand request, MessageExt msg,
                SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext,
                ChannelHandlerContext ctx, int queueIdInt, TraceContext traceContext) {
            this.response = response;
            this.request = request;
            this.msg = msg;
            this.responseHeader = responseHeader;
            this.sendMessageContext = sendMessageContext;
            this.ctx = ctx;
            this.queueIdInt = queueIdInt;
            this.traceContext = traceContext;
        }

        public void callback(org.apache.rocketmq.store.PutMessageResult putMessageResult) {
            try {
                handlePutMessageResult(putMessageResult, response, request, msg, responseHeader, sendMessageContext,
                        queueIdInt);
            } catch (Exception e) {
                log.error("SendMessageCallback callback failed.", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("execute callback failed");
            }

            // Trace point:/rop/persist finish
            if (traceContext != null && putMessageResult instanceof RopPutMessageResult) {
                long now = System.currentTimeMillis();
                RopPutMessageResult ropPutMessageResult = (RopPutMessageResult) putMessageResult;
                response.addExtField(ROP_INNER_MESSAGE_ID,
                        JSON.toJSONString(ropPutMessageResult.getPutMessageResults()));

                traceContext.setCode(response.getCode());
                for (PutMessageResult result : ropPutMessageResult.getPutMessageResults()) {
                    traceContext.setOffset(result.getOffset());
                    traceContext.setMsgId(result.getMsgId());
                    traceContext.setOffsetMsgId(result.getOffsetMsgId());
                    traceContext.setPulsarMsgId(result.getPulsarMsgId());
                    traceContext.setEndTime(now);
                    traceContext.setDuration(now - traceContext.getPersistStartTime());
                    TraceManager.get().tracePersist(traceContext);
                }
            }

            doResponse(ctx, request, response);

            // execute send message hook
            executeSendMessageHookAfter(response, sendMessageContext);

            // Trace point:/rop/put finish
            if (traceContext != null && !traceContext.isFromProxy()
                    && putMessageResult instanceof RopPutMessageResult) {
                long now = System.currentTimeMillis();
                RopPutMessageResult ropPutMessageResult = (RopPutMessageResult) putMessageResult;

                traceContext.setCode(response.getCode());
                for (PutMessageResult result : ropPutMessageResult.getPutMessageResults()) {
                    traceContext.setOffset(result.getOffset());
                    traceContext.setMsgId(result.getMsgId());
                    traceContext.setMsgKey(result.getMsgKey());
                    traceContext.setTags(result.getMsgTag());
                    traceContext.setPartitionId(result.getPartition());
                    traceContext.setOffsetMsgId(result.getOffsetMsgId());
                    traceContext.setPulsarMsgId(result.getPulsarMsgId());
                    traceContext.setEndTime(now);
                    traceContext.setDuration(now - traceContext.getPutStartTime());
                    TraceManager.get().tracePut(traceContext);
                }
            }

        }
    }

    /**
     * Send message back callback.
     */
    public class SendMessageBackCallback implements PutMessageCallback {

        private final RemotingCommand response;
        private final RemotingCommand request;
        private final ChannelHandlerContext ctx;
        private final ConsumerSendMsgBackRequestHeader requestHeader;
        private final MessageExt msgExt;

        public SendMessageBackCallback(RemotingCommand response, RemotingCommand request,
                ChannelHandlerContext ctx, ConsumerSendMsgBackRequestHeader requestHeader,
                MessageExt msgExt) {
            this.response = response;
            this.request = request;
            this.ctx = ctx;
            this.requestHeader = requestHeader;
            this.msgExt = msgExt;
        }

        public void callback(org.apache.rocketmq.store.PutMessageResult putMessageResult) {
            try {
                if (putMessageResult != null) {
                    if (putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                        String backTopic = msgExt.getTopic();
                        String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                        if (correctTopic != null) {
                            backTopic = correctTopic;
                        }

                        brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);

                        response.setCode(ResponseCode.SUCCESS);
                        response.setRemark(null);
                    } else {
                        response.setCode(ResponseCode.SYSTEM_ERROR);
                        response.setRemark(putMessageResult.getPutMessageStatus().name());
                    }
                } else {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("putMessageResult is null");
                }
            } catch (Exception e) {
                log.error("SendMessageBackCallback callback failed.", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("execute callback failed");
            }
            doResponse(ctx, request, response);
        }
    }
}
