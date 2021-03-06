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

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBodyForC;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.ConsumerGroupInfo;
import org.streamnative.pulsar.handlers.rocketmq.inner.namesvr.MQTopicManager;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientTopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Broker to client.
 */
@Slf4j
public class Broker2Client {

    private final RocketMQBrokerController brokerController;

    public Broker2Client(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void checkProducerTransactionState(
            final String group,
            final Channel channel,
            final CheckTransactionStateRequestHeader requestHeader,
            final MessageExt messageExt) throws Exception {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
        request.setBody(MessageDecoder.encode(messageExt, false));
        try {
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("Check transaction failed because invoke producer exception. group={}, msgId={}, exception={}",
                    group,
                    messageExt.getMsgId(), e.getMessage());
        }
    }

    public RemotingCommand callClient(final Channel channel,
            final RemotingCommand request
    ) throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        return this.brokerController.getRemotingServer().invokeSync(channel, request, 10000);
    }

    public void notifyConsumerIdsChanged(
            final Channel channel,
            final String consumerGroup) {
        if (null == consumerGroup) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);

        try {
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e.getMessage());
        }
    }

    public RemotingCommand resetOffset(String rmqTopic, String group, long timeStamp, boolean isForce, boolean isC) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(group);
        if (consumerGroupInfo == null || consumerGroupInfo.getAllChannel().isEmpty()) {
            String errorInfo =
                    String.format("Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d",
                            group, rmqTopic, timeStamp);
            log.error(errorInfo);
            response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
            response.setRemark(errorInfo);
            return response;
        }

        MQTopicManager topicManager = brokerController.getTopicConfigManager();
        TopicConfig topicConfig = topicManager.selectTopicConfig(rmqTopic);
        if (null == topicConfig) {
            log.error("[reset-offset] reset offset failed, no topic in this broker. topic={}", rmqTopic);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("[reset-offset] reset offset failed, no topic in this broker. topic=" + rmqTopic);
            return response;
        }

        Map<MessageQueue, Long> offsetTable = new HashMap<>();
        TopicName topicName = new RocketMQTopic(rmqTopic).getPulsarTopicName();

        List<Integer> partitionList = this.brokerController.getRopBrokerProxy()
                .getPulsarTopicPartitionIdList(topicName);
        if (partitionList == null || partitionList.isEmpty()) {
            response.setCode(ResponseCode.SUCCESS);
            response.setBody(null);
            return response;
        }

        CountDownLatch latch = new CountDownLatch(partitionList.size());
        for (int i = 0; i < partitionList.size(); i++) {
            final int queueId = i;
            this.brokerController.getAdminWorkerExecutor().execute(() -> {
                try {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(rmqTopic);
                    mq.setBrokerName(brokerController.getRopBrokerProxy().getBrokerTag());
                    mq.setQueueId(queueId);

                    int partitionId = partitionList.get(queueId);

                    /*
                     * get offset from logic broker
                     */
                    long consumeOffset = brokerController.getConsumerOffsetManager()
                            .queryOffsetByPartitionId(group, rmqTopic, partitionId);
                    if (-1 == consumeOffset) {
                        log.warn("Rop group [{}] not found consume offset for topic [{}].", group, rmqTopic);
                        return;
                    }

                    /*
                     * get offset by timestamp from physical broker
                     */
                    long timeStampOffset = -1L;
                    if (brokerController.getTopicConfigManager().isPartitionTopicOwner(topicName, partitionId)) {
                        ClientGroupAndTopicName groupAndTopicName = new ClientGroupAndTopicName(Strings.EMPTY,
                                rmqTopic);
                        try {
                            if (timeStamp != -1) {
                                timeStampOffset = brokerController.getConsumerOffsetManager()
                                        .searchOffsetByTimestamp(groupAndTopicName, partitionId, timeStamp);
                            } else {
                                timeStampOffset = brokerController.getConsumerOffsetManager()
                                        .getNextOffset(groupAndTopicName.getClientTopicName(), partitionId);
                            }
                        } catch (Exception e) {
                            log.warn(
                                    "Rop [{}] reset offset searchOffsetByTimestamp or getMaxOffset from local failed.",
                                    groupAndTopicName, e);
                        }
                    } else {
                        ClientTopicName clientTopicName = new ClientTopicName(rmqTopic);
                        try {
                            if (timeStamp != -1) {
                                timeStampOffset = brokerController.getRopBrokerProxy()
                                        .searchOffsetByTimestamp(clientTopicName, queueId, partitionId,
                                                timeStamp);
                            } else {
                                timeStampOffset = brokerController.getRopBrokerProxy()
                                        .searchMaxOffset(clientTopicName, queueId, partitionId);
                            }
                        } catch (Exception e) {
                            log.warn(
                                    "Rop [{}] reset offset searchOffsetByTimestamp or getMaxOffset from remote failed.",
                                    clientTopicName, e);
                        }
                    }

                    if (timeStampOffset < 0) {
                        log.warn("Rop reset offset is invalid. topic={}, queueId={}, timeStampOffset={}", rmqTopic,
                                partitionId,
                                timeStampOffset);
                        timeStampOffset = 0;
                    }

                    // if isForce is true and timeStampOffset < consumeOffset,reset offset to timeStampOffset
                    if (isForce || timeStampOffset < consumeOffset) {
                        offsetTable.put(mq, timeStampOffset);
                    } else {
                        offsetTable.put(mq, consumeOffset);
                    }
                } catch (Exception e) {
                    log.warn("Rop reset offset for [{}] [{}] failed.", group, rmqTopic, e);
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Rop reset offset for [{}] [{}] error.", group, rmqTopic, e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
            return response;
        }

        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(rmqTopic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timeStamp);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, requestHeader);
        if (isC) {
            // c++ language
            ResetOffsetBodyForC body = new ResetOffsetBodyForC();
            List<MessageQueueForC> offsetList = convertOffsetTable2OffsetList(offsetTable);
            body.setOffsetTable(offsetList);
            request.setBody(body.encode());
        } else {
            // other language
            ResetOffsetBody body = new ResetOffsetBody();
            body.setOffsetTable(offsetTable);
            request.setBody(body.encode());
        }

        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
        for (Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
            int version = entry.getValue().getVersion();
            if (version >= MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                try {
                    this.brokerController.getRemotingServer().invokeOneway(entry.getKey(), request, 5000);
                    log.info("[reset-offset] reset offset success. topic={}, group={}, clientId={}",
                            rmqTopic, group, entry.getValue().getClientId());
                } catch (Exception e) {
                    log.error("[reset-offset] reset offset exception. topic={}, group={}",
                            new Object[]{rmqTopic, group}, e);
                }
            } else {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("the client does not support this feature. version="
                        + MQVersion.getVersionDesc(version));
                log.warn("[reset-offset] the client does not support this feature. remoteAddr={}, version={}",
                        RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        ResetOffsetBody resBody = new ResetOffsetBody();
        resBody.setOffsetTable(offsetTable);
        response.setBody(resBody.encode());
        return response;
    }

    private List<MessageQueueForC> convertOffsetTable2OffsetList(Map<MessageQueue, Long> table) {
        List<MessageQueueForC> list = new ArrayList<>();
        for (Entry<MessageQueue, Long> entry : table.entrySet()) {
            MessageQueue mq = entry.getKey();
            MessageQueueForC tmp =
                    new MessageQueueForC(mq.getTopic(), mq.getBrokerName(), mq.getQueueId(), entry.getValue());
            list.add(tmp);
        }
        return list;
    }

    public RemotingCommand getConsumeStatus(String topic, String group, String originClientId) {
        final RemotingCommand result = RemotingCommand.createResponseCommand(null);

        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT,
                        requestHeader);

        Map<String, Map<MessageQueue, Long>> consumerStatusTable = new HashMap<>();
        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(group).getChannelInfoTable();
        if (null == channelInfoTable || channelInfoTable.isEmpty()) {
            result.setCode(ResponseCode.SYSTEM_ERROR);
            result.setRemark(String.format("No Any Consumer online in the consumer group: [%s]", group));
            return result;
        }

        for (Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
            int version = entry.getValue().getVersion();
            String clientId = entry.getValue().getClientId();
            if (version < MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                result.setCode(ResponseCode.SYSTEM_ERROR);
                result.setRemark("the client does not support this feature. version="
                        + MQVersion.getVersionDesc(version));
                log.warn("[get-consumer-status] the client does not support this feature. remoteAddr={}, version={}",
                        RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                return result;
            } else if (UtilAll.isBlank(originClientId) || originClientId.equals(clientId)) {
                try {
                    RemotingCommand response =
                            this.brokerController.getRemotingServer().invokeSync(entry.getKey(), request, 5000);
                    assert response != null;
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        if (response.getBody() != null) {
                            GetConsumerStatusBody body =
                                    GetConsumerStatusBody.decode(response.getBody(),
                                            GetConsumerStatusBody.class);

                            consumerStatusTable.put(clientId, body.getMessageQueueTable());
                            log.info("[get-consumer-status] get consumer status success. topic={}, group={}, "
                                    + "channelRemoteAddr={}", topic, group, clientId);
                        }
                    }
                } catch (Exception e) {
                    log.error(
                            "[get-consumer-status] get consumer status exception. topic={}, group={}, offset={}",
                            topic, group, e);
                }

                if (!UtilAll.isBlank(originClientId) && originClientId.equals(clientId)) {
                    break;
                }
            }
        }

        result.setCode(ResponseCode.SUCCESS);
        GetConsumerStatusBody resBody = new GetConsumerStatusBody();
        resBody.setConsumerTable(consumerStatusTable);
        result.setBody(resBody.encode());
        return result;
    }
}

