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

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.ConsumerGroupInfo;
import org.streamnative.pulsar.handlers.rocketmq.inner.consumer.ConsumerOffsetManager;

/**
 * Consumer manage processor.
 */
@Slf4j
public class ConsumerManageProcessor implements NettyRequestProcessor {

    private final RocketMQBrokerController brokerController;

    public ConsumerManageProcessor(final RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                return this.getConsumerListByGroup(ctx, request);
            case RequestCode.UPDATE_CONSUMER_OFFSET:
                return this.updateConsumerOffset(ctx, request);
            case RequestCode.QUERY_CONSUMER_OFFSET:
                return this.queryConsumerOffset(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        final GetConsumerListByGroupRequestHeader requestHeader =
                (GetConsumerListByGroupRequestHeader) request
                        .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(
                        requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            if (!clientIds.isEmpty()) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(clientIds);
                response.setBody(body.encode());
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            } else {
                log.warn("getAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            log.warn("getConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }

    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        final UpdateConsumerOffsetRequestHeader requestHeader =
                (UpdateConsumerOffsetRequestHeader) request
                        .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
        this.brokerController.getConsumerOffsetManager()
                .commitOffset(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader responseHeader =
                (QueryConsumerOffsetResponseHeader) response.readCustomHeader();
        final QueryConsumerOffsetRequestHeader requestHeader =
                (QueryConsumerOffsetRequestHeader) request
                        .decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);
        ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();

        long offset = consumerOffsetManager.queryOffset(
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());

        if (offset >= 0) {
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
        } else {
            responseHeader.setOffset(-1L);
            response.setCode(ResponseCode.QUERY_NOT_FOUND);
            response.setRemark("Not found offset.");
//            ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager()
//                    .getConsumerGroupInfo(requestHeader.getConsumerGroup());
//            if (ConsumeType.CONSUME_PASSIVELY.equals(consumerGroupInfo.getConsumeType())
//                    && ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET.equals(consumerGroupInfo.getConsumeFromWhere())) {
//                responseHeader.setOffset(Long.MAX_VALUE);
//                response.setCode(ResponseCode.SUCCESS);
//            } else {
//                responseHeader.setOffset(-1L);
//                response.setCode(ResponseCode.QUERY_NOT_FOUND);
//                response.setRemark("Not found offset.");
//            }
        }

        return response;
    }
}