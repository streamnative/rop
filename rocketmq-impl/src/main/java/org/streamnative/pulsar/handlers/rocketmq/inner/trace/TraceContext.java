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

package org.streamnative.pulsar.handlers.rocketmq.inner.trace;

import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TraceContext {

    private String traceId;
    private String operationName;
    private long putStartTime;
    private long persistStartTime;
    private long endTime;
    private long duration;

    private String topic;
    private String group;
    private int partitionId;
    private String brokerTag;
    private long offset;
    private long queueId;
    private String msgId;
    private String offsetMsgId;
    private String pulsarMsgId;
    private String msgKey;
    private String instanceName;
    private String tags;
    private int code;

    private String messageModel;

    private boolean fromProxy;

    /**
     * 构建发送轨迹上下文
     */
    public static TraceContext buildMsgContext(ChannelHandlerContext ctx, SendMessageRequestHeader requestHeader) {
        TraceContext traceContext = new TraceContext();
        traceContext.setTopic(new RocketMQTopic(requestHeader.getTopic()).getOrigNoDomainTopicName());
        traceContext.setInstanceName(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        return traceContext;
    }
}
