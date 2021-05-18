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

import io.netty.channel.ChannelHandlerContext;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

/**
 * Rop client channel cnx.
 */
@Slf4j
public class RopClientChannelCnx extends ClientChannelInfo {

    @Getter
    private final RocketMQBrokerController brokerController;
    @Getter
    private final RopServerCnx serverCnx;

    public RopClientChannelCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        this(brokerController, ctx, (String) null, (LanguageCode) null, 0);
    }

    public RopClientChannelCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx, String clientId,
            LanguageCode language, int version) {
        super(ctx.channel(), clientId, language, version);
        this.brokerController = brokerController;
        this.serverCnx = new RopServerCnx(brokerController, ctx);
    }

    public String toString() {
        return super.toString();
    }

    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), brokerController, serverCnx);
    }
}
