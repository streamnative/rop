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

package org.streamnative.pulsar.handlers.rocketmq;

import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQRemoteServer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.rocketmq.remoting.netty.NettyDecoder;

/**
 * rocketmq output data encoder.
 */
@Slf4j
public class RocketMQChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Getter
    private final BrokerService brokerService;
    @Getter
    private final RocketMQServiceConfiguration rocketmqConfig;
    @Getter
    private final RocketMQBrokerController brokerController;
    @Getter
    private final RocketMQRemoteServer remoteServer;
    @Getter
    private final boolean enableTls;


    public RocketMQChannelInitializer(RocketMQServiceConfiguration rocketmqConfig,
            RocketMQBrokerController rocketmqBroker, BrokerService brokerService,
            boolean enableTls) {
        super();
        this.rocketmqConfig = rocketmqConfig;
        this.brokerController = rocketmqBroker;
        this.brokerService = brokerService;
        this.enableTls = enableTls;
        this.remoteServer = rocketmqBroker.getRemotingServer();
    }


    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        //remoteServer.setRocketMQChannel(ch);
        ch.pipeline()
                .addLast(remoteServer.getDefaultEventExecutorGroup(), remoteServer.HANDSHAKE_HANDLER_NAME,
                        remoteServer.getHandshakeHandler())
                .addLast(remoteServer.getDefaultEventExecutorGroup(),
                        remoteServer.getEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, rocketmqConfig.getServerChannelMaxIdleTimeSeconds()),
                        remoteServer.getConnectionManageHandler(),
                        remoteServer.getServerHandler()
                );

        log.info("Successfully init channel {}", ch.remoteAddress().getHostString());
    }
}
