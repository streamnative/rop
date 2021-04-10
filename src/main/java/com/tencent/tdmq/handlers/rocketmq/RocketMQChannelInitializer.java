package com.tencent.tdmq.handlers.rocketmq;

import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQRemoteServer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.rocketmq.remoting.netty.NettyDecoder;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 1:47 下午
 */

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
