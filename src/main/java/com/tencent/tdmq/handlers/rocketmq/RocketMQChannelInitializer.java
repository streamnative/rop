package com.tencent.tdmq.handlers.rocketmq;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.rocketmq.remoting.netty.NettyDecoder;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 1:47 下午
 */

/**
 * rocketmq output data encoder.
 */
public class RocketMQChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Getter
    private final PulsarService pulsarService;
    @Getter
    private final RocketMQServiceConfiguration rocketmqConfig;
    @Getter
    private final RocketMQBrokerController brokerController;
    @Getter
    private final RocketMQRemoteServer remoteServer;
    @Getter
    private final boolean enableTls;


    public RocketMQChannelInitializer(RocketMQServiceConfiguration rocketmqConfig, PulsarService pulsarService,
            boolean enableTls) {
        super();
        this.rocketmqConfig = rocketmqConfig;
        this.pulsarService = pulsarService;
        this.enableTls = enableTls;
        this.brokerController = new RocketMQBrokerController(rocketmqConfig, pulsarService, this);
        this.remoteServer = brokerController.getRemotingServer();
    }


    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new LengthFieldPrepender(4));
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
    }
}
