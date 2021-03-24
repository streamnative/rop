package com.tencent.tdmq.handlers.rocketmq;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 1:47 下午
 */

/**
 * rocketmq output data encoder.
 */
public class RocketmqChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Getter
    private final PulsarService pulsarService;
    @Getter
    private final RocketmqServiceConfiguration rocketmqConfig;

    public RocketmqChannelInitializer(RocketmqServiceConfiguration rocketmqConfig, PulsarService pulsarService) {
        super();
        this.rocketmqConfig = rocketmqConfig;
        this.pulsarService = pulsarService;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        socketChannel.pipeline().addLast("frameEncoder", new RocketmqEncoder());
    }
}
