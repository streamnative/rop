package com.tencent.tdmq.handlers.rocketmq;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import org.apache.pulsar.broker.PulsarService;
import org.apache.rocketmq.remoting.netty.NettyEncoder;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 1:47 下午
 */

/**
 * rocketmq output data encoder.
 */
@Data
public class RocketMQChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final PulsarService pulsarService;
    private final RocketMQServiceConfiguration rocketmqConfig;
    private final RocketMQBrokerController rocketMQBrokerController;



    public RocketMQChannelInitializer(RocketMQServiceConfiguration rocketmqConfig, PulsarService pulsarService) {
        super();
        this.rocketmqConfig = rocketmqConfig;
        this.pulsarService = pulsarService;
        this.rocketMQBrokerController = new RocketMQBrokerController(this.rocketmqConfig);
    }

    private void prepareRocketMQHandler() {

    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {

        socketChannel.pipeline().addLast("frameEncoder", null);
    }
}
