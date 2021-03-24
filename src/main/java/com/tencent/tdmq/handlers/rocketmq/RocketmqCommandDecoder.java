package com.tencent.tdmq.handlers.rocketmq;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 1:23 下午
 */

/**
 * 这个类主要继承 netty 的 ChannelInboundHandlerAdapter 类来达到对 request 和 response 的解析
 */

@Slf4j
public class RocketmqCommandDecoder extends ChannelInboundHandlerAdapter {

    protected ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;

    @Getter
    protected AtomicBoolean isActive = new AtomicBoolean(false);

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
        }
    }

}
