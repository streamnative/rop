package com.tencent.tdmq.handlers.rocketmq.inner;

import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;

@Slf4j
public class RopServerCnx extends ServerCnx {

    @Getter
    private ChannelHandlerContext ctx;

    @Getter
    private RocketMQBrokerController brokerController;

    public RopServerCnx(RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        super(brokerController.getBrokerService().getPulsar());
        this.ctx = ctx;
        // this is the client address that connect to this server.
        this.remoteAddress = ctx.channel().remoteAddress();

        // mock some values, or Producer create will meet NPE.
        // used in test, which will not call channel.active, and not call updateCtx.
        if (this.remoteAddress == null) {
            this.remoteAddress = new InetSocketAddress("localhost", 9999);
        }
        this.ctx.pipeline().addLast(this);
    }

    @Override
    protected void handleLookup(CommandLookupTopic lookup) {
        super.handleLookup(lookup);
    }
}
