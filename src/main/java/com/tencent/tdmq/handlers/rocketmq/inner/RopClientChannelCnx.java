package com.tencent.tdmq.handlers.rocketmq.inner;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

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
}
