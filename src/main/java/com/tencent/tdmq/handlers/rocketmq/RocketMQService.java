package com.tencent.tdmq.handlers.rocketmq;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

@Slf4j
public class RocketMQService extends PulsarService {

    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private final RocketMQServiceConfiguration config;
    private final RocketMQBrokerController rocketMQBrokerController;

    private final ExecutorService publicExecutor;
    private final ChannelEventListener channelEventListener;
    private NettyEncoder encoder;
    private final Timer timer = new Timer("ServerHouseKeepingService", true);

    private NettyConnectManageHandler connectionManageHandler;
    private NettyServerHandler serverHandler;


    public RocketMQService(RocketMQServiceConfiguration config) {
        super(config);
        //super(config.getPermitsOneway(), config.getPermitsAsync());
        this.config = config;
        this.rocketMQBrokerController = new RocketMQBrokerController(config);
        this.channelEventListener = null;

        this.publicExecutor = Executors.newFixedThreadPool(config.getCallbackThreadPoolsNum(), new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
    }

    //@Override
    public ChannelEventListener getChannelEventListener() {
        return null;
    }

    //@Override
    public ExecutorService getCallbackExecutor() {
        return null;
    }

    @ChannelHandler.Sharable
    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            /* processMessageReceived(ctx, msg);*/
        }
    }

    @ChannelHandler.Sharable
    class NettyConnectManageHandler extends ChannelDuplexHandler {

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (RocketMQService.this.channelEventListener != null) {
               /* RocketMQService.this
                        .putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));*/
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (RocketMQService.this.channelEventListener != null) {
               /* RocketMQService.this
                        .putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));*/
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (RocketMQService.this.channelEventListener != null) {
                       /* RocketMQService.this
                                .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));*/
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (RocketMQService.this.channelEventListener != null) {
              /*  RocketMQService.this
                        .putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));*/
            }

            RemotingUtil.closeChannel(ctx.channel());
        }
    }


}
