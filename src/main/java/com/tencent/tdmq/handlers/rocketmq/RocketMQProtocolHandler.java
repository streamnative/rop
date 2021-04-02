package com.tencent.tdmq.handlers.rocketmq;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.utils.ConfigurationUtils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 12:17 下午
 */

/**
 * 这个类主要用来实现 pulsar 中 ProtocolHandler 的接口
 * 可以通过 pulsar service 加载以及运行
 */
@Slf4j
@Data
public class RocketMQProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "rocketmq";
    public static final String SSL_PREFIX = "SSL://";
    public static final String PLAINTEXT_PREFIX = "PLAINTEXT://";
    public static final String LISTENER_DEL = ",";
    public static final String LISTENER_PATTEN = "^(PLAINTEXT?|SSL)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*:([0-9]+)";

    private RocketMQServiceConfiguration rocketmqConfig;
    private BrokerService brokerService;
    private RocketMQBrokerController rocketMQBroker;
    private String bindAddress;

    public static int getListenerPort(String listener) {
        checkState(listener.matches(LISTENER_PATTEN), "listener not match patten");

        int lastIndex = listener.lastIndexOf(':');
        return Integer.parseInt(listener.substring(lastIndex + 1));
    }

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equals(protocol.toLowerCase(Locale.ROOT));
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // init config
        if (conf instanceof RocketMQServiceConfiguration) {
            // in unit test, passed in conf will be AmqpServiceConfiguration
            rocketmqConfig = (RocketMQServiceConfiguration) conf;
        } else {
            // when loaded with PulsarService as NAR, `conf` will be type of ServiceConfiguration
            rocketmqConfig = ConfigurationUtils.create(conf.getProperties(), RocketMQServiceConfiguration.class);
        }
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(rocketmqConfig.getBindAddress());
        this.rocketMQBroker = new RocketMQBrokerController(rocketmqConfig);
        this.rocketMQBroker.initialize();
    }

    @Override
    public String getProtocolDataToAdvertise() {
        if (log.isDebugEnabled()) {
            log.debug("Get configured listeners", rocketmqConfig.getRocketmqListeners());
        }
        return rocketmqConfig.getRocketmqListeners();
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;
        rocketMQBroker.setBrokerService(brokerService);
        log.info("Starting RocketmqProtocolHandler, rop version is: '{}'", RopVersion.getVersion());
        log.info("Git Revision {}", RopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
                RopVersion.getBuildUser(),
                RopVersion.getBuildHost(),
                RopVersion.getBuildTime());
        try {
            rocketMQBroker.start();
        } catch (Exception e) {
            log.error("start rop error.", e);
        }

    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(rocketmqConfig != null);
        checkState(rocketmqConfig.getRocketmqListeners() != null);
        checkState(brokerService != null);
        checkState(rocketMQBroker != null);

        String listeners = rocketmqConfig.getRocketmqListeners();
        String[] parts = listeners.split(LISTENER_DEL);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.builder();

            for (String listener : parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new RocketMQChannelInitializer(rocketmqConfig, rocketMQBroker,
                                    brokerService, false)); // todo: 这里看是否需要定义专门的 RocketmqBrokerService 来包装
                } else {
                    log.error("Rocketmq listener {} not supported. supports {} and {}",
                            listener, PLAINTEXT_PREFIX, SSL_PREFIX);
                }
            }

            return builder.build();
        } catch (Exception e) {
            log.error("RocketmqProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {
        // no-on
    }
}
