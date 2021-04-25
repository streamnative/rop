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

package com.tencent.tdmq.handlers.rocketmq;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.utils.ConfigurationUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
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

@Slf4j
@Data
public class RocketMQProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "rocketmq";
    public static final String SSL_PREFIX = "SSL://";
    public static final String PLAINTEXT_PREFIX = "rocketmq://";
    public static final String LISTENER_DEL = ",";
    public static final String LISTENER_PATTEN = "^(rocketmq?|SSL)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*:([0-9]+)";

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
        RocketMQTopic.init(rocketmqConfig.getRocketmqMetadataTenant(), rocketmqConfig.getRocketmqMetadataNamespace(),
                rocketmqConfig.getRocketmqTenant(), rocketmqConfig.getRocketmqNamespace());
    }

    @Override
    public String getProtocolDataToAdvertise() {
        if (log.isDebugEnabled()) {
            log.debug("Get configured listeners:{}", rocketmqConfig.getRocketmqListeners());
        }
        return rocketmqConfig.getRocketmqListeners();
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;
        rocketMQBroker.setBrokerService(service);
//        log.info("Starting RocketmqProtocolHandler, listener: {}, rop version is: '{}'",
//                rocketmqConfig.getRocketmqListeners(), RopVersion.getVersion());
//        log.info("Git Revision {}", RopVersion.getGitSha());
//        log.info("Built by {} on {} at {}",
//                RopVersion.getBuildUser(),
//                RopVersion.getBuildHost(),
//                RopVersion.getBuildTime());
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
                                    brokerService, false));
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
