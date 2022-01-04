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

package org.streamnative.pulsar.handlers.rocketmq.inner.consumer;

import static org.streamnative.pulsar.handlers.rocketmq.inner.proxy.RopBrokerProxy.INNER_CLIENT_NAME_PREFIX;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.streamnative.pulsar.handlers.rocketmq.inner.RopClientChannelCnx;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupAndTopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.producer.ClientGroupName;
import org.streamnative.pulsar.handlers.rocketmq.metrics.RopMetricsGroup;
import org.streamnative.pulsar.handlers.rocketmq.metrics.RopYammerMetrics;

/**
 * Consumer manager service.
 */
@Slf4j
public class ConsumerManager extends RopMetricsGroup {

    private static final long CHANNEL_EXPIRED_TIMEOUT = 120000L;
    private final RocketMQBrokerController brokerController;
    private final ConcurrentHashMap<ClientGroupName, ConsumerGroupInfo> consumerTable = new ConcurrentHashMap<>(1024);
    //ConsumerIdsChangeListener groupName is rocketmq groupName example: tenant|ns%topicName; %RETRY%tenant|ns%topicName
    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    public ConsumerManager(final RocketMQBrokerController brokerController,
            ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.brokerController = brokerController;
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }

    public void start() {
        brokerController.getBrokerService().getPulsar().addPrometheusRawMetricsProvider(this);
    }

    public ClientChannelInfo findChannel(String group, String clientId) {
        ClientGroupName clientGroupName = new ClientGroupName(group);
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(clientGroupName);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(clientId);
        }
        return null;
    }

    public SubscriptionData findSubscriptionData(String group, String topic) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        return consumerGroupInfo != null ? consumerGroupInfo.findSubscriptionData(topic) : null;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(String group) {
        ClientGroupName clientGroupName = new ClientGroupName(group);
        return this.consumerTable.get(clientGroupName);
    }

    public boolean registerProxyRequestConsumer(String proxyConsumerGroup, String ropConsumerGroup,
            RocketMQBrokerController brokerController, ChannelHandlerContext ctx) {
        ClientGroupName clientGroupName = new ClientGroupName(proxyConsumerGroup);
        ConsumerGroupInfo consumerGroupInfo = consumerTable.get(clientGroupName);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(ropConsumerGroup,
                    ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING,
                    ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            ConsumerGroupInfo prev = consumerTable.putIfAbsent(clientGroupName, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
        ClientChannelInfo clientChannelInfo = channelInfoTable.get(ctx.channel());
        if (clientChannelInfo == null) {
            String clientId =
                    ctx.channel().remoteAddress().toString() + "@" + System.currentTimeMillis();
            clientChannelInfo = new RopClientChannelCnx(brokerController, ctx, clientId,
                    LanguageCode.JAVA, 0);
            channelInfoTable.putIfAbsent(ctx.channel(), clientChannelInfo);
        } else {
            clientChannelInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        }
        return channelInfoTable.get(ctx.channel()) != null;
    }

    public int findSubscriptionDataCount(String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        return consumerGroupInfo != null ? consumerGroupInfo.getSubscriptionTable().size() : 0;
    }

    public void doChannelCloseEvent(String remoteAddr, Channel channel) {

        for (Entry<ClientGroupName, ConsumerGroupInfo> clientGroupNameConsumerGroupInfoEntry : this.consumerTable
                .entrySet()) {
            ConsumerGroupInfo info = clientGroupNameConsumerGroupInfoEntry.getValue();
            boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
            if (removed) {
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove =
                            this.consumerTable.remove(clientGroupNameConsumerGroupInfoEntry.getKey());
                    if (remove != null) {
                        log.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                                clientGroupNameConsumerGroupInfoEntry.getKey().getPulsarGroupName());
                        this.consumerIdsChangeListener
                                .handle(ConsumerGroupEvent.UNREGISTER,
                                        clientGroupNameConsumerGroupInfoEntry.getKey().getRmqGroupName());
                    }
                }

                this.consumerIdsChangeListener
                        .handle(ConsumerGroupEvent.CHANGE,
                                clientGroupNameConsumerGroupInfoEntry.getKey().getRmqGroupName(),
                                info.getAllChannel());
            }
        }

    }

    public boolean registerConsumer(String group, ClientChannelInfo clientChannelInfo, ConsumeType consumeType,
            MessageModel messageModel, ConsumeFromWhere consumeFromWhere, Set<SubscriptionData> subList,
            boolean isNotifyConsumerIdsChangedEnable) {
        ClientGroupName clientGroupName = new ClientGroupName(group);
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(clientGroupName);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(clientGroupName, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        boolean r1 = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel, consumeFromWhere);
        boolean r2 = consumerGroupInfo.updateSubscription(subList);
        if ((r1 || r2) && isNotifyConsumerIdsChangedEnable) {
            this.consumerIdsChangeListener
                    .handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
        }

        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, group, subList);
        return r1 || r2;
    }

    public void unregisterConsumer(String group, ClientChannelInfo clientChannelInfo,
            boolean isNotifyConsumerIdsChangedEnable) {
        ClientGroupName clientGroupName = new ClientGroupName(group);
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(clientGroupName);
        if (null != consumerGroupInfo) {
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                ConsumerGroupInfo remove = this.consumerTable.remove(clientGroupName);
                if (remove != null) {
                    log.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                            clientGroupName);
                    this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, group, 0);
                }
            }

            if (isNotifyConsumerIdsChangedEnable) {
                this.consumerIdsChangeListener
                        .handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }

    }

    public void scanNotActiveChannel() {
        Iterator<Entry<ClientGroupName, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<ClientGroupName, ConsumerGroupInfo> next = it.next();
            ClientGroupName group = next.getKey();

            // TODO: hanmz 2021/12/20 skip proxy channel expire check
            if (group.getRmqGroupName().startsWith(INNER_CLIENT_NAME_PREFIX)) {
                continue;
            }

            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();

            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    log.warn("SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, "
                                    + "consumerGroup={}",
                            RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                }
            }

            if (channelInfoTable.isEmpty()) {
                log.warn("SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                        group);
                it.remove();
            }
        }

    }

    public HashSet<String> queryTopicConsumeByWho(String topic) {
        HashSet<String> groups = new HashSet<>();

        for (Entry<ClientGroupName, ConsumerGroupInfo> entry : this.consumerTable.entrySet()) {
            ConcurrentMap<String, SubscriptionData> subscriptionTable = entry.getValue().getSubscriptionTable();
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey().getRmqGroupName());
            }
        }
        return groups;
    }

    @Override
    public void generate(SimpleTextOutputStream stream) {
        log.info("RoP generate consumer relate metrics.");

        for (Entry<ClientGroupName, ConsumerGroupInfo> entry : consumerTable.entrySet()) {
            ClientGroupName clientGroupName = entry.getKey();
            ConsumerGroupInfo consumerGroupInfo = entry.getValue();

            String ropGroupName = clientGroupName.getRmqGroupName();
            if (ropGroupName.startsWith(INNER_CLIENT_NAME_PREFIX)) {
                continue;
            }

            if (!brokerController.getRopBrokerProxy().getZkService()
                    .isGroupExist(clientGroupName.getPulsarGroupName())) {
                continue;
            }

            for (Entry<String, SubscriptionData> subEntry : consumerGroupInfo.getSubscriptionTable().entrySet()) {
                String ropTopicName = subEntry.getKey();
                ClientGroupAndTopicName clientGroupAndTopicName = new ClientGroupAndTopicName(ropGroupName,
                        ropTopicName);

                String pulsarGroupName = clientGroupAndTopicName.getClientGroupName().getPulsarGroupName();
                String pulsarTopicName = clientGroupAndTopicName.getClientTopicName().getPulsarTopicName();

                stream.write(RopYammerMetrics.ROP_CONSUMERS_COUNT)
                        .write("{cluster=\"").write(this.brokerController.getRopClusterName())
                        .write("\",topic=\"").write(pulsarTopicName)
                        .write("\",group=\"").write(pulsarGroupName).write("\"} ");
                stream.write(consumerGroupInfo.consumerCount()).write(' ').write(System.currentTimeMillis())
                        .write('\n');
            }
        }
    }
}

