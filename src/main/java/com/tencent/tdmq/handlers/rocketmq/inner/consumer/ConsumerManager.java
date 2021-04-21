package com.tencent.tdmq.handlers.rocketmq.inner.consumer;

import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import io.netty.channel.Channel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

@Slf4j
public class ConsumerManager {

    private static final long CHANNEL_EXPIRED_TIMEOUT = 120000L;
    private final ConcurrentMap<String, ConsumerGroupInfo> consumerTable = new ConcurrentHashMap(1024);
    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    public ConsumerManager(ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }

    public ClientChannelInfo findChannel(String group, String clientId) {
        String tdmqGroupName = CommonUtils.tdmqGroupName(group);
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(tdmqGroupName);
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
        String tdmqGroupName = CommonUtils.tdmqGroupName(group);
        return this.consumerTable.get(tdmqGroupName);
    }

    public int findSubscriptionDataCount(String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        return consumerGroupInfo != null ? consumerGroupInfo.getSubscriptionTable().size() : 0;
    }

    public void doChannelCloseEvent(String remoteAddr, Channel channel) {
        Iterator it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = (Entry) it.next();
            ConsumerGroupInfo info = next.getValue();
            boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
            if (removed) {
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        log.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                                next.getKey());
                        this.consumerIdsChangeListener
                                .handle(ConsumerGroupEvent.UNREGISTER, next.getKey(), new Object[0]);
                    }
                }

                this.consumerIdsChangeListener
                        .handle(ConsumerGroupEvent.CHANGE, next.getKey(), new Object[]{info.getAllChannel()});
            }
        }

    }

    public boolean registerConsumer(String group, ClientChannelInfo clientChannelInfo, ConsumeType consumeType,
            MessageModel messageModel, ConsumeFromWhere consumeFromWhere, Set<SubscriptionData> subList,
            boolean isNotifyConsumerIdsChangedEnable) {
        String tdmqGroupName = CommonUtils.tdmqGroupName(group);
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(tdmqGroupName);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(tdmqGroupName, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(tdmqGroupName, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        boolean r1 = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel, consumeFromWhere);
        boolean r2 = consumerGroupInfo.updateSubscription(subList);
        if ((r1 || r2) && isNotifyConsumerIdsChangedEnable) {
            this.consumerIdsChangeListener
                    .handle(ConsumerGroupEvent.CHANGE, tdmqGroupName, new Object[]{consumerGroupInfo.getAllChannel()});
        }

        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, tdmqGroupName, new Object[]{subList});
        return r1 || r2;
    }

    public void unregisterConsumer(String group, ClientChannelInfo clientChannelInfo,
            boolean isNotifyConsumerIdsChangedEnable) {
        String tdmqGroupName = CommonUtils.tdmqGroupName(group);
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(tdmqGroupName);
        if (null != consumerGroupInfo) {
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                ConsumerGroupInfo remove = this.consumerTable.remove(tdmqGroupName);
                if (remove != null) {
                    log.info("unregister consumer ok, no any connection, and remove consumer group, {}", tdmqGroupName);
                    this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, tdmqGroupName, new Object[0]);
                }
            }

            if (isNotifyConsumerIdsChangedEnable) {
                this.consumerIdsChangeListener
                        .handle(ConsumerGroupEvent.CHANGE, tdmqGroupName,
                                new Object[]{consumerGroupInfo.getAllChannel()});
            }
        }

    }

    public void scanNotActiveChannel() {
        Iterator it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = (Entry) it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
            Iterator itChannel = channelInfoTable.entrySet().iterator();

            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = (Entry) itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    log.warn(
                            "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
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
        HashSet<String> groups = new HashSet();
        Iterator it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> entry = (Entry) it.next();
            ConcurrentMap<String, SubscriptionData> subscriptionTable = entry.getValue().getSubscriptionTable();
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }
        return groups;
    }
}

