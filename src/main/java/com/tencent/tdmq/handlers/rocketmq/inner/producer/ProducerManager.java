package com.tencent.tdmq.handlers.rocketmq.inner.producer;

import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

@Slf4j
public class ProducerManager {

    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private static final int GET_AVALIABLE_CHANNEL_RETRY_COUNT = 3;
    /**
     * groupName = [tenant/ns/groupName]
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap<>();
    private PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    public ProducerManager() {
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        return groupChannelTable;
    }

    public void scanNotActiveChannel() {
        for (final Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                .entrySet()) {
            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

            Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Channel, ClientChannelInfo> item = it.next();
                // final Integer id = item.getKey();
                final ClientChannelInfo info = item.getValue();

                long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    it.remove();
                    clientChannelTable.remove(info.getClientId());
                    log.warn(
                            "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                            RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                    RemotingUtil.closeChannel(info.getChannel());
                }
            }
        }
    }

    public synchronized void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            for (final Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                    .entrySet()) {
                final String group = entry.getKey();
                final ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                        entry.getValue();
                final ClientChannelInfo clientChannelInfo =
                        clientChannelInfoTable.remove(channel);
                if (clientChannelInfo != null) {
                    clientChannelTable.remove(clientChannelInfo.getClientId());
                    log.info(
                            "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                            clientChannelInfo.toString(), remoteAddr, group);
                }

            }
        }
    }

    public synchronized void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo clientChannelInfoFound = null;
        String tdmqGroupName = CommonUtils.tdmqGroupName(group);
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(tdmqGroupName);
        if (null == channelTable) {
            channelTable = new ConcurrentHashMap<>();
            this.groupChannelTable.put(tdmqGroupName, channelTable);
        }

        clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
        if (null == clientChannelInfoFound) {
            channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("new producer connected, group: {} channel: {}", tdmqGroupName,
                    clientChannelInfo.toString());
        }

        if (clientChannelInfoFound != null) {
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    public synchronized void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        String tdmqGroupName = CommonUtils.tdmqGroupName(group);
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(tdmqGroupName);
        if (null != channelTable && !channelTable.isEmpty()) {
            ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
            clientChannelTable.remove(clientChannelInfo.getClientId());
            if (old != null) {
                log.info("unregister a producer[{}] from groupChannelTable {}", tdmqGroupName,
                        clientChannelInfo.toString());
            }

            if (channelTable.isEmpty()) {
                this.groupChannelTable.remove(tdmqGroupName);
                log.info("unregister a producer group[{}] from groupChannelTable", tdmqGroupName);
            }
        }
    }

    public Channel getAvaliableChannel(String groupId) {
        if (groupId == null) {
            return null;
        }
        String tdmqGroupName = CommonUtils.tdmqGroupName(groupId);
        List<Channel> channelList = new ArrayList<>();
        ConcurrentHashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(tdmqGroupName);
        if (channelClientChannelInfoHashMap != null) {
            for (Channel channel : channelClientChannelInfoHashMap.keySet()) {
                channelList.add(channel);
            }
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", tdmqGroupName);
            return null;
        }

        int size = channelList.size();
        if (0 == size) {
            log.warn("Channel list is empty. groupId={}", tdmqGroupName);
            return null;
        }

        Channel lastActiveChannel = null;

        int index = positiveAtomicCounter.incrementAndGet() % size;
        Channel channel = channelList.get(index);
        int count = 0;
        boolean isOk = channel.isActive() && channel.isWritable();
        while (count++ < GET_AVALIABLE_CHANNEL_RETRY_COUNT) {
            if (isOk) {
                return channel;
            }
            if (channel.isActive()) {
                lastActiveChannel = channel;
            }
            index = (++index) % size;
            channel = channelList.get(index);
            isOk = channel.isActive() && channel.isWritable();
        }

        return lastActiveChannel;
    }

    public Channel findChannel(String clientId) {
        return clientChannelTable.get(clientId);
    }

    public ClientChannelInfo findChlInfo(String groupName, Channel channel) {
        String tdmqGroupName = CommonUtils.tdmqGroupName(groupName);
        if (Strings.isNotBlank(tdmqGroupName) && groupChannelTable.containsKey(tdmqGroupName)) {
            return groupChannelTable.get(tdmqGroupName).get(channel);
        } else {
            for (Map<Channel, ClientChannelInfo> m : groupChannelTable.values()) {
                if (m.containsKey(channel)) {
                    return m.get(channel);
                }
            }
        }
        return null;
    }
}
