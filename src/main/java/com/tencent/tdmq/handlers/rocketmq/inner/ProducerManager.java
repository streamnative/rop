package com.tencent.tdmq.handlers.rocketmq.inner;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

@Slf4j
public class ProducerManager {

    private static final long CHANNEL_EXPIRED_TIMEOUT = 120000L;
    private static final int GET_AVALIABLE_CHANNEL_RETRY_COUNT = 3;
    private final ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable = new ConcurrentHashMap();
    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap();
    private PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    public ProducerManager() {
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        return this.groupChannelTable;
    }

    public void scanNotActiveChannel() {
        Iterator var1 = this.groupChannelTable.entrySet().iterator();

        while (var1.hasNext()) {
            Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry = (Entry) var1.next();
            String group = (String) entry.getKey();
            ConcurrentHashMap<Channel, ClientChannelInfo> chlMap = (ConcurrentHashMap) entry.getValue();
            Iterator it = chlMap.entrySet().iterator();

            while (it.hasNext()) {
                Entry<Channel, ClientChannelInfo> item = (Entry) it.next();
                ClientChannelInfo info = (ClientChannelInfo) item.getValue();
                long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                if (diff > 120000L) {
                    it.remove();
                    this.clientChannelTable.remove(info.getClientId());
                    log.warn(
                            "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                            RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                    RemotingUtil.closeChannel(info.getChannel());
                }
            }
        }

    }

    public synchronized void doChannelCloseEvent(String remoteAddr, Channel channel) {
        if (channel != null) {
            Iterator var3 = this.groupChannelTable.entrySet().iterator();

            while (var3.hasNext()) {
                Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry = (Entry) var3.next();
                String group = (String) entry.getKey();
                ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable = (ConcurrentHashMap) entry
                        .getValue();
                ClientChannelInfo clientChannelInfo = (ClientChannelInfo) clientChannelInfoTable.remove(channel);
                if (clientChannelInfo != null) {
                    this.clientChannelTable.remove(clientChannelInfo.getClientId());
                    log.info(
                            "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                            new Object[]{clientChannelInfo.toString(), remoteAddr, group});
                }
            }
        }

    }

    public synchronized void registerProducer(String group, ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo clientChannelInfoFound = null;
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = (ConcurrentHashMap) this.groupChannelTable
                .get(group);
        if (null == channelTable) {
            channelTable = new ConcurrentHashMap();
            this.groupChannelTable.put(group, channelTable);
        }

        clientChannelInfoFound = (ClientChannelInfo) channelTable.get(clientChannelInfo.getChannel());
        if (null == clientChannelInfoFound) {
            channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            this.clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("new producer connected, group: {} channel: {}", group, clientChannelInfo.toString());
        }

        if (clientChannelInfoFound != null) {
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }

    }

    public synchronized void unregisterProducer(String group, ClientChannelInfo clientChannelInfo) {
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = (ConcurrentHashMap) this.groupChannelTable
                .get(group);
        if (null != channelTable && !channelTable.isEmpty()) {
            ClientChannelInfo old = (ClientChannelInfo) channelTable.remove(clientChannelInfo.getChannel());
            this.clientChannelTable.remove(clientChannelInfo.getClientId());
            if (old != null) {
                log.info("unregister a producer[{}] from groupChannelTable {}", group, clientChannelInfo.toString());
            }

            if (channelTable.isEmpty()) {
                this.groupChannelTable.remove(group);
                log.info("unregister a producer group[{}] from groupChannelTable", group);
            }
        }

    }

    public Channel getAvaliableChannel(String groupId) {
        if (groupId == null) {
            return null;
        } else {
            List<Channel> channelList = new ArrayList();
            ConcurrentHashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = (ConcurrentHashMap) this.groupChannelTable
                    .get(groupId);
            if (channelClientChannelInfoHashMap == null) {
                log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
                return null;
            } else {
                Iterator var4 = channelClientChannelInfoHashMap.keySet().iterator();

                Channel lastActiveChannel;
                while (var4.hasNext()) {
                    lastActiveChannel = (Channel) var4.next();
                    channelList.add(lastActiveChannel);
                }

                int size = channelList.size();
                if (0 == size) {
                    log.warn("Channel list is empty. groupId={}", groupId);
                    return null;
                } else {
                    lastActiveChannel = null;
                    int index = this.positiveAtomicCounter.incrementAndGet() % size;
                    Channel channel = (Channel) channelList.get(index);
                    int count = 0;

                    for (boolean isOk = channel.isActive() && channel.isWritable(); count++ < 3;
                            isOk = channel.isActive() && channel.isWritable()) {
                        if (isOk) {
                            return channel;
                        }

                        if (channel.isActive()) {
                            lastActiveChannel = channel;
                        }

                        ++index;
                        index %= size;
                        channel = (Channel) channelList.get(index);
                    }

                    return lastActiveChannel;
                }
            }
        }
    }

    public Channel findChannel(String clientId) {
        return (Channel) this.clientChannelTable.get(clientId);
    }
}
