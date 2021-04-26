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

package com.tencent.tdmq.handlers.rocketmq.inner.producer;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * Producer Manager is responsible for processing producer-related operations.
 */
@Slf4j
public class ProducerManager {

    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private static final int GET_AVAILABLE_CHANNEL_RETRY_COUNT = 3;

    // groupName = [tenant/ns/groupName]
    private final ConcurrentHashMap<ClientGroupName, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Channel> clientIdChannelTable = new ConcurrentHashMap<>();
    private PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    public ProducerManager() {
    }

    public ConcurrentHashMap<ClientGroupName, ConcurrentHashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        return groupChannelTable;
    }

    public void scanNotActiveChannel() {
        for (final Map.Entry<ClientGroupName, ConcurrentHashMap<Channel, ClientChannelInfo>> entry :
                this.groupChannelTable.entrySet()) {
            final String group = entry.getKey().getPulsarGroupName();
            final ConcurrentHashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

            Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Channel, ClientChannelInfo> item = it.next();
                // final Integer id = item.getKey();
                final ClientChannelInfo info = item.getValue();

                long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    it.remove();
                    clientIdChannelTable.remove(info.getClientId());
                    log.warn(
                            "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, "
                                    + "producer group name: {}",
                            RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                    RemotingUtil.closeChannel(info.getChannel());
                }
            }
        }
    }

    public synchronized void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            for (final Map.Entry<ClientGroupName, ConcurrentHashMap<Channel, ClientChannelInfo>> entry :
                    this.groupChannelTable.entrySet()) {
                final String group = entry.getKey().getPulsarGroupName();
                final ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                        entry.getValue();
                final ClientChannelInfo clientChannelInfo =
                        clientChannelInfoTable.remove(channel);
                if (clientChannelInfo != null) {
                    clientIdChannelTable.remove(clientChannelInfo.getClientId());
                    log.info(
                            "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, "
                                    + "producer group: {}",
                            clientChannelInfo.toString(), remoteAddr, group);
                }

            }
        }
    }

    public synchronized void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo clientChannelInfoFound = null;
        ClientGroupName clientGroupName = new ClientGroupName(group);
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(clientGroupName);
        if (null == channelTable) {
            channelTable = new ConcurrentHashMap<>();
            this.groupChannelTable.put(clientGroupName, channelTable);
        }

        clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
        if (null == clientChannelInfoFound) {
            channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            clientIdChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("new producer connected, group: {} channel: {}", clientGroupName,
                    clientChannelInfo.toString());
        }

        if (clientChannelInfoFound != null) {
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    public synchronized void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ClientGroupName clientGroupName = new ClientGroupName(group);
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(clientGroupName);
        if (null != channelTable && !channelTable.isEmpty()) {
            ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
            clientIdChannelTable.remove(clientChannelInfo.getClientId());
            if (old != null) {
                log.info("unregister a producer[{}] from groupChannelTable {}", clientGroupName,
                        clientChannelInfo.toString());
            }

            if (channelTable.isEmpty()) {
                this.groupChannelTable.remove(clientGroupName);
                log.info("unregister a producer group[{}] from groupChannelTable", clientGroupName);
            }
        }
    }

    public Channel getAvaliableChannel(String groupName) {
        if (groupName == null) {
            return null;
        }
        ClientGroupName clientGroupName = new ClientGroupName(groupName);
        List<Channel> channelList = new ArrayList<>();
        ConcurrentHashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable
                .get(clientGroupName);
        if (channelClientChannelInfoHashMap != null) {
            for (Channel channel : channelClientChannelInfoHashMap.keySet()) {
                channelList.add(channel);
            }
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", clientGroupName);
            return null;
        }

        int size = channelList.size();
        if (0 == size) {
            log.warn("Channel list is empty. groupId={}", clientGroupName);
            return null;
        }

        Channel lastActiveChannel = null;

        int index = positiveAtomicCounter.incrementAndGet() % size;
        Channel channel = channelList.get(index);
        int count = 0;
        boolean isOk = channel.isActive() && channel.isWritable();
        while (count++ < GET_AVAILABLE_CHANNEL_RETRY_COUNT) {
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
        return clientIdChannelTable.get(clientId);
    }

    public ClientChannelInfo findChlInfo(String groupName, Channel channel) {
        ClientGroupName clientGroupName = new ClientGroupName(groupName);
        if (groupChannelTable.containsKey(clientGroupName)) {
            return groupChannelTable.get(clientGroupName).get(channel);
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
