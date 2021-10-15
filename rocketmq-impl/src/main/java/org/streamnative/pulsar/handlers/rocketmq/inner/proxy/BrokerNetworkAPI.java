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

package org.streamnative.pulsar.handlers.rocketmq.inner.proxy;

import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.getInnerRemoteClientTag;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * it help broker to access owned-partition data from each other.
 */
@Slf4j
public class BrokerNetworkAPI implements AutoCloseable {

    private final RopBrokerProxy ropBrokerProxy;
    private final Map<String, RemotingClient[]> innerClients = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> innerOpaques = new ConcurrentHashMap<>();
    private final int clientCapacity;


    public BrokerNetworkAPI(RopBrokerProxy ropBrokerProxy, int capacity) {
        this.ropBrokerProxy = ropBrokerProxy;
        this.clientCapacity = capacity > 0 ? capacity : 1;
    }

    private RemotingClient getRemoteClientByConn(String conn, String requestTag) {
        Preconditions.checkArgument(Strings.isNotBlank(conn), "BrokerNetworkApi conn can't be null or empty");
        String requestHashStr =
                (requestTag == null) ? Strings.EMPTY : requestTag;
        int index = Math.abs(requestHashStr.hashCode()) % clientCapacity;
        innerClients.putIfAbsent(conn, new RemotingClient[this.clientCapacity]);
        RemotingClient[] remotingClients = innerClients.get(conn);
        RemotingClient remotingClient = remotingClients[index];
        if (remotingClient == null) {
            synchronized (remotingClients) {
                if (remotingClients[index] == null) {
                    NettyClientConfig clientConfig = new NettyClientConfig();
                    remotingClient = new NettyRemotingClient(clientConfig);
                    remotingClient.start();
                    remotingClients[index] = remotingClient;
                    log.info("BrokerNetworkAPI internal client[{}] started.", conn);
                }
            }
        }
        return remotingClient;
    }

    public RemotingCommand invokeSync(String conn, RemotingCommand remotingCommand, long timeout)
            throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        String innerRemoteClientTag = getInnerRemoteClientTag(remotingCommand);
        return getRemoteClientByConn(conn, innerRemoteClientTag).invokeSync(conn, remotingCommand, timeout);
    }

    public void invokeAsync(String conn, RemotingCommand remotingCommand, long timeout, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException {
        String innerRemoteClientTag = getInnerRemoteClientTag(remotingCommand);
        int opaque = getOpaques(conn);
        remotingCommand.setOpaque(opaque);
        getRemoteClientByConn(conn, innerRemoteClientTag).invokeAsync(conn, remotingCommand, timeout, invokeCallback);
    }

    private int getOpaques(String conn) {
        AtomicInteger atomicLong = innerOpaques.computeIfAbsent(conn, k -> new AtomicInteger());
        return atomicLong.getAndIncrement();
    }

    @Override
    public void close() throws Exception {
        innerClients.values().stream().flatMap(t -> Arrays.stream(t)).forEach((i) -> {
            if (i != null) {
                try {
                    i.shutdown();
                } catch (Exception e) {
                    log.warn("BrokerNetworkAPI shutdown remote client[{}] error.", i, e);
                }
            }
        });
        innerClients.clear();
    }
}
