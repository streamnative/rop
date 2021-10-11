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

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<String, RemotingClient> innerClients = new ConcurrentHashMap<>();

    public BrokerNetworkAPI(RopBrokerProxy ropBrokerProxy) {
        this.ropBrokerProxy = ropBrokerProxy;
    }

    private RemotingClient getRemoteClientByConn(String conn) {
        Preconditions.checkArgument(Strings.isNotBlank(conn), "BrokerNetworkApi conn can't be null or empty");
        return innerClients.computeIfAbsent(conn, k -> {
            NettyClientConfig clientConfig = new NettyClientConfig();
            NettyRemotingClient remotingClient = new NettyRemotingClient(clientConfig);
            remotingClient.start();
            log.info("BrokerNetworkAPI internal client[{}] started.", conn);
            return remotingClient;
        });
    }

    public RemotingCommand invokeSync(String conn, RemotingCommand remotingCommand, long timeout)
            throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        return getRemoteClientByConn(conn).invokeSync(conn, remotingCommand, timeout);
    }

    public void invokeAsync(String conn, RemotingCommand remotingCommand, long timeout, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException {
        getRemoteClientByConn(conn).invokeAsync(conn, remotingCommand, timeout, invokeCallback);
    }

    @Override
    public void close() throws Exception {
        innerClients.values().forEach(RemotingClient::shutdown);
    }
}
