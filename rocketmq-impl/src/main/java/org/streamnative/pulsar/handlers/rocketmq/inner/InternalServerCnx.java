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

package org.streamnative.pulsar.handlers.rocketmq.inner;

import java.net.InetSocketAddress;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;

/**
 * InternalServerCnx, this only used to construct internalProducer / internalConsumer.
 */
@Slf4j
public class InternalServerCnx extends ServerCnx {

    @Getter
    RopServerCnx ropServerCnx;

    public InternalServerCnx(RopServerCnx ropServerCnx) {
        super(ropServerCnx.getBrokerController().getBrokerService().pulsar());
        this.ropServerCnx = ropServerCnx;
        // this is the client address that connect to this server.
        this.remoteAddress = ropServerCnx.getRemoteAddress();

        // mock some values, or Producer create will meet NPE.
        // used in test, which will not call channel.active, and not call updateCtx.
        if (this.remoteAddress == null) {
            this.remoteAddress = new InetSocketAddress("localhost", 9999);
        }
    }

    // this will call back by bundle unload
    @Override
    public void closeProducer(Producer producer) {
    }

    // called after channel active
    public void updateCtx() {
        this.remoteAddress = ropServerCnx.getRemoteAddress();
    }

    @Override
    public void enableCnxAutoRead() {
        // do nothing is this mock
    }

    @Override
    public void disableCnxAutoRead() {
        // do nothing is this mock
    }

    @Override
    public void cancelPublishBufferLimiting() {
        // do nothing is this mock
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
