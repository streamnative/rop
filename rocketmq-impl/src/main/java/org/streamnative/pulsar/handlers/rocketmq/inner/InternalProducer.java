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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;

/**
 * InternalProducer.
 */
@Slf4j
public class InternalProducer extends Producer {

    private final ServerCnx cnx;

    public InternalProducer(Topic topic, ServerCnx cnx, long producerId, String producerName,
            Map<String, String> metadata) {
        super(topic, cnx, producerId, producerName, null,
                false, metadata, null, 0, false, ProducerAccessMode.Shared, Optional.empty());
        this.cnx = cnx;
    }

    // this will call back by bundle unload
    @Override
    public CompletableFuture<Void> disconnect() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public ServerCnx getCnx() {
        return cnx;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
