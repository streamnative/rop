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

package com.tencent.tdmq.handlers.rocketmq.inner.format;

import com.tencent.tdmq.handlers.rocketmq.inner.exception.RopEncodeException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Predicate;
import org.apache.pulsar.client.api.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * The formatter for conversion between Kafka records and Bookie entries.
 */
public interface EntryFormatter<T> {

    /**
     * Encode RocketMQ record/Batch records to a ByteBuf.
     *
     * @param record with RocketMQ's format
     * @param numMessages the number of messages
     * @return the ByteBuf of an entry that is to be written to Bookie
     */
    List<byte[]> encode(final T record, final int numMessages) throws RopEncodeException;

    List<MessageExt> decodePulsarMessage(final List<Message> entries, Predicate predicate);

    List<ByteBuffer> decodePulsarMessageResBuffer(final List<Message> entries, Predicate predicate);

    default int parseNumMessages(final T record) {
        return 1;
    }
}
