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

import static org.apache.pulsar.common.protocol.Commands.hasChecksum;

import com.google.common.base.Preconditions;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import com.tencent.tdmq.handlers.rocketmq.inner.exception.RopEncodeException;
import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.MessageExtBrokerInner;

/**
 * Rop entry formatter.
 */
@Slf4j
public class RopEntryFormatter implements EntryFormatter<MessageExt> {

    // The maximum size of message,default is 4M
    private static final int MAX_MESSAGE_SIZE = 1024 * 1024 * 4;
    private static final ThreadLocal<ByteBuffer> msgStoreItemMemoryThreadLocal = ThreadLocal
            .withInitial(() -> ByteBuffer.allocate(MAX_MESSAGE_SIZE));

    public static MessageExt decodePulsarMessage(Message<byte[]> message) {
        if (message instanceof TopicMessageImpl) {
            TopicMessageImpl topicMessage = (TopicMessageImpl) message;
            return CommonUtils.decode(ByteBuffer.wrap(message.getData()),
                    (MessageIdImpl) topicMessage.getInnerMessageId(), true, false);
        } else {
            return CommonUtils.decode(ByteBuffer.wrap(message.getData()),
                    (MessageIdImpl) message.getMessageId(), true, false);
        }
    }

    public static ByteBuffer decodePulsarMessageResBuffer(Message<byte[]> message) {
        return CommonUtils.decode(message);
    }

    @Override
    public List<byte[]> encode(MessageExt record, int numMessages) throws RopEncodeException {
        Preconditions.checkNotNull(record);
        if (record instanceof MessageExtBrokerInner) {
            MessageExtBrokerInner mesg = (MessageExtBrokerInner) record;
            String tags = mesg.getProperty(MessageConst.PROPERTY_TAGS);
            long tagsCode = 0L;
            if (tags != null && tags.length() > 0) {
                tagsCode = MessageExtBrokerInner
                        .tagsString2tagsCode(MessageExt.parseTopicFilterType(mesg.getSysFlag()), tags);
            }
            byte[] msgBytes = convertRocketmq2Pulsar(tagsCode, mesg);
            return Collections.singletonList(msgBytes);
        } else if (record instanceof MessageExtBatch) {
            MessageExtBatch msg = (MessageExtBatch) record;

            return convertRocketmq2Pulsar(msg);
        }
        throw new RopEncodeException("UNKNOWN Message Type");
    }

    private boolean verifyChecksum(ByteBuf headersAndPayload, MessageIdData messageId) {

        if (hasChecksum(headersAndPayload)) {
            int checksum = Commands.readChecksum(headersAndPayload);
            int computedChecksum = Crc32cIntChecksum.computeChecksum(headersAndPayload);
            if (checksum != computedChecksum) {
                log.error(
                        "Checksum mismatch for message at {}:{}. Received checksum: 0x{}, Computed checksum: 0x{}",
                        messageId.getLedgerId(), messageId.getEntryId(),
                        Long.toHexString(checksum), Integer.toHexString(computedChecksum));
                return false;
            }
        }

        return true;
    }

    @Override
    public List<MessageExt> decodePulsarMessage(List<Message<byte[]>> messages, Predicate predicate) {
        if (predicate != null) {
            return messages.stream().filter((Predicate<Message>) predicate).map(RopEntryFormatter::decodePulsarMessage)
                    .collect(Collectors.toList());
        } else {
            return messages.stream().map(RopEntryFormatter::decodePulsarMessage).collect(Collectors.toList());
        }
    }

    @Override
    public List<ByteBuffer> decodePulsarMessageResBuffer(List<Message<byte[]>> messages,
            Predicate predicate) {//Message in pulsar
        if (messages == null || messages.isEmpty()) {
            return Collections.emptyList();
        }
        if (predicate != null) {
            return messages.stream().filter((Predicate<Message>) predicate)
                    .map(RopEntryFormatter::decodePulsarMessageResBuffer)
                    .collect(Collectors.toList());
        } else {
            return messages.stream().map(RopEntryFormatter::decodePulsarMessageResBuffer).collect(Collectors.toList());
        }
    }

    private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
        byteBuffer.clear();
    }

    private List<byte[]> convertRocketmq2Pulsar(final MessageExtBatch messageExtBatch) throws RopEncodeException {
        ByteBuffer msgStoreItemMemory = msgStoreItemMemoryThreadLocal.get();
        List<ByteBuffer> result = new ArrayList<>();
        int totalMsgLen = 0;
        ByteBuffer messagesByteBuff = messageExtBatch.wrap();
        int sysFlag = messageExtBatch.getSysFlag();
        int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
        ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

        msgStoreItemMemory.clear();
        while (messagesByteBuff.hasRemaining()) {
            // 1 TOTALSIZE
            messagesByteBuff.getInt();
            // 2 MAGICCODE
            messagesByteBuff.getInt();
            // 3 BODYCRC
            messagesByteBuff.getInt();
            // 4 FLAG
            int flag = messagesByteBuff.getInt();
            // 5 BODY
            int bodyLen = messagesByteBuff.getInt();
            int bodyPos = messagesByteBuff.position();
            int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
            messagesByteBuff.position(bodyPos + bodyLen);
            // 6 properties
            short propertiesLen = messagesByteBuff.getShort();
            int propertiesPos = messagesByteBuff.position();
            byte[] propStr = new byte[propertiesLen];
            messagesByteBuff.get(propStr);
            //messagesByteBuff.position(propertiesPos + propertiesLen);

            final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

            final int topicLength = topicData.length;

            final int msgLen = CommonUtils
                    .calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, propertiesLen);

            // Exceeds the maximum message
            if (msgLen > MAX_MESSAGE_SIZE) {
                log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + MAX_MESSAGE_SIZE);
                throw new RopEncodeException("message size exceeded");
            }

            totalMsgLen += msgLen;
            // Determines whether there is sufficient free space
            if (totalMsgLen > MAX_MESSAGE_SIZE) {
                throw new RopEncodeException("message size exceeded");
            }

            long tagsCode = 0L;
            if (propertiesLen > 0) {
                String properties = new String(propStr, MessageDecoder.CHARSET_UTF8);
                Map<String, String> propertiesMap = MessageDecoder.string2messageProperties(properties);
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner
                            .tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }
            }

            ByteBuffer tempBuffer = msgStoreItemMemory.slice();
            //TAGSCODE
            msgStoreItemMemory.putLong(tagsCode);
            // 1 TOTALSIZE
            msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            msgStoreItemMemory.putInt(bodyCrc);
            // 4 QUEUEID
            msgStoreItemMemory.putInt(messageExtBatch.getQueueId());
            // 5 FLAG
            msgStoreItemMemory.putInt(flag);
            // 6 QUEUEOFFSET
            msgStoreItemMemory.putLong(0);
            // 7 PHYSICALOFFSET
            msgStoreItemMemory.putLong(0);
            // 8 SYSFLAG
            msgStoreItemMemory.putInt(messageExtBatch.getSysFlag());
            // 9 BORNTIMESTAMP
            msgStoreItemMemory.putLong(messageExtBatch.getBornTimestamp());
            // 10 BORNHOST
            resetByteBuffer(bornHostHolder, bornHostLength);
            msgStoreItemMemory.put(messageExtBatch.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP
            msgStoreItemMemory.putLong(Instant.now().toEpochMilli());
            // 12 STOREHOSTADDRESS
            resetByteBuffer(storeHostHolder, storeHostLength);
            msgStoreItemMemory.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
            // 13 RECONSUMETIMES
            msgStoreItemMemory.putInt(messageExtBatch.getReconsumeTimes());
            // 14 Prepared Transaction Offset, batch does not support transaction
            msgStoreItemMemory.putLong(0);
            // 15 BODY
            msgStoreItemMemory.putInt(bodyLen);
            if (bodyLen > 0) {
                msgStoreItemMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
            }
            // 16 TOPIC
            msgStoreItemMemory.put((byte) topicLength);
            msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            msgStoreItemMemory.putShort(propertiesLen);
            if (propertiesLen > 0) {
                msgStoreItemMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            tempBuffer.limit(msgStoreItemMemory.position());
            result.add(tempBuffer);
        }
        return result.stream().collect(ArrayList::new, (arr, item) -> {
            byte[] msgBytes = new byte[item.limit()];
            item.get(msgBytes);
            arr.add(msgBytes);
        }, ArrayList::addAll);
    }


    private byte[] convertRocketmq2Pulsar(long tagsCode, MessageExtBrokerInner msgInner) throws RopEncodeException {
        int sysflag = msgInner.getSysFlag();
        int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
        ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
        ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);
        this.resetByteBuffer(storeHostHolder, storeHostLength);

        ByteBuffer msgStoreItemMemory = msgStoreItemMemoryThreadLocal.get();
        msgStoreItemMemory.clear();
        final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null
                        : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
        final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

        if (propertiesLength > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long. length={}", propertiesData.length);
            throw new RopEncodeException(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED.toString());
        }

        final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        final int topicLength = topicData.length;
        final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;
        final int msgLen = CommonUtils.calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

        // Exceeds the maximum message
        if (msgLen > MAX_MESSAGE_SIZE) {
            log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + MAX_MESSAGE_SIZE);
            throw new RopEncodeException(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED.toString());
        }

        // Initialization of storage space
        this.resetByteBuffer(msgStoreItemMemory, msgLen);
        // TAGSCODE
        msgStoreItemMemory.putLong(tagsCode);
        // 1 TOTALSIZE
        msgStoreItemMemory.putInt(msgLen);
        // 2 MAGICCODE
        msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
        // 3 BODYCRC
        msgStoreItemMemory.putInt(msgInner.getBodyCRC());
        // 4 QUEUEID
        msgStoreItemMemory.putInt(msgInner.getQueueId());
        // 5 FLAG
        msgStoreItemMemory.putInt(msgInner.getFlag());
        // 6 QUEUEOFFSET
        msgStoreItemMemory.putLong(0L);
        // 7 PHYSICALOFFSET
        msgStoreItemMemory.putLong(0L);
        // 8 SYSFLAG
        msgStoreItemMemory.putInt(msgInner.getSysFlag());
        // 9 BORNTIMESTAMP
        msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
        // 10 BORNHOST
        resetByteBuffer(bornHostHolder, bornHostLength);
        msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
        // 11 STORETIMESTAMP
        msgStoreItemMemory.putLong(Instant.now().toEpochMilli());
        // 12 STOREHOSTADDRESS
        resetByteBuffer(storeHostHolder, storeHostLength);
        msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
        // 13 RECONSUMETIMES
        msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
        // 14 Prepared Transaction Offset
        msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
        // 15 BODY
        msgStoreItemMemory.putInt(bodyLength);
        if (bodyLength > 0) {
            msgStoreItemMemory.put(msgInner.getBody());
        }
        // 16 TOPIC
        msgStoreItemMemory.put((byte) topicLength);
        msgStoreItemMemory.put(topicData);
        // 17 PROPERTIES
        msgStoreItemMemory.putShort((short) propertiesLength);
        if (propertiesLength > 0) {
            msgStoreItemMemory.put(propertiesData);
        }
        // Write messages to the queue buffer
        msgStoreItemMemory.flip();
        byte[] msgBytes = new byte[msgStoreItemMemory.limit()];
        msgStoreItemMemory.get(msgBytes);
        return msgBytes;
    }
}
