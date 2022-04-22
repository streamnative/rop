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

package org.streamnative.pulsar.handlers.rocketmq.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.rocketmq.common.message.MessageDecoder.CHARSET_UTF8;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Common utils class.
 */
@Slf4j
public class CommonUtils {

    public static final String UNDERSCORE_CHAR = "_";
    public static final String PERCENTAGE_CHAR = "%";
    public static final String VERTICAL_LINE_CHAR = "|";
    public static final String SLASH_CHAR = "/";
    public static final String COLO_CHAR = ":";
    public static final String DOT_CHAR = ".";
    private static final int ROP_QUEUE_OFFSET_INDEX = 8 + 4 + 4 + 4 + 4 + 4;
    private static final int ROP_PHYSICAL_OFFSET_INDEX = 8 + 4 + 4 + 4 + 4 + 4 + 8;
    public static final String PULSAR_REAL_PARTITION_ID_TAG = "prpi";
    public static final String ROP_INNER_REMOTE_CLIENT_TAG = "rirc";
    public static final String ROP_TRACE_START_TIME = "rtst";
    public static final String ROP_INNER_MESSAGE_ID = "rimi";
    public static final String ROP_INNER_CLIENT_ADDRESS = "rica";
    public static final String ROP_PROXY_SEND_TIMESTAMP = "rpst";
    public static final String ROP_OWNER_RECEIVE_TIMESTAMP = "rort";
    public static final String ROP_OWNER_FINISH_TIMESTAMP = "roft";
    public static final String ROP_OWNER_COST_TIME = "roct";
    public static final String ROP_MESSAGE_ID = "rop_message_id";
    public static final int ROP_CACHE_INITIAL_SIZE = 1024;
    public static final int ROP_CACHE_MAX_SIZE = 1024 << 8;
    public static final int ROP_CACHE_EXPIRE_TIME_MS = 360 * 1000;
    public static final String DELIVER_AT_TIME_PROPERTY_NAME = "__STARTDELIVERTIME";

    /**
     * @param pulsarTopicName => [tenant/ns/topicName]
     * @return rmqTopicName => [tenant|ns%topicName]
     */
    public static String rmqTopicName(String pulsarTopicName) {
        if (Strings.isBlank(pulsarTopicName)) {
            return Strings.EMPTY;
        }
        List<String> splits = Splitter.on('/').splitToList(pulsarTopicName);
        if (splits.size() >= 3) {
            return splits.get(0) + VERTICAL_LINE_CHAR + splits.get(1) + PERCENTAGE_CHAR + splits.get(2);
        }
        return pulsarTopicName;
    }

    public static String pulsarTopicName(String rmqTopicName) {
        if (Strings.isBlank(rmqTopicName)) {
            return Strings.EMPTY;
        }
        RocketMQTopic rmqTopic = new RocketMQTopic(rmqTopicName);
        return rmqTopic.getOrigNoDomainTopicName();
    }

    public static String pulsarGroupName(String rmqGroupName) {
        return pulsarTopicName(rmqGroupName);
    }

    public static String rmqGroupName(String pulsarGroupName) {
        return rmqTopicName(pulsarGroupName);
    }

    public static int newBrokerId(final InetSocketAddress address) {
        return Murmur3_32Hash.getInstance().makeHash((address.getHostString() + address.getPort()).getBytes(UTF_8));
    }

    public static String createMessageId(final ByteBuffer input, final ByteBuffer addr, final long offset) {
        input.flip();
        int msgIDLength = addr.limit() == 8 ? 16 : 28;
        input.limit(msgIDLength);

        input.put(addr);
        input.putLong(offset);

        return UtilAll.bytes2string(input.array());
    }

    public static String createMessageId(SocketAddress socketAddress,
            int port, long transactionIdhashCode) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        int msgIDLength = inetSocketAddress.getAddress() instanceof Inet4Address ? 16 : 28;
        ByteBuffer byteBuffer = ByteBuffer.allocate(msgIDLength);
        byteBuffer.put(inetSocketAddress.getAddress().getAddress());
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.putLong(transactionIdhashCode);
        byteBuffer.flip();
        return UtilAll.bytes2string(byteBuffer.array());
    }

    public static MessageIdImpl decodeMessageId(final String msgId) throws UnknownHostException {
        long offset;
        int ipLength = msgId.length() == 32 ? 4 * 2 : 16 * 2;

        byte[] port = UtilAll.string2bytes(msgId.substring(ipLength, ipLength + 8));
        ByteBuffer bb = ByteBuffer.wrap(port);
        bb.getInt(0);
        // offset
        byte[] data = UtilAll.string2bytes(msgId.substring(ipLength + 8, ipLength + 8 + 16));
        bb = ByteBuffer.wrap(data);
        offset = bb.getLong(0);

        return MessageIdUtils.getMessageId(offset);
    }

    public static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        return 8 +  //tagsCode
                4 +  //TOTALSIZE
                4 +  //MAGICCODE
                4 +  //BODYCRC
                4 +  //QUEUEID
                4 +  //FLAG
                8 +  //QUEUEOFFSET
                8 +  //PHYSICALOFFSET
                4 +  //SYSFLAG
                8 +  //BORNTIMESTAMP
                bornhostLength +  //BORNHOST
                8 +  //STORETIMESTAMP
                storehostAddressLength +  //STOREHOSTADDRESS
                4 +  //RECONSUMETIMES
                8 +  //Prepared Transaction Offset
                4 + (Math.max(bodyLength, 0)) +  //BODY
                1 + topicLength +  //TOPIC
                2 + (Math.max(propertiesLength, 0));
    }

    public static MessageExt decode(
            ByteBuffer byteBuffer, MessageIdImpl messageId, boolean readBody,
            boolean deCompressBody) {
        return decode(byteBuffer, messageId, readBody, deCompressBody, false);
    }

    public static MessageExt decode(
            ByteBuf byteBuf, boolean readBody,
            final boolean deCompressBody) {
        Preconditions.checkArgument(byteBuf != null && byteBuf.readableBytes() > 0);
        try {
            MessageExt msgExt = new MessageExt();

            // 1 TOTALSIZE
            int storeSize = byteBuf.readInt();
            msgExt.setStoreSize(storeSize);

            // 2 MAGICCODE
            byteBuf.readInt();

            // 3 BODYCRC
            int bodyCRC = byteBuf.readInt();
            msgExt.setBodyCRC(bodyCRC);

            // 4 QUEUEID
            int queueId = byteBuf.readInt();
            msgExt.setQueueId(queueId);

            // 5 FLAG
            int flag = byteBuf.readInt();
            msgExt.setFlag(flag);

            // 6 QUEUEOFFSET
            long queueOffset = byteBuf.readLong();
            msgExt.setQueueOffset(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuf.readLong();
            msgExt.setCommitLogOffset(physicOffset);

            // 8 SYSFLAG
            int sysFlag = byteBuf.readInt();
            msgExt.setSysFlag(sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuf.readLong();
            msgExt.setBornTimestamp(bornTimeStamp);

            // 10 BORNHOST
            int bornhostIPLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 : 16;
            byte[] bornHost = new byte[bornhostIPLength];
            byteBuf.readBytes(bornHost, 0, bornhostIPLength);
            int port = byteBuf.readInt();
            msgExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuf.readLong();
            msgExt.setStoreTimestamp(storeTimestamp);

            // 12 STOREHOST
            int storehostIPLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 : 16;
            byte[] storeHost = new byte[storehostIPLength];
            byteBuf.readBytes(storeHost, 0, storehostIPLength);
            port = byteBuf.readInt();
            msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuf.readInt();
            msgExt.setReconsumeTimes(reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuf.readLong();
            msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

            // 15 BODY
            int bodyLen = byteBuf.readInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byte[] body = new byte[bodyLen];
                    byteBuf.readBytes(body);

                    // uncompress body
                    if (deCompressBody
                            && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
                        body = UtilAll.uncompress(body);
                    }

                    msgExt.setBody(body);
                } else {
                    byteBuf.readerIndex(byteBuf.readerIndex() + bodyLen);
                }
            }

            // 16 TOPIC
            byte topicLen = byteBuf.readByte();
            byte[] topic = new byte[(int) topicLen];
            byteBuf.readBytes(topic);
            msgExt.setTopic(new String(topic, CHARSET_UTF8));

            // 17 properties
            short propertiesLength = byteBuf.readShort();
            if (propertiesLength > 0) {
                byte[] properties = new byte[propertiesLength];
                byteBuf.readBytes(properties);
                String propertiesString = new String(properties, CHARSET_UTF8);
                Map<String, String> map = MessageDecoder.string2messageProperties(propertiesString);
                MessageAccessor.setProperties(msgExt, map);
            }

            int msgIDLength = storehostIPLength + 4 + 8;
            ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
            String msgId = createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(), physicOffset);
            msgExt.setMsgId(msgId);
            return msgExt;
        } catch (IOException ex) {
            log.warn("Decode message error.", ex);
        }
        return null;
    }

    public static MessageExt decode(
            ByteBuffer byteBuffer, final MessageIdImpl messageId, final boolean readBody,
            final boolean deCompressBody,
            final boolean isClient) {
        try {

            MessageExt msgExt = new MessageExt();

            byteBuffer.getLong();
            // 1 TOTALSIZE
            int storeSize = byteBuffer.getInt();
            msgExt.setStoreSize(storeSize);

            // 2 MAGICCODE
            byteBuffer.getInt();

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();
            msgExt.setBodyCRC(bodyCRC);

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();
            msgExt.setQueueId(queueId);

            // 5 FLAG
            int flag = byteBuffer.getInt();
            msgExt.setFlag(flag);

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();
            msgExt.setQueueOffset(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            msgExt.setCommitLogOffset(physicOffset);

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();
            msgExt.setSysFlag(sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            msgExt.setBornTimestamp(bornTimeStamp);

            // 10 BORNHOST
            int bornhostIPLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 : 16;
            byte[] bornHost = new byte[bornhostIPLength];
            byteBuffer.get(bornHost, 0, bornhostIPLength);
            int port = byteBuffer.getInt();
            msgExt.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();
            msgExt.setStoreTimestamp(storeTimestamp);

            // 12 STOREHOST
            int storehostIPLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 : 16;
            byte[] storeHost = new byte[storehostIPLength];
            byteBuffer.get(storeHost, 0, storehostIPLength);
            port = byteBuffer.getInt();
            msgExt.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();
            msgExt.setReconsumeTimes(reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            msgExt.setPreparedTransactionOffset(preparedTransactionOffset);

            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byte[] body = new byte[bodyLen];
                    byteBuffer.get(body);

                    // uncompress body
                    if (deCompressBody
                            && (sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
                        body = UtilAll.uncompress(body);
                    }

                    msgExt.setBody(body);
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            byteBuffer.get(topic);
            msgExt.setTopic(new String(topic, CHARSET_UTF8));

            // 17 properties
            short propertiesLength = byteBuffer.getShort();
            if (propertiesLength > 0) {
                byte[] properties = new byte[propertiesLength];
                byteBuffer.get(properties);
                String propertiesString = new String(properties, CHARSET_UTF8);
                Map<String, String> map = MessageDecoder.string2messageProperties(propertiesString);
                MessageAccessor.setProperties(msgExt, map);
            }

            if (messageId != null) {
                int msgIDLength = storehostIPLength + 4 + 8;
                ByteBuffer byteBufferMsgId = ByteBuffer.allocate(msgIDLength);
                String msgId = createMessageId(byteBufferMsgId, msgExt.getStoreHostBytes(),
                        MessageIdUtils.getOffset(messageId.getLedgerId(), messageId.getEntryId(), queueId));
                msgExt.setMsgId(msgId);
            }

            return msgExt;
        } catch (UnknownHostException e) {
            log.warn("Decode message getByAddress failed.", e);
        } catch (IOException e) {
            log.warn("Decode message uncompress error.", e);
        } catch (Exception e) {
            log.warn("Decode message error.", e);
        }
        byteBuffer.position(byteBuffer.limit());
        return null;
    }

    public static int getPulsarPartitionIdByRequest(RemotingCommand request) {
        String partitionId = request.getExtFields().get(PULSAR_REAL_PARTITION_ID_TAG);
        if (partitionId == null) {
            throw new RuntimeException("Not found partitionId from RemotingCommand extFields.");
        }
        return Integer.parseInt(partitionId);
    }

    public static String getInnerRemoteClientTag(RemotingCommand remotingCommand) {
        String tag = remotingCommand.getExtFields().get(ROP_INNER_REMOTE_CLIENT_TAG);
        return tag == null ? Strings.EMPTY : tag;
    }

    public static String getInnerProducerGroupName(RemotingCommand remotingCommand, String defaultGroupName) {
        String tag = remotingCommand.getExtFields().get(ROP_INNER_REMOTE_CLIENT_TAG);
        if (Strings.isNotBlank(tag)) {
            return tag;
        }
        return defaultGroupName;
    }

    public static boolean isFromProxy(RemotingCommand remotingCommand) {
        return remotingCommand.getExtFields().containsKey(ROP_INNER_REMOTE_CLIENT_TAG);
    }
}
