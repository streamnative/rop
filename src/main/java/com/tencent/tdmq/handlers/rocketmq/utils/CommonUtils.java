package com.tencent.tdmq.handlers.rocketmq.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

public class CommonUtils {

    public static int newBrokerId(final InetSocketAddress address) {
        return Murmur3_32Hash.getInstance().makeHash((address.getHostString() + address.getPort()).getBytes(UTF_8));
    }

    public static String createMessageId(long ledgerId, long entryId, long partitionId, int batchSize) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(28);
        byteBuffer.putLong(ledgerId);
        byteBuffer.putLong(entryId);
        byteBuffer.putLong(partitionId);
        byteBuffer.putInt(batchSize);
        byteBuffer.flip();
        return UtilAll.bytes2string(byteBuffer.array());
    }

    public static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        final int msgLen = 8 //tagsCode
                + 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + bornhostLength //BORNHOST
                + 8 //STORETIMESTAMP
                + storehostAddressLength //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }
}
