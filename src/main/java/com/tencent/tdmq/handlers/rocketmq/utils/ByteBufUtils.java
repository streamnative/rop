package com.tencent.tdmq.handlers.rocketmq.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/27 1:35 下午
 */
public class ByteBufUtils {

    public static ByteBuffer getKeyByteBuffer(SingleMessageMetadata messageMetadata) {
        if (messageMetadata.hasOrderingKey()) {
            return ByteBuffer.wrap(messageMetadata.getOrderingKey().toByteArray()).asReadOnlyBuffer();
        }

        if (messageMetadata.hasPartitionKey()) {
            final String key = messageMetadata.getPartitionKey();
            if (messageMetadata.hasPartitionKeyB64Encoded()) {
                return ByteBuffer.wrap(Base64.getDecoder().decode(key)).asReadOnlyBuffer();
            } else {
                // for Base64 not encoded string, convert to UTF_8 chars
                return ByteBuffer.wrap(key.getBytes(UTF_8));
            }
        } else {
            return ByteBuffer.allocate(0);
        }
    }

    public static ByteBuffer getKeyByteBuffer(MessageMetadata messageMetadata) {
        if (messageMetadata.hasOrderingKey()) {
            return ByteBuffer.wrap(messageMetadata.getOrderingKey().toByteArray()).asReadOnlyBuffer();
        }

        String key = messageMetadata.getPartitionKey();
        if (messageMetadata.hasPartitionKeyB64Encoded()) {
            return ByteBuffer.wrap(Base64.getDecoder().decode(key));
        } else {
            // for Base64 not encoded string, convert to UTF_8 chars
            return ByteBuffer.wrap(key.getBytes(UTF_8));
        }
    }

    public static ByteBuffer getNioBuffer(ByteBuf buffer) {
        if (buffer.isDirect()) {
            return buffer.nioBuffer();
        }
        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        return ByteBuffer.wrap(bytes);
    }

}
