package com.tencent.tdmq.handlers.rocketmq.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetSocketAddress;
import org.apache.pulsar.common.util.Murmur3_32Hash;

public class CommonUtils {

    public static int newBrokerId(final InetSocketAddress address) {
        return Murmur3_32Hash.getInstance().makeHash((address.getHostString() + address.getPort()).getBytes(UTF_8));
    }
}
