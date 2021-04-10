package com.tencent.tdmq.handlers.rocketmq.utils;

import java.util.UUID;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/4/7 5:54 下午
 */
public class Random {

    public static Long randomLong(int bit) throws Exception {
        if (bit > 16) {
            throw new Exception("bit must <= 16");
        }
        if (bit < 6) {
            throw new Exception("bit must >=6");
        }
        String midStr = "";
        byte[] bytes = UUID.randomUUID().toString().getBytes();
        for (int i = 0; i < bit; i++) {
            midStr += String.valueOf(bytes[i]).toCharArray()[String.valueOf(bytes[i]).toCharArray().length - 1];
        }
        return Long.parseLong(midStr);
    }
}
