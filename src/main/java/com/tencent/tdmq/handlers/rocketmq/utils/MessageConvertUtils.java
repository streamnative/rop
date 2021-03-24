package com.tencent.tdmq.handlers.rocketmq.utils;

import java.io.UnsupportedEncodingException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 12:07 下午
 */


/**
 * Util for convert message between TDMQ and RocketMQ.
 */
@UtilityClass
@Slf4j
public class MessageConvertUtils {

    private static final String DEFAULT_CHARSET_NAME = "ISO8859-1";

    private static String byteToString(byte b) throws UnsupportedEncodingException {
        byte[] bytes = {b};
        return new String(bytes, DEFAULT_CHARSET_NAME);
    }

    private static Byte stringToByte(String string) throws UnsupportedEncodingException {
        if (string != null && string.length() > 0) {
            return string.getBytes(DEFAULT_CHARSET_NAME)[0];
        }
        return null;
    }

}
