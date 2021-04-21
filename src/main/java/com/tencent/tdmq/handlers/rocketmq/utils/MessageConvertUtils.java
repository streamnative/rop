/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tdmq.handlers.rocketmq.utils;

import java.io.UnsupportedEncodingException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

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
