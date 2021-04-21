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

import java.util.UUID;

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
