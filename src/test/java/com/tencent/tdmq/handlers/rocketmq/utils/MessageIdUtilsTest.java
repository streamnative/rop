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

package com.tencent.tdmq.handlers.rocketmq.utils;

import static com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils.getMessageId;
import static com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils.getOffset;
import static org.junit.Assert.assertEquals;

import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Test;

/**
 * Test messageID utils.
 */
public class MessageIdUtilsTest {

    @Test
    public void testGetOffset() {
        long offset = getOffset(1897, -1);
        assertEquals(124387327, offset);
        MessageIdImpl messageId = getMessageId(offset);
        assertEquals(1897, messageId.getLedgerId());
        assertEquals(-1, messageId.getEntryId());
    }

    public void testTestGetOffset() {
    }

    public void testGetMessageId() {
    }

    public void testGetPosition() {
    }

    public void testGetPartitionId() {
    }
}