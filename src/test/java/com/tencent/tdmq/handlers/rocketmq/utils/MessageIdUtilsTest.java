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

import static com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils.MAX_ROP_OFFSET;
import static com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils.MIN_ROP_OFFSET;
import static com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils.getMessageId;
import static com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils.getOffset;
import static org.junit.Assert.assertEquals;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Test;

/**
 * Test messageID utils.
 */
public class MessageIdUtilsTest {

    @Test
    public void testGetOffset() {
        long offset = getOffset(-1, -1);
        assertEquals(-1, offset);
        MessageIdImpl messageId = getMessageId(offset);
        assertEquals(-1, messageId.getLedgerId());
        assertEquals(-1, messageId.getEntryId());

        offset = getOffset(0x7FFFFFFFFFFFL, -1);
        messageId = getMessageId(offset);
        assertEquals(0x7FFFFFFFFFFFL, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());

        offset = getOffset(0x7FFFFFFFFFFFL, -1);
        messageId = getMessageId(offset);
        assertEquals(0x7FFFFFFFFFFFL, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());

        offset = getOffset(1872L, -1);
        messageId = getMessageId(offset);
        assertEquals(1872L, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());

        offset = getOffset(1872L, 65534L);
        messageId = getMessageId(offset);
        assertEquals(1872L, messageId.getLedgerId());
        assertEquals(65534L, messageId.getEntryId());

        offset = getOffset(1872L, 0L);
        messageId = getMessageId(offset);
        assertEquals(1872L, messageId.getLedgerId());
        assertEquals(0L, messageId.getEntryId());

        offset = getOffset(Long.MAX_VALUE, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, offset);
        messageId = getMessageId(offset);
        assertEquals(Long.MAX_VALUE, messageId.getLedgerId());
        assertEquals(Long.MAX_VALUE, messageId.getEntryId());
    }

    @Test
    public void testTestGetOffset() {
        MessageIdImpl messageId = (MessageIdImpl) MessageId.latest;
        long offset = getOffset(messageId);
        assertEquals(offset, MAX_ROP_OFFSET);
        MessageIdImpl messageId1 = getMessageId(offset);
        assertEquals(messageId1, MessageId.latest);

        messageId = (MessageIdImpl) MessageId.earliest;
        offset = getOffset(messageId);
        assertEquals(offset, MIN_ROP_OFFSET);
        MessageIdImpl minMsgId = getMessageId(offset);
        assertEquals(minMsgId, MessageId.earliest);
    }

    @Test
    public void testGetMessageId() {

        MessageIdImpl messageId = new MessageIdImpl(1234L, -1L, -1);
        long offset = getOffset(messageId);
        MessageIdImpl messageId1 = getMessageId(offset);
        assertEquals(1234L, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());
    }

    public void testGetPosition() {
    }

    public void testGetPartitionId() {
    }
}