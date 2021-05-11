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

import static org.junit.Assert.assertEquals;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test messageID utils.
 */
public class MessageIdUtilsTest {

    @Test
    public void testGetOffset() {
        long offset = MessageIdUtils.getOffset(-1, -1, -1);
        Assert.assertEquals(MessageIdUtils.MIN_ROP_OFFSET, offset);
        MessageIdImpl messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(-1, messageId.getLedgerId());
        assertEquals(-1, messageId.getEntryId());
        assertEquals(-1, messageId.getPartitionIndex());

        offset = MessageIdUtils.getOffset(MessageIdUtils.MAX_LEDGER_ID, -1, -1);
        messageId = MessageIdUtils.getMessageId(offset);
        Assert.assertEquals(MessageIdUtils.MAX_LEDGER_ID, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());
        assertEquals(-1L, messageId.getPartitionIndex());

        offset = MessageIdUtils.getOffset(-1, MessageIdUtils.MAX_ENTRY_ID, -1);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(-1L, messageId.getLedgerId());
        Assert.assertEquals(MessageIdUtils.MAX_ENTRY_ID, messageId.getEntryId());
        assertEquals(-1, messageId.getPartitionIndex());

        offset = MessageIdUtils.getOffset(-1, -1, MessageIdUtils.MAX_PARTITION_ID);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(-1L, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());
        Assert.assertEquals(MessageIdUtils.MAX_PARTITION_ID, messageId.getPartitionIndex());

        offset = MessageIdUtils.getOffset(1872L, -1, -1);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(1872L, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());

        offset = MessageIdUtils.getOffset(1872L, 65534L, -1);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(1872L, messageId.getLedgerId());
        assertEquals(65534L, messageId.getEntryId());

        offset = MessageIdUtils.getOffset(1872L, 0L, -1);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(1872L, messageId.getLedgerId());
        assertEquals(0L, messageId.getEntryId());

        offset = MessageIdUtils.getOffset(Long.MAX_VALUE, Long.MAX_VALUE, 12);
        assertEquals(Long.MAX_VALUE, offset);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(Long.MAX_VALUE, messageId.getLedgerId());
        assertEquals(Long.MAX_VALUE, messageId.getEntryId());
    }

    @Test
    public void testTestGetOffset() {
        MessageIdImpl messageId = (MessageIdImpl) MessageId.latest;
        long offset = MessageIdUtils.getOffset(messageId);
        Assert.assertEquals(offset, MessageIdUtils.MAX_ROP_OFFSET);
        MessageIdImpl messageId1 = MessageIdUtils.getMessageId(offset);
        assertEquals(messageId1, MessageId.latest);

        messageId = (MessageIdImpl) MessageId.earliest;
        offset = MessageIdUtils.getOffset(messageId);
        Assert.assertEquals(offset, MessageIdUtils.MIN_ROP_OFFSET);
        MessageIdImpl minMsgId = MessageIdUtils.getMessageId(offset);
        assertEquals(minMsgId, MessageId.earliest);
    }

    @Test
    public void testGetMessageId() {

        MessageIdImpl messageId = new MessageIdImpl(1234L, -1L, -1);
        long offset = MessageIdUtils.getOffset(messageId);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(1234L, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());

        messageId = new MessageIdImpl(1234L, 0L, -1);
        offset = MessageIdUtils.getOffset(messageId);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(1234L, messageId.getLedgerId());
        assertEquals(0L, messageId.getEntryId());

        messageId = new MessageIdImpl(1234L, 123L, 126);
        offset = MessageIdUtils.getOffset(messageId);
        messageId = MessageIdUtils.getMessageId(offset);
        assertEquals(1234L, messageId.getLedgerId());
        assertEquals(123L, messageId.getEntryId());
        assertEquals(126, messageId.getPartitionIndex());

        messageId = MessageIdUtils.getMessageId(0L);
        assertEquals(-1L, messageId.getLedgerId());
        assertEquals(-1L, messageId.getEntryId());
        assertEquals(-1L, messageId.getPartitionIndex());

    }

    public void testGetPosition() {
    }

    public void testGetPartitionId() {
    }
}