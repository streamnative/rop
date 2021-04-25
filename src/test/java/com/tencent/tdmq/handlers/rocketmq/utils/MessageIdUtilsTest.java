package com.tencent.tdmq.handlers.rocketmq.utils;

import static com.tencent.tdmq.handlers.rocketmq.utils.MessageIdUtils.*;
import static org.junit.Assert.assertEquals;

import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Test;

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