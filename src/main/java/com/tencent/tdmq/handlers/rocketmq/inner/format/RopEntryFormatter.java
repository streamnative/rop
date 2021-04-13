package com.tencent.tdmq.handlers.rocketmq.inner.format;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.rocketmq.store.MessageExtBrokerInner;

public class RopEntryFormatter implements EntryFormatter<MessageExtBrokerInner> {

    @Override
    public MessageExtBrokerInner decode(Entry list, byte magic) {

        return null;
    }

    @Override
    public ByteBuf encode(MessageExtBrokerInner record, int numMessages) {
        final ByteBuf recordsWrapper = Unpooled.wrappedBuffer(record.getBody());
        final ByteBuf buf = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.None,
                getMessageMetadataWithNumberMessages(numMessages),
                recordsWrapper);
        recordsWrapper.release();
        return buf;
    }

    private static PulsarApi.MessageMetadata getMessageMetadataWithNumberMessages(int numMessages) {
        final PulsarApi.MessageMetadata.Builder builder = PulsarApi.MessageMetadata.newBuilder();
        builder.addProperties(PulsarApi.KeyValue.newBuilder()
                .setKey("entry.format")
                .setValue("rocketmq")
                .build());
        builder.setProducerName("");
        builder.setSequenceId(0L);
        builder.setPublishTime(0L);
        builder.setNumMessagesInBatch(numMessages);
        return builder.build();
    }

//    @Override
//    public ByteBuf encode(MessageExt records, int numMessages) {
//        final ByteBuf recordsWrapper = Unpooled.wrappedBuffer(records.);
//        final ByteBuf buf = Commands.serializeMetadataAndPayload(
//                Commands.ChecksumType.None,
//                getMessageMetadataWithNumberMessages(numMessages),
//                recordsWrapper);
//        recordsWrapper.release();
//        return buf;
//    }
//
//    @Override
//    public MemoryRecords decode(List<Entry> entries, byte magic) {
//        int size = 0;
//        for (Entry entry : entries) {
//            size += entry.getLength();
//        }
//        final MemoryRecordsBuilder builder = MemoryRecords.builder(
//                ByteBuffer.allocate(size),
//                magic,
//                CompressionType.NONE,
//                TimestampType.CREATE_TIME,
//                MessageIdUtils.getOffset(entries.get(0).getLedgerId(), entries.get(0).getEntryId()));
//        entries.forEach(entry -> {
//            final ByteBuf byteBuf = entry.getDataBuffer();
//            Commands.skipMessageMetadata(byteBuf);
//            final MemoryRecords records = MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(byteBuf));
//            long offset = MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId());
//            for (Record record : records.records()) {
//                builder.appendWithOffset(offset, record);
//                offset++;
//            }
//            entry.release();
//        });
//        return builder.build();
//    }
//
//    private static PulsarApi.MessageMetadata getMessageMetadataWithNumberMessages(int numMessages) {
//        final PulsarApi.MessageMetadata.Builder builder = PulsarApi.MessageMetadata.newBuilder();
//        builder.addProperties(PulsarApi.KeyValue.newBuilder()
//                .setKey("entry.format")
//                .setValue(EntryFormatterFactory.EntryFormat.KAFKA.name().toLowerCase())
//                .build());
//        builder.setProducerName("");
//        builder.setSequenceId(0L);
//        builder.setPublishTime(0L);
//        builder.setNumMessagesInBatch(numMessages);
//        return builder.build();
//    }
}
