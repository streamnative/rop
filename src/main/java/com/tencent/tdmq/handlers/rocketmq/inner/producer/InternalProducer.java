package com.tencent.tdmq.handlers.rocketmq.inner.producer;

import io.netty.channel.ChannelHandlerContext;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

@Slf4j
public class InternalProducer extends Producer {

    public InternalProducer(Topic topic, TransportCnx cnx, long producerId,
            String producerName, String appId, boolean isEncrypted,
            Map<String, String> metadata, SchemaVersion schemaVersion,
            long epoch, boolean userProvidedProducerName) {
        super(topic, cnx, producerId, producerName, appId, isEncrypted, metadata, schemaVersion, epoch,
                userProvidedProducerName);
    }

    /* private ServerCnx serverCnx;
        public InternalProducer(Topic topic, ChannelHandlerContext ctx,
                long producerId, String producerName) {
            super(topic, ctx, producerId, producerName, null,
                    false, null, null, 0, false);
            this.serverCnx = cnx;
        }

        // this will call back by bundle unload
        @Override
        public CompletableFuture<Void> disconnect() {
            InternalServerCnx cnx = (InternalServerCnx) getCnx();
            CompletableFuture<Void> future = new CompletableFuture<>();

            cnx.getBrokerService().executor().execute(() -> {
                log.info("Disconnecting producer: {}", this);
                getTopic().removeProducer(this);
                cnx.closeProducer(this);
                future.complete(null);
            });

            return future;
        }
    */
    @Override
    public ServerCnx getCnx() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
