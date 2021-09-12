package org.streamnative.pulsar.handlers.rocketmq.inner.coordinator;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@EqualsAndHashCode
public class RopCoordinatorBroker {

    public final String serviceUrl;

}
