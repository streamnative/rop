package org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RopGroupContent {

    private SubscriptionGroupConfig config;

}
