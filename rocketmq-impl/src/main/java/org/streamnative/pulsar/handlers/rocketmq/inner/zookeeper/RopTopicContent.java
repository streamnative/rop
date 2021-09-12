package org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.common.TopicConfig;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RopTopicContent {

    private TopicConfig config;
    private Map<String, List<Integer>> routeMap;

}
