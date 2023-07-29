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

package org.streamnative.pulsar.handlers.rocketmq.metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;

/**
 * Rop Metrics manager.
 */
@Slf4j
public class RopMetricsManager {

    private final RocketMQBrokerController brokerController;

    private final ScheduledExecutorService clearMetricsExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("RoP-clear-metrics");
        t.setDaemon(true);
        return t;
    });

    public RopMetricsManager(final RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;

        clearMetricsExecutor.scheduleAtFixedRate(() -> {
            try {
                clearMetrics();
            } catch (Throwable e) {
                log.error("RoP clear metrics error.", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    private void clearMetrics() {
        Map<MetricName, Metric> metricMap = RopYammerMetrics.defaultRegistry().allMetrics();
        for (Entry<MetricName, Metric> entry : metricMap.entrySet()) {
            String name = entry.getKey().getName();
            String scope = entry.getKey().getScope();
            if (StringUtils.isNotBlank(name) && name.startsWith("rop_")) {
                String pulsarTopic = RopYammerMetrics.getTopicFromScope(scope);
                if (!this.brokerController.getBrokerService().isTopicNsOwnedByBroker(TopicName.get(pulsarTopic))) {
                    RopYammerMetrics.defaultRegistry().removeMetric(entry.getKey());
                }
            }
        }
    }

    public void close() {
        clearMetricsExecutor.shutdownNow();
    }

}
