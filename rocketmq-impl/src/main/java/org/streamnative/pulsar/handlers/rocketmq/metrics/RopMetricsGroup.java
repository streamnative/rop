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

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider;
import org.streamnative.pulsar.handlers.rocketmq.utils.Sanitizer;

/**
 * Rop Metrics group class.
 */
public abstract class RopMetricsGroup implements PrometheusRawMetricsProvider {

    public MetricName metricName(String name, Map<String, String> tags) {
        Class<?> klass = this.getClass();
        String pkg = (klass.getPackage() == null) ? "" : klass.getPackage().getName();
        String simpleName = klass.getSimpleName().replaceAll("\\$$", "");
        return explicitMetricName(pkg, simpleName, name, tags);
    }

    protected MetricName explicitMetricName(String group, String typeName, String name, Map<String, String> tags) {
        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(ObjectName.quote(group));
        nameBuilder.append(":type=");
        nameBuilder.append(ObjectName.quote(typeName));

        if (Strings.isNotBlank(name)) {
            nameBuilder.append(",name=");
            nameBuilder.append(ObjectName.quote(name));
        }

        String scope = toScope(tags);
        if (Strings.isNotBlank(scope)) {
            nameBuilder.append(",scope=");
            nameBuilder.append(ObjectName.quote(scope));
        }

        return new MetricName(group, typeName, name, scope, nameBuilder.toString());
    }

    public <T> Gauge<T> newGauge(String name, Gauge<T> metric, Map<String, String> tags) {
        return RopYammerMetrics.defaultRegistry().newGauge(metricName(name, tags), metric);
    }

    public Counter newCounter(String name, Map<String, String> tags) {
        return RopYammerMetrics.defaultRegistry().newCounter(metricName(name, tags));
    }

    public Meter newMeter(String name, String eventType, TimeUnit timeUnit, Map<String, String> tags) {
        return RopYammerMetrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit);
    }

    public Histogram newHistogram(String name, Boolean biased, Map<String, String> tags) {
        return RopYammerMetrics.defaultRegistry().newHistogram(metricName(name, tags), biased);
    }

    public Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit, Map<String, String> tags) {
        return RopYammerMetrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit);
    }

    public void removeMetric(String name, Map<String, String> tags) {
        RopYammerMetrics.defaultRegistry().removeMetric(metricName(name, tags));
    }

    private String toMBeanName(Map<String, String> tags) {
        if (tags != null && !tags.isEmpty()) {
            List<Entry<String, String>> filteredTags = tags.entrySet().stream()
                    .filter(entry -> Strings.isNotBlank(entry.getValue()))
                    .collect(Collectors.toList());
            if (!filteredTags.isEmpty()) {
                String tagsString = filteredTags.stream()
                        .map(entry -> "%s=%s".format(entry.getKey(), Sanitizer.jmxSanitize(entry.getValue())))
                        .collect(Collectors.joining(","));
                return tagsString;
            }
        }
        return Strings.EMPTY;
    }

    private String toScope(Map<String, String> tags) {
        if (tags != null && !tags.isEmpty()) {
            List<Entry<String, String>> filteredTags = tags.entrySet().stream()
                    .filter(entry -> Strings.isNotBlank(entry.getValue()))
                    .sorted(Entry.comparingByKey())
                    .collect(Collectors.toList());

            if (!filteredTags.isEmpty()) {
                StringBuilder sbd = new StringBuilder();
                sbd.append("{");
                for (Entry<String, String> entry : tags.entrySet()) {
                    sbd.append(entry.getKey()).append("=\"").append(entry.getValue()).append("\"").append(",");
                }
                return sbd.substring(0, sbd.length() - 1) + "}";
            }
        }
        return Strings.EMPTY;
    }
}
