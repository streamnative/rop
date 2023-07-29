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

import com.google.common.base.Splitter;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopRuntimeException;

/**
 * Rop Yammer Metrics.
 */
@Slf4j
public class RopYammerMetrics {

    public static final String ROP_RATE_IN = "rop_rate_in";
    public static final String ROP_RATE_OUT = "rop_rate_out";
    public static final String ROP_THROUGHPUT_IN = "rop_throughput_in";
    public static final String ROP_THROUGHPUT_OUT = "rop_throughput_out";
    public static final String ROP_PRODUCERS_COUNT = "rop_producers_count";
    public static final String ROP_CONSUMERS_COUNT = "rop_consumers_count";
    public static final String ROP_MSG_BACKLOG = "rop_msg_backlog";

    public static final String METRICS_CONFIG_PREFIX = "metrics.jmx.";
    public static final String EXCLUDE_CONFIG = METRICS_CONFIG_PREFIX + "exclude";
    public static final String INCLUDE_CONFIG = METRICS_CONFIG_PREFIX + "include";
    public static final String DEFAULT_INCLUDE = ".*";
    public static final String DEFAULT_EXCLUDE = "";

    public static final RopYammerMetrics INSTANCE = new RopYammerMetrics();

    public static MetricsRegistry defaultRegistry() {
        return INSTANCE.metricsRegistry;
    }

    private final MetricsRegistry metricsRegistry = new MetricsRegistry();
    private final FilteringJmxReporter jmxReporter = new FilteringJmxReporter(metricsRegistry,
            metricName -> true);

    private RopYammerMetrics() {
        jmxReporter.start();
        Runtime.getRuntime().addShutdownHook(new Thread(jmxReporter::shutdown));
    }

    public void configure(Map<String, ?> configs) {
        reconfigure(configs);
    }

    public void reconfigure(Map<String, ?> configs) {
        Predicate<String> mBeanPredicate = compilePredicate(configs);
        jmxReporter.updatePredicate(metricName -> mBeanPredicate.test(metricName.getMBeanName()));
    }

    public static Predicate<String> compilePredicate(Map<String, ?> configs) {
        String include = (String) configs.get(INCLUDE_CONFIG);
        String exclude = (String) configs.get(EXCLUDE_CONFIG);

        if (include == null) {
            include = DEFAULT_INCLUDE;
        }

        if (exclude == null) {
            exclude = DEFAULT_EXCLUDE;
        }

        try {
            Pattern includePattern = Pattern.compile(include);
            Pattern excludePattern = Pattern.compile(exclude);

            return s -> includePattern.matcher(s).matches()
                    && !excludePattern.matcher(s).matches();
        } catch (PatternSyntaxException e) {
            throw new RopRuntimeException("JMX filter for configuration" + METRICS_CONFIG_PREFIX
                    + ".(include/exclude) is not a valid regular expression");
        }
    }

    public static String getTopicFromScope(String scope) {
        try {
            for (String str : Splitter.on(",").omitEmptyStrings().trimResults().split(scope)) {
                if (str.startsWith("topic=")) {
                    return str.substring(7, str.length() - 1);
                }
            }
        } catch (Exception e) {
            log.warn("RoP get topic from scope [{}] failed.", scope, e);
        }
        return "";
    }
}
