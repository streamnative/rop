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

package org.streamnative.pulsar.handlers.rocketmq.inner.trace;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.skywalking.apm.agent.core.context.ids.GlobalIdGenerator;
import sun.misc.BASE64Encoder;

public class TraceManager {

    static {
        instance = new TraceManager();
    }

    private static final TraceManager instance;

    private static final Logger LOGGER = LogManager.getLogger("tdmq.trace");
    private static final char SEPARATOR = '|';

    private final ThreadLocal<StringBuilder> BUILDER = ThreadLocal.withInitial(() -> new StringBuilder(518));

    public static TraceManager get() {
        return instance;
    }

    public static final int TYPE_PUT = 0;
    public static final int TYPE_PERSIST = 1;
    public static final int TYPE_GET = 2;
    public static final int TYPE_COMMIT = 3;

    public static final int PROTOCOL_VERSION = 1;

    private final TraceStatsReportService reporter;

    private final BASE64Encoder base64Encoder = new BASE64Encoder();

    private String buildMessage(Object... args) {
        StringBuilder builder = BUILDER.get();
        builder.append(PROTOCOL_VERSION);
        builder.append(SEPARATOR);
        for (Object arg : args) {
            builder.append(arg);
            builder.append(SEPARATOR);
        }

        String result = builder.toString();
        builder.setLength(0);
        return result;
    }

    private void logDiskWrite() {
        if (reporter != null) {
            reporter.logWriteToDisk(1);
        }
    }

    public TraceManager() {
        reporter = new TraceStatsReportService();
        try {
            reporter.boot();
        } catch (Throwable e) {
            // do nothing
        }
    }

    public void tracePut(TraceContext context) {
        // encode other properties with base64
        Map<String, Object> map = Maps.newHashMap();
        map.put("broker_tag", context.getBrokerTag());
        map.put("queue_id", context.getQueueId());
        map.put("offset_msg_id", context.getOffsetMsgId());
        map.put("msg_key", context.getMsgKey());
        map.put("instance_name", context.getInstanceName());
        map.put("tags", context.getTags());
        String encodeProperties = base64Encoder.encode(JSON.toJSONString(map).getBytes(StandardCharsets.UTF_8));
        String message = buildMessage(GlobalIdGenerator.generate(), 0, 0, context.getPutStartTime(),
                context.getEndTime(), TYPE_PUT, context.getTopic(), context.getPartitionId(), context.getOffset(),
                context.getMsgId(), context.getPulsarMsgId(), context.getCode(), context.getDuration(),
                encodeProperties);
        logToDisk(message);
    }

    public void tracePersist(TraceContext context) {
        String message = buildMessage(GlobalIdGenerator.generate(), 0, 0,
                context.getPersistStartTime(), context.getEndTime(), TYPE_PERSIST, context.getTopic(),
                context.getMsgId(),
                context.getCode(), context.getDuration());
        logToDisk(message);
    }

    public void traceGet(TraceContext context) {
        // encode group with base64
        String encodeGroup = base64Encoder.encode(context.getGroup().getBytes(StandardCharsets.UTF_8));
        String message = buildMessage(GlobalIdGenerator.generate(), 0, 0, System.currentTimeMillis(),
                context.getEndTime(), TYPE_GET, context.getTopic(), encodeGroup, context.getMsgId(),
                context.getInstanceName(), context.getMessageModel());
        logToDisk(message);
    }

    public void traceCommit(TraceContext context) {
        // encode group with base64
        String encodeGroup = base64Encoder.encode(context.getGroup().getBytes(StandardCharsets.UTF_8));
        String message = buildMessage(GlobalIdGenerator.generate(), 0, 0, System.currentTimeMillis(),
                context.getEndTime(), TYPE_COMMIT, context.getTopic(), encodeGroup, context.getPartitionId(),
                context.getOffset());
        logToDisk(message);
    }

    private void logToDisk(String log) {
        if (reporter == null || reporter.canWriteDisk()) {
            LOGGER.info(log);
            logDiskWrite();
        }
    }
}
