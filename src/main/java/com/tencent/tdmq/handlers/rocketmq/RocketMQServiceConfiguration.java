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

package com.tencent.tdmq.handlers.rocketmq;

import lombok.Data;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;

/**
 * RocketMQ service configuration.
 */
@Data
public class RocketMQServiceConfiguration extends ServiceConfiguration {

    @Category
    private static final String CATEGORY_ROCKETMQ = "RocketMQ on Pulsar";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "Rocketmq on Pulsar Broker tenant"
    )
    private String rocketmqTenant = "rocketmq";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "The tenant used for storing Rocketmq metadata topics"
    )
    private String rocketmqMetadataTenant = "rocketmq";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "Rocketmq on Pulsar Broker namespace"
    )
    private String rocketmqNamespace = "default";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "The namespace used for storing rocket metadata topics"
    )
    private String rocketmqMetadataNamespace = "__rocketmq";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "Comma-separated list of URIs we will listen on and the listener names.\n"
                    + "e.g. rocketmq://localhost:9876.\n"
                    + "If hostname is not set, bind to the default interface."
    )
    private String rocketmqListeners = "rocketmq://127.0.0.1:9876";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Server worker threads number.\n"
    )
    private int serverWorkerThreads = 8;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of permits for one-way requests.\n"
    )
    private int permitsOneway = 256;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of permits for asynchronous requests.\n"
    )
    private int permitsAsync = 64;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests.\n"
    )
    private int callbackThreadPoolsNum = 4;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The capacity of send thread pool queue.\n"
    )
    private int sendThreadPoolQueueCapacity = 10000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The capacity of pull thread pool queue.\n"
    )
    private int pullThreadPoolQueueCapacity = 100000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The capacity of replay thread pool queue.\n"
    )
    private int replyThreadPoolQueueCapacity = 10000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The capacity query thread pool queue.\n"
    )
    private int queryThreadPoolQueueCapacity = 20000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The capacity of client manager thread pool queue.\n"
    )
    private int clientManagerThreadPoolQueueCapacity = 1000000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The capacity of consumer manager thread pool queue.\n"
    )
    private int consumerManagerThreadPoolQueueCapacity = 1000000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The capacity of heartbeat thread pool queue.\n"
    )
    private int heartbeatThreadPoolQueueCapacity = 50000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The capacity of end transaction pool queue.\n"
    )
    private int endTransactionPoolQueueCapacity = 100000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Error rate of bloom filter, 1~100.\n"
    )
    private int maxErrorRateOfBloomFilter = 20;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Expect num of consumers will use filter.\n"
    )
    private int expectConsumerNumUseFilter = 32;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "how long to clean filter data after dead.Default: 24h\n"
    )
    private int filterDataCleanTimeSpan = 24 * 3600 * 1000;


    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of send message thread pool.\n"
    )
    private int sendMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of pull message thread pool.\n"
    )
    private int pullMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of query message thread pool.\n"
    )
    private int queryMessageThreadPoolNums = 8 + Runtime.getRuntime().availableProcessors();

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of admin broker thread pool.\n"
    )
    private int adminBrokerThreadPoolNums = 16;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of client manager thread pool.\n"
    )
    private int clientManageThreadPoolNums = 32;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of consumer manager thread pool.\n"
    )
    private int consumerManageThreadPoolNums = 32;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of heartbeat thread pool.\n"
    )
    private int heartbeatThreadPoolNums = Math.min(32, Runtime.getRuntime().availableProcessors());
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of end transaction thread pool.\n"
    )
    private int endTransactionThreadPoolNums = 8 + Runtime.getRuntime().availableProcessors() * 2;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The interval time of flush consumer offset.\n"
    )
    private int flushConsumerOffsetInterval = 1000 * 5;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The interval time of flush consumer offset history.\n"
    )
    private int flushConsumerOffsetHistoryInterval = 1000 * 60;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The name of broker.\n"
    )
    private String brokerName = "rocketmq-broker";
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The interval time of transaction check.\n"
    )
    private int transactionCheckInterval = 60 * 1000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The timeout of transaction.\n"
    )
    private long transactionTimeOut = 6 * 1000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private int transactionCheckMax = 15;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Whether enable auto create topic, the default is true.\n"
    )
    private boolean autoCreateTopicEnable = true;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of default topics queue.\n"
    )
    private int defaultTopicQueueNums = 3;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Whether enable cluster topic function, the default is true.\n"
    )
    private boolean clusterTopicEnable = true;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Whether enable broker topic function, the default is true.\n"
    )
    private boolean brokerTopicEnable = true;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Whether enable trace topic function, the default is true.\n"
    )
    private boolean traceTopicEnable = false;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The name of message trace topic.\n"
    )
    private String msgTraceTopicName = "trace";
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Whether enable long polling function in consumer.\n"
    )
    private boolean longPollingEnable = true;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The time of short polling.\n"
    )
    private long shortPollingTimeMills = 1000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The permission of broker.\n"
    )
    private int brokerPermission = (PermName.PERM_READ | PermName.PERM_WRITE);
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The count of commercial base.\n"
    )
    private int commercialBaseCount = 1;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Whether enable notify consumer IDs change function.\n"
    )
    private boolean notifyConsumerIdsChangedEnable = true;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Whether enable auto create subscription group function.\n"
    )
    private boolean autoCreateSubscriptionGroup = true;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Whether enable transfer message by heap.\n"
    )
    private boolean transferMsgByHeap = true;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Max number of default query.\n"
    )
    private int defaultQueryMaxNum = 10000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The time of server channel max idle time.\n"
    )
    private int serverChannelMaxIdleTimeSeconds = 120;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The name of consumer offset.\n"
    )
    private String consumerOffsetsTopicName = "__consumer_offsets";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The topic of RocketMQ system transfer half.\n"
    )
    private String rmqSysTransHalfTopic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The topic of RocketMQ system transfer OP half.\n"
    )
    private String rmqSysTransOpHalfTopic = MixAll.RMQ_SYS_TRANS_OP_HALF_TOPIC;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The topic of RocketMQ transfer check max time.\n"
    )
    private String rmqTransCheckMaxTimeTopic = MixAll.TRANS_CHECK_MAX_TIME_TOPIC;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The name of RocketMQ schedule topic.\n"
    )
    private String rmqScheduleTopic = "SCHEDULE_TOPIC_XXXX";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Number of RocketMQ schedule topic partition.\n"
    )
    private int rmqScheduleTopicPartitionNum = 5;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "rocketmq delayed message level.\n"
    )
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "rocketmq max number of delayed level.\n"
    )
    private int maxDelayLevelNum = 16;
}
