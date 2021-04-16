package com.tencent.tdmq.handlers.rocketmq;

import lombok.Data;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 1:20 下午
 */

/**
 * 这个类主要用来继承 pulsar broker 的 ServiceConfiguration 类，实现配置的注入
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
    private String rocketmqTenant = "public";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "The tenant used for storing Rocketmq metadata topics"
    )
    private String rocketmqMetadataTenant = "public";

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
            doc = "Comma-separated list of URIs we will listen on and the listener names.\n"
                    + "e.g. PLAINTEXT://localhost:9096.\n"
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
            doc = "nets permits for oneway send.\n"
    )
    private int permitsOneway = 256;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "nets permits for async send.\n"
    )
    private int permitsAsync = 64;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int callbackThreadPoolsNum = 4;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int sendThreadPoolQueueCapacity = 10000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int pullThreadPoolQueueCapacity = 100000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int replyThreadPoolQueueCapacity = 10000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int queryThreadPoolQueueCapacity = 20000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int clientManagerThreadPoolQueueCapacity = 1000000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int consumerManagerThreadPoolQueueCapacity = 1000000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int heartbeatThreadPoolQueueCapacity = 50000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
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
            doc = "pool size for processing callback requests..\n"
    )
    private int sendMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int pullMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int queryMessageThreadPoolNums = 8 + Runtime.getRuntime().availableProcessors();

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int adminBrokerThreadPoolNums = 16;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int clientManageThreadPoolNums = 32;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int consumerManageThreadPoolNums = 32;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int heartbeatThreadPoolNums = Math.min(32, Runtime.getRuntime().availableProcessors());
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int endTransactionThreadPoolNums = 8 + Runtime.getRuntime().availableProcessors() * 2;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int flushConsumerOffsetInterval = 1000 * 5;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int flushConsumerOffsetHistoryInterval = 1000 * 60;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private String brokerName = "rocketmq-broker";
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
    )
    private int transactionCheckInterval = 60 * 1000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "pool size for processing callback requests..\n"
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
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private boolean autoCreateTopicEnable = true;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private int defaultTopicQueueNums = 3;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private boolean clusterTopicEnable = true;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private boolean brokerTopicEnable = true;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private boolean traceTopicEnable = false;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private String msgTraceTopicName = "trace";
    private boolean longPollingEnable = true;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private long shortPollingTimeMills = 1000;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private int brokerPermission = (PermName.PERM_READ | PermName.PERM_WRITE);
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private int commercialBaseCount = 1;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private boolean notifyConsumerIdsChangedEnable = true;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private boolean autoCreateSubscriptionGroup = true;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private boolean transferMsgByHeap = true;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private int defaultQueryMaxNum = 10000;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private int serverChannelMaxIdleTimeSeconds = 120;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private String consumerOffsetsTopicName = "__consumer_offsets";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private String rmqSysTransHalfTopic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private String rmqSysTransOpHalfTopic = MixAll.RMQ_SYS_TRANS_OP_HALF_TOPIC;
    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private String rmqTransCheckMaxTimeTopic = MixAll.TRANS_CHECK_MAX_TIME_TOPIC;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private String rmqScheduleTopic = "SCHEDULE_TOPIC_XXXX";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "The maximum number of times the message was checked, "
                    + "if exceed this value, this message will be discarded.\n"
    )
    private int rmqScheduleTopicPartitionNum = 5;

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "rocketmq delayed message level.\n"
    )
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

}
