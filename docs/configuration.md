# Configuration

The following table lists all RoP configurations.

|Name|Description|Default|
|---|---|---|
|rocketmqTenant|Rocketmq on Pulsar Broker tenant|rocketmq
|rocketmqMetadataTenant|The tenant used for storing Rocketmq metadata topics|rocketmq
|rocketmqNamespace|Rocketmq on Pulsar Broker namespace|default
|rocketmqMetadataNamespace|The namespace used for storing rocket metadata topics|__rocketmq
|rocketmqListeners|Comma-separated list of URIs we will listen on and the listener names.|rocketmq://127.0.0.1:9876
|serverWorkerThreads|Server worker threads number|8
|permitsOneway|nets permits for oneway send|256
|permitsAsync|nets permits for async send|64
|callbackThreadPoolsNum|pool size for processing callback requests|4
|sendThreadPoolQueueCapacity|pool size for processing callback requests|10000
|pullThreadPoolQueueCapacity|pool size for processing callback requests|100000
|replyThreadPoolQueueCapacity|pool size for processing callback requests|10000
|queryThreadPoolQueueCapacity|pool size for processing callback requests|20000
|clientManagerThreadPoolQueueCapacity|pool size for processing callback requests|1000000
|consumerManagerThreadPoolQueueCapacity|pool size for processing callback requests|1000000
|heartbeatThreadPoolQueueCapacity|pool size for processing callback requests|50000
|endTransactionPoolQueueCapacity|pool size for processing callback requests|100000
|maxErrorRateOfBloomFilter|Error rate of bloom filter, 1~100|20
|expectConsumerNumUseFilter|Expect num of consumers will use filter|32
|filterDataCleanTimeSpan|how long to clean filter data after dead.Default: 24h|24 * 3600 * 1000
|sendMessageThreadPoolNums|pool size for processing callback requests|16 + Runtime.getRuntime().availableProcessors() * 2
|pullMessageThreadPoolNums|pool size for processing callback requests|16 + Runtime.getRuntime().availableProcessors() * 2
|adminBrokerThreadPoolNums|pool size for processing callback requests|16
|clientManageThreadPoolNums|pool size for processing callback requests|32
|consumerManageThreadPoolNums|pool size for processing callback requests|32
|heartbeatThreadPoolNums|pool size for processing callback requests|Math.min(32, Runtime.getRuntime().availableProcessors())
|endTransactionThreadPoolNums|pool size for processing callback requests|8 + Runtime.getRuntime().availableProcessors() * 2
|flushConsumerOffsetInterval|pool size for processing callback requests|1000 * 5
|flushConsumerOffsetHistoryInterval|pool size for processing callback requests|1000 * 60
|brokerName|pool size for processing callback requests|rocketmq-broker
|transactionCheckInterval|pool size for processing callback requests|60 * 1000
|transactionTimeOut|pool size for processing callback requests|6 * 1000
|transactionCheckMax|The maximum number of times the message was checked, if exceed this value, this message will be discarded|15
|autoCreateTopicEnable|The maximum number of times the message was checked, if exceed this value, this message will be discarded|true
|defaultTopicQueueNums|The maximum number of times the message was checked, if exceed this value, this message will be discarded|3
|clusterTopicEnable|The maximum number of times the message was checked, if exceed this value, this message will be discarded|true
|brokerTopicEnable|The maximum number of times the message was checked, if exceed this value, this message will be discarded|true
|traceTopicEnable|The maximum number of times the message was checked, if exceed this value, this message will be discarded|false
|msgTraceTopicName|The maximum number of times the message was checked, if exceed this value, this message will be discarded|trace
|longPollingEnable|The maximum number of times the message was checked, if exceed this value, this message will be discarded|false
|shortPollingTimeMills|The maximum number of times the message was checked, if exceed this value, this message will be discarded|1000
|brokerPermission|The maximum number of times the message was checked, if exceed this value, this message will be discarded|(PermName.PERM_READ/PermName.PERM_WRITE)
|commercialBaseCount|The maximum number of times the message was checked, if exceed this value, this message will be discarded|1
|notifyConsumerIdsChangedEnable|The maximum number of times the message was checked, if exceed this value, this message will be discarded|true
|autoCreateSubscriptionGroup|The maximum number of times the message was checked, if exceed this value, this message will be discarded|true
|transferMsgByHeap|The maximum number of times the message was checked, if exceed this value, this message will be discarded|true
|defaultQueryMaxNum|The maximum number of times the message was checked, if exceed this value, this message will be discarded|10000
|serverChannelMaxIdleTimeSeconds|The maximum number of times the message was checked, if exceed this value, this message will be discarded|120
|consumerOffsetsTopicName|The maximum number of times the message was checked, if exceed this value, this message will be discarded|__consumer_offsets
|rmqSysTransHalfTopic|The maximum number of times the message was checked, if exceed this value, this message will be discarded|MixAll.RMQ_SYS_TRANS_HALF_TOPIC
|rmqSysTransOpHalfTopic|The maximum number of times the message was checked, if exceed this value, this message will be discarded|MixAll.RMQ_SYS_TRANS_OP_HALF_TOPIC
|rmqTransCheckMaxTimeTopic|The maximum number of times the message was checked, if exceed this value, this message will be discarded|MixAll.TRANS_CHECK_MAX_TIME_TOPIC
|rmqScheduleTopic|The maximum number of times the message was checked, if exceed this value, this message will be discarded|SCHEDULE_TOPIC_XXXX
|messageDelayLevel|rocketmq delayed message level|1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
|maxDelayLevelNum|rocketmq max number of delayed level|16
