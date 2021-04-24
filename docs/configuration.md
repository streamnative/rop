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
|permitsOneway|Number of permits for one-way requests|256
|permitsAsync|Number of permits for asynchronous requests|64
|callbackThreadPoolsNum|pool size for processing callback requests|4
|sendThreadPoolQueueCapacity|The capacity of send thread pool queue|10000
|pullThreadPoolQueueCapacity|The capacity of pull thread pool queue|100000
|replyThreadPoolQueueCapacity|The capacity of replay thread pool queue|10000
|queryThreadPoolQueueCapacity|The capacity query thread pool queue|20000
|clientManagerThreadPoolQueueCapacity|The capacity of client manager thread pool queue|1000000
|consumerManagerThreadPoolQueueCapacity|The capacity of consumer manager thread pool queue|1000000
|heartbeatThreadPoolQueueCapacity|The capacity of heartbeat thread pool queue|50000
|endTransactionPoolQueueCapacity|The capacity of end transaction pool queue|100000
|maxErrorRateOfBloomFilter|Error rate of bloom filter, 1~100|20
|expectConsumerNumUseFilter|Expect num of consumers will use filter|32
|filterDataCleanTimeSpan|how long to clean filter data after dead.Default: 24h|24 * 3600 * 1000
|sendMessageThreadPoolNums|Number of send message thread pool|16 + Runtime.getRuntime().availableProcessors() * 2
|pullMessageThreadPoolNums|Number of pull message thread pool|16 + Runtime.getRuntime().availableProcessors() * 2
|queryMessageThreadPoolNums|Number of query message thread pool|8 + Runtime.getRuntime().availableProcessors()
|adminBrokerThreadPoolNums|Number of admin broker thread pool|16
|clientManageThreadPoolNums|Number of client manager thread pool|32
|consumerManageThreadPoolNums|Number of consumer manager thread pool|32
|heartbeatThreadPoolNums|Number of heartbeat thread pool|Math.min(32, Runtime.getRuntime().availableProcessors())
|endTransactionThreadPoolNums|Number of end transaction thread pool|8 + Runtime.getRuntime().availableProcessors() * 2
|flushConsumerOffsetInterval|The interval time of flush consumer offset|1000 * 5
|flushConsumerOffsetHistoryInterval|The interval time of flush consumer offset history|1000 * 60
|brokerName|The name of broker|rocketmq-broker
|transactionCheckInterval|The interval time of transaction check|60 * 1000
|transactionTimeOut|The timeout of transaction|6 * 1000
|transactionCheckMax|The maximum number of times the message was checked, if exceed this value, this message will be discarded|15
|autoCreateTopicEnable|Whether enable auto create topic, the default is true|true
|defaultTopicQueueNums|Number of default topics queue|3
|clusterTopicEnable|Whether enable cluster topic function, the default is true|true
|brokerTopicEnable|Whether enable broker topic function, the default is true|true
|traceTopicEnable|Whether enable trace topic function, the default is true|false
|msgTraceTopicName|The name of message trace topic|trace
|longPollingEnable|Whether enable long polling function in consumer|false
|shortPollingTimeMills|The time of short polling|1000
|brokerPermission|The permission of broker|(PermName.PERM_READ/PermName.PERM_WRITE)
|commercialBaseCount|The count of commercial base|1
|notifyConsumerIdsChangedEnable|Whether enable notify consumer IDs change function|true
|autoCreateSubscriptionGroup|Whether enable auto create subscription group function|true
|transferMsgByHeap|Whether enable transfer message by heap|true
|defaultQueryMaxNum|Max number of default query|10000
|serverChannelMaxIdleTimeSeconds|The time of server channel max idle time|120
|consumerOffsetsTopicName|The name of consumer offset|__consumer_offsets
|rmqSysTransHalfTopic|The topic of RocketMQ system transfer half|MixAll.RMQ_SYS_TRANS_HALF_TOPIC
|rmqSysTransOpHalfTopic|The topic of RocketMQ system transfer OP half|MixAll.RMQ_SYS_TRANS_OP_HALF_TOPIC
|rmqTransCheckMaxTimeTopic|The topic of RocketMQ transfer check max time|MixAll.TRANS_CHECK_MAX_TIME_TOPIC
|rmqScheduleTopic|The name of RocketMQ schedule topic|SCHEDULE_TOPIC_XXXX
|rmqScheduleTopicPartitionNum|Number of RocketMQ schedule topic partition|5
|messageDelayLevel|rocketmq delayed message level|1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
|maxDelayLevelNum|rocketmq max number of delayed level|16
