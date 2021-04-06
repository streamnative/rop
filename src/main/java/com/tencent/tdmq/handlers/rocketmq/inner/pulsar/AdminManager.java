package com.tencent.tdmq.handlers.rocketmq.inner.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;

@Slf4j
class AdminManager {

    private final PulsarAdmin admin;

    AdminManager(PulsarAdmin admin) {
        this.admin = admin;
    }

    /*CompletableFuture<Map<String, MQClientException>> createTopicsAsync(Map<String, TopicConfig> createInfo) {
        final Map<String, CompletableFuture<MQClientException>> futureMap = new ConcurrentHashMap<>();
        final AtomicInteger numTopics = new AtomicInteger(createInfo.size());
        final CompletableFuture<Map<String, MQClientException>> resultFuture = new CompletableFuture<>();

        Runnable complete = () -> {
            // prevent `futureMap` from being modified by createPartitionedTopicAsync()'s callback
            numTopics.set(0);

            futureMap.values().forEach(future -> {
                if (!future.isDone()) {
                    future.complete(new MQClientException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, null));
                }
            });
            resultFuture.complete(futureMap.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().getNow(ResponseCode.MESSAGE_ILLEGAL)
            )));
        };

        createInfo.forEach((topic, detail) -> {
            final CompletableFuture<MQClientException> errorFuture = new CompletableFuture<>();
            futureMap.put(topic, errorFuture);

            RocketMQTopic rocketMQTopic;
            try {
                rocketMQTopic = new RocketMQTopic(topic);
            } catch (RuntimeException e) {
                errorFuture.complete(new MQClientException());
                return;
            }
            admin.topics().createPartitionedTopicAsync(rocketMQTopic.getFullName(), detail.getWriteQueueNums())
                    .whenComplete((ignored, e) -> {
                        if (e == null) {
                            if (log.isDebugEnabled()) {
                                log.debug("Successfully create topic '{}'", topic);
                            }
                        } else {
                            log.error("Failed to create topic '{}': {}", topic, e);
                        }

                        int restNumTopics = numTopics.decrementAndGet();
                        if (restNumTopics < 0) {
                            return;
                        }
                        errorFuture.complete((e == null) ? ApiError.NONE : ApiError.fromThrowable(e));
                        if (restNumTopics == 0) {
                            complete.run();
                        }
                    });
        });

        complete.run();

        return resultFuture;
    }

    CompletableFuture<Map<ConfigResource, DescribeConfigsResponse.Config>> describeConfigsAsync(
            Map<ConfigResource, Optional<Set<String>>> resourceToConfigNames) {
        // Since Kafka's storage and policies are much different from Pulsar, here we just return a default config to
        // avoid some Kafka based systems need to send DescribeConfigs request, like confluent schema registry.
        final DescribeConfigsResponse.Config defaultTopicConfig = new DescribeConfigsResponse.Config(ApiError.NONE,
                KafkaLogConfig.getEntries().entrySet().stream().map(entry ->
                        new DescribeConfigsResponse.ConfigEntry(entry.getKey(), entry.getValue(),
                                DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG, false, false,
                                Collections.emptyList())
                ).collect(Collectors.toList()));

        Map<ConfigResource, CompletableFuture<DescribeConfigsResponse.Config>> futureMap =
                resourceToConfigNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    ConfigResource resource = entry.getKey();
                    try {
                        CompletableFuture<DescribeConfigsResponse.Config> future = new CompletableFuture<>();
                        switch (resource.type()) {
                            case TOPIC:
                                KopTopic kopTopic = new KopTopic(resource.name());
                                admin.topics().getPartitionedTopicMetadataAsync(kopTopic.getFullName())
                                        .whenComplete((metadata, e) -> {
                                            if (e != null) {
                                                future.complete(new DescribeConfigsResponse.Config(
                                                        ApiError.fromThrowable(e), Collections.emptyList()));
                                            } else if (metadata.partitions > 0) {
                                                future.complete(defaultTopicConfig);
                                            } else {
                                                final ApiError error = new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                                        "Topic " + kopTopic.getOriginalName() + " doesn't exist");
                                                future.complete(new DescribeConfigsResponse.Config(
                                                        error, Collections.emptyList()));
                                            }
                                        });
                                break;
                            case BROKER:
                                throw new RuntimeException("KoP doesn't support resource type: " + resource.type());
                            default:
                                throw new InvalidRequestException("Unsupported resource type: " + resource.type());
                        }
                        return future;
                    } catch (Exception e) {
                        return CompletableFuture.completedFuture(
                                new DescribeConfigsResponse.Config(ApiError.fromThrowable(e), Collections.emptyList()));
                    }
                }));
        CompletableFuture<Map<ConfigResource, DescribeConfigsResponse.Config>> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(futureMap.values().toArray(new CompletableFuture[0])).whenComplete((ignored, e) -> {
            resultFuture.complete(futureMap.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getNow(null))
            ));
        });
        return resultFuture;
    }*/
}
