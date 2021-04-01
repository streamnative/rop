package com.tencent.tdmq.handlers.rocketmq.utils;

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import lombok.Getter;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * RocketMQTopic maintains two topic name, one is the original topic name, the other is the full topic name used in
 * Pulsar.
 * We shouldn't use the original topic name directly in RoP source code. Instead, we should
 * 1. getOriginalName() when read a RocketMQ request from client or write a RocketMQ response to client.
 * 2. getFullName() when access Pulsar resources.
 */
public class RocketMQTopic {

    private static final String persistentDomain = "persistent://";
    private static volatile String namespacePrefix;  // the full namespace prefix, e.g. "public/default"

    public static String removeDefaultNamespacePrefix(String fullTopicName) {
        final String topicPrefix = persistentDomain + namespacePrefix + "/";
        if (fullTopicName.startsWith(topicPrefix)) {
            return fullTopicName.substring(topicPrefix.length());
        } else {
            return fullTopicName;
        }
    }

    public static void initialize(String namespace) {
        if (namespace.split("/").length != 2) {
            throw new IllegalArgumentException("Invalid namespace: " + namespace);
        }
        RocketMQTopic.namespacePrefix = namespace;
    }

    @Getter
    private final String originalName;
    @Getter
    private final String fullName;

    public RocketMQTopic(String topic) {
        if (namespacePrefix == null) {
            throw new RuntimeException("RocketMQTopic is not initialized");
        }
        originalName = topic;
        fullName = expandToFullName(topic);
    }

    private String expandToFullName(String topic) {
        if (topic.startsWith(persistentDomain)) {
            if (topic.substring(persistentDomain.length()).split("/").length != 3) {
                throw new IllegalArgumentException("Invalid topic name '" + topic + "', it should be "
                        + " persistent://<tenant>/<namespace>/<topic>");
            }
            return topic;
        }

        String[] parts = topic.split("/");
        if (parts.length == 3) {
            return persistentDomain + topic;
        } else if (parts.length == 1) {
            return persistentDomain + namespacePrefix + "/" + topic;
        } else {
            throw new IllegalArgumentException("Invalid short topic name '" + topic + "', it should be in the format"
                    + " of <tenant>/<namespace>/<topic> or <topic>");
        }
    }

    public String getPartitionName(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Invalid partition " + partition + ", it should be non-negative number");
        }
        return fullName + PARTITIONED_TOPIC_SUFFIX + partition;
    }

    public static String toString(MessageQueue messageQueue) {
        return (new RocketMQTopic(messageQueue.getTopic())).getPartitionName(messageQueue.getQueueId());
    }
}

