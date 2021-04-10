package com.tencent.tdmq.handlers.rocketmq.utils;

import com.google.common.base.Joiner;
import lombok.Getter;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

/**
 * RocketMQTopic maintains two topic name, one is the original topic name, the other is the full topic name used in
 * Pulsar.
 * We shouldn't use the original topic name directly in RoP source code. Instead, we should
 * 1. getOriginalName() when read a RocketMQ request from client or write a RocketMQ response to client.
 * 2. getFullName() when access Pulsar resources.
 */
public class RocketMQTopic {

    private static final String TENANT_NAMESPACE_SEP = "|";
    private static final TopicDomain domain = TopicDomain.persistent;
    private static volatile String tenant = "rocketmq";
    private static volatile String namespace = "public";
    @Getter
    private TopicName pulsarTopicName;

    //rocketmq topicname => namespace%originaltopic   namespace%DLQ%originaltopic  originaltopic %DLQ%originaltopic
    public RocketMQTopic(String tenant, String namespace, String rmqTopicName) {
        String prefix = NamespaceUtil.getNamespaceFromResource(rmqTopicName);
        String realNs = Strings.isNotBlank(prefix) ? prefix : namespace;
        String realTenant =
                realNs.indexOf(TENANT_NAMESPACE_SEP) > 0 ? prefix.substring(0, realNs.indexOf(TENANT_NAMESPACE_SEP))
                        : tenant;
        realNs = realNs.indexOf(TENANT_NAMESPACE_SEP) > 0 ? realNs.substring(realNs.indexOf(TENANT_NAMESPACE_SEP) + 1)
                : realNs;
        this.pulsarTopicName = TopicName
                .get(domain.name(), realTenant, realNs, NamespaceUtil.withoutNamespace(rmqTopicName));
    }

    public RocketMQTopic(String rmqTopicName) {
        this(tenant, namespace, rmqTopicName);
    }

    public final static void init(String tenant, String namespace) {
        RocketMQTopic.tenant = tenant;
        RocketMQTopic.namespace = namespace;
    }

    public String getNoDomainTopicName() {
        return Joiner.on('/').join(pulsarTopicName.getTenant(), pulsarTopicName.getNamespacePortion(),
                pulsarTopicName.getLocalName());
    }

    public String getPulsarFullName() {
        return this.pulsarTopicName.toString();
    }

    public String getPartitionName(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Invalid partition " + partition + ", it should be non-negative number");
        }
        return this.pulsarTopicName.getPartition(partition).toString();
    }

    public TopicName getPartitionTopicName(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Invalid partition " + partition + ", it should be non-negative number");
        }
        return this.pulsarTopicName.getPartition(partition);
    }

}

