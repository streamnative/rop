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

package com.tencent.tdmq.handlers.rocketmq.utils;

import com.google.common.base.Joiner;
import lombok.Getter;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

/**
 * RocketMQTopic maintains two topic name, one is the original topic name, the other is the full topic name used in
 * Pulsar.
 * We shouldn't use the original topic name directly in RoP source code. Instead, we should
 * 1. getOriginalName() when read a RocketMQ request from client or write a RocketMQ response to client.
 * 2. getFullName() when access Pulsar resources.
 */
public class RocketMQTopic {

    private static final char TENANT_NAMESPACE_SEP = '|';
    private static final char ROCKETMQ_NAMESPACE_TOPIC_SEP = NamespaceUtil.NAMESPACE_SEPARATOR;
    private static final TopicDomain domain = TopicDomain.persistent;
    public static String defaultTenant = "rocketmq";
    public static String defaultNamespace = "public";
    public static String metaTenant = "rocketmq";
    public static String metaNamespace = "__rocketmq";
    @Getter
    private TopicName pulsarTopicName;
    private String rocketmqTenant = Strings.EMPTY;
    private String rocketmqNs = Strings.EMPTY;

    //rocketmq topicname => namespace%originaltopic   namespace%DLQ%originaltopic  originaltopic %DLQ%originaltopic
    public RocketMQTopic(String defaultTenant, String defaultNamespace, String rmqTopicName) {
        String prefix = NamespaceUtil.getNamespaceFromResource(rmqTopicName);
        if (Strings.isNotBlank(prefix)) {
            if (prefix.indexOf(TENANT_NAMESPACE_SEP) > 0) {
                this.rocketmqTenant = prefix.substring(0, prefix.indexOf(TENANT_NAMESPACE_SEP));
                this.rocketmqNs = prefix.substring(prefix.indexOf(TENANT_NAMESPACE_SEP) + 1);
            } else {
                this.rocketmqNs = prefix;
            }
        }
        String realTenant = Strings.isNotBlank(this.rocketmqTenant) ? this.rocketmqTenant : defaultTenant;
        String realNs = Strings.isNotBlank(this.rocketmqNs) ? this.rocketmqNs : defaultNamespace;
        this.pulsarTopicName = TopicName
                .get(domain.name(), realTenant, realNs, NamespaceUtil.withoutNamespace(rmqTopicName));
    }

    public RocketMQTopic(String rmqTopicName) {
        this(defaultTenant, defaultNamespace, rmqTopicName);
    }

    public static final void init(String metaTenant, String metaNamespace, String defaultTenant,
            String defaultNamespace) {
        RocketMQTopic.defaultTenant = defaultTenant;
        RocketMQTopic.defaultNamespace = defaultNamespace;
        RocketMQTopic.metaTenant = metaTenant;
        RocketMQTopic.metaNamespace = metaNamespace;
    }

    public static final String getPulsarOrigNoDomainTopic(String rmqTopic) {
        return new RocketMQTopic(rmqTopic).getOrigNoDomainTopicName();
    }

    public static final String getPulsarMetaNoDomainTopic(String rmqTopic) {
        return new RocketMQTopic(rmqTopic).getMetaNoDomainTopic();
    }

    public static final String getPulsarDefaultNoDomainTopic(String rmqTopic) {
        return new RocketMQTopic(rmqTopic).getDefaultNoDomainTopic();
    }

    public static final RocketMQTopic getRocketMQMetaTopic(String rmqTopic) {
        return new RocketMQTopic(RocketMQTopic.metaTenant, RocketMQTopic.metaNamespace, rmqTopic);
    }

    public String getRocketDLQTopic() {
        if (Strings.isBlank(rocketmqTenant) && Strings.isBlank(rocketmqNs)) {
            return MixAll.DLQ_GROUP_TOPIC_PREFIX + pulsarTopicName.getLocalName();
        } else if (Strings.isBlank(rocketmqTenant) && Strings.isNotBlank(rocketmqNs)) {
            return MixAll.DLQ_GROUP_TOPIC_PREFIX + rocketmqNs + ROCKETMQ_NAMESPACE_TOPIC_SEP + pulsarTopicName
                    .getLocalName();
        } else {
            return MixAll.DLQ_GROUP_TOPIC_PREFIX + ROCKETMQ_NAMESPACE_TOPIC_SEP + pulsarTopicName.getLocalName();
        }
    }

    public String getMetaNoDomainTopic() {
        return Joiner.on('/').join(metaTenant, metaNamespace, pulsarTopicName.getLocalName());
    }

    public String getDefaultNoDomainTopic() {
        return Joiner.on('/').join(defaultTenant, defaultNamespace, pulsarTopicName.getLocalName());
    }

    public String getOrigNoDomainTopicName() {
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

    public boolean isDLQTopic() {
        return Strings.isNotBlank(pulsarTopicName.getLocalName()) && NamespaceUtil.isDLQTopic(pulsarTopicName.getLocalName());
    }

    public boolean isRetryTopic() {
        return Strings.isNotBlank(pulsarTopicName.getLocalName()) && NamespaceUtil.isRetryTopic(pulsarTopicName.getLocalName());
    }

}

