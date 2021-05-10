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

package org.streamnative.pulsar.handlers.rocketmq.inner.producer;

import com.google.common.base.Joiner;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

/**
 * Client topic name.
 */
@Data
@EqualsAndHashCode
@ToString
public class ClientTopicName {

    private final String rmqTopicName;
    private final String pulsarTopicName;

    public ClientTopicName(String rmqTopicName) {
        this.rmqTopicName = rmqTopicName;
        this.pulsarTopicName = CommonUtils.pulsarGroupName(this.rmqTopicName);
    }

    public ClientTopicName(TopicName tdmpTopicName) {
        TopicName tempTopic = TopicName.get(tdmpTopicName.getPartitionedTopicName());
        this.pulsarTopicName = Joiner.on("/")
                .join(tempTopic.getTenant(), tempTopic.getNamespacePortion(), tempTopic.getLocalName());
        if (tdmpTopicName.getTenant() == RocketMQTopic.metaTenant
                && (tdmpTopicName.getNamespacePortion() == RocketMQTopic.metaNamespace
                || tdmpTopicName.getNamespacePortion() == RocketMQTopic.defaultNamespace)) {
            this.rmqTopicName = tempTopic.getLocalName();
        } else {
            String rmqNamespace = tempTopic.getTenant() + "|" + tempTopic.getNamespacePortion();
            this.rmqTopicName = NamespaceUtil.wrapNamespace(rmqNamespace, tempTopic.getLocalName());
        }
    }

    public TopicName toPulsarTopicName() {
        return TopicName.get(this.pulsarTopicName);
    }
}
