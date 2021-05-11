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
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.common.naming.TopicName;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Client Group Name, consists of rmqGroupName and pulsarGroupName.
 */
@Data
@EqualsAndHashCode
@ToString
public class ClientGroupName {

    private final String rmqGroupName;
    private final String pulsarGroupName;

    public ClientGroupName(String rmqGroupName) {
        this.rmqGroupName = rmqGroupName;
        this.pulsarGroupName = CommonUtils.pulsarGroupName(this.rmqGroupName);
    }

    public ClientGroupName(TopicName tdmpGroupName) {
        this.pulsarGroupName = Joiner.on("/")
                .join(tdmpGroupName.getTenant(), tdmpGroupName.getNamespacePortion(), tdmpGroupName.getLocalName());
        if (tdmpGroupName.getTenant() == RocketMQTopic.metaTenant
                && (tdmpGroupName.getNamespacePortion() == RocketMQTopic.metaNamespace
                || tdmpGroupName.getNamespacePortion() == RocketMQTopic.defaultNamespace)) {
            this.rmqGroupName = tdmpGroupName.getLocalName();
        } else {
            this.rmqGroupName =
                    tdmpGroupName.getTenant() + "|" + tdmpGroupName.getNamespacePortion() + "%" + tdmpGroupName
                            .getLocalName();
        }
    }
}
