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

import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.PERCENTAGE_CHAR;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.SLASH_CHAR;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.VERTICAL_LINE_CHAR;

import com.google.common.base.Joiner;
import java.io.Serializable;
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
public class ClientGroupName implements Serializable {

    private final String rmqGroupName;
    private final String pulsarGroupName;

    public ClientGroupName(String rmqGroupName) {
        this.rmqGroupName = rmqGroupName;
        this.pulsarGroupName = CommonUtils.tdmqGroupName(this.rmqGroupName);
    }

    public ClientGroupName(TopicName pulsarGroupName) {
        this.pulsarGroupName = Joiner.on(SLASH_CHAR)
                .join(pulsarGroupName.getTenant(), pulsarGroupName.getNamespacePortion(),
                        pulsarGroupName.getLocalName());
        if (pulsarGroupName.getTenant() == RocketMQTopic.metaTenant
                && (pulsarGroupName.getNamespacePortion() == RocketMQTopic.metaNamespace
                || pulsarGroupName.getNamespacePortion() == RocketMQTopic.defaultNamespace)) {
            this.rmqGroupName = pulsarGroupName.getLocalName();
        } else {
            this.rmqGroupName =
                    pulsarGroupName.getTenant() + VERTICAL_LINE_CHAR + pulsarGroupName.getNamespacePortion()
                            + PERCENTAGE_CHAR + pulsarGroupName
                            .getLocalName();
        }
    }
}
