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

package org.streamnative.pulsar.handlers.rocketmq.utils;

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.testng.Assert.assertEquals;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.testng.annotations.Test;

/**
 * Validate TopicNameUtils.
 */
@Slf4j
public class TopicNameUtilsTest {

    @Test(timeOut = 20000)
    public void testTopicNameConvert() throws Exception {
        String topicName = "kopTopicNameConvert";
        int partitionNumber = 77;
        MessageQueue topicPartition = new MessageQueue(topicName, "test-cluster", partitionNumber);

        String tenantName = "tenant_name";
        String nsName = "ns_name";
        NamespaceName ns = NamespaceName.get(tenantName, nsName);
        String expectedPulsarName = "persistent://" + tenantName + "/" + nsName + "/"
                + topicName + PARTITIONED_TOPIC_SUFFIX + partitionNumber;

        TopicName topicName1 = TopicNameUtils.pulsarTopicName(topicPartition, ns);
        TopicName topicName2 = TopicNameUtils.pulsarTopicName(topicName, partitionNumber, ns);

        assertEquals(expectedPulsarName, topicName1.toString());
        assertEquals(expectedPulsarName, topicName2.toString());

        TopicName topicName3 = TopicNameUtils.pulsarTopicName(topicName, ns);
        String expectedPulsarName3 = "persistent://" + tenantName + "/" + nsName + "/"
                + topicName;
        assertEquals(expectedPulsarName3, topicName3.toString());
    }
}
