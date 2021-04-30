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

package com.tencent.tdmq.handlers.rocketmq.inner.producer;

import static org.junit.Assert.assertEquals;

import org.apache.pulsar.common.naming.TopicName;
import org.junit.Test;

/**
 * Test client topic name.
 */
public class ClientTopicNameTest {

    @Test
    public void testClientTopicName() {
        String oldTopicName = "test1|InstanceTest%cidTest";

        ClientTopicName clientTopicName = new ClientTopicName(oldTopicName);
        assertEquals("test1|InstanceTest%cidTest", clientTopicName.getRmqTopicName());
        assertEquals("test1/InstanceTest/cidTest", clientTopicName.getPulsarTopicName());
    }

    @Test
    public void testClientTopicNameByTopicName() {
        TopicName topicName = TopicName.get("test/test-ns/test-topic");
        ClientTopicName clientTopicName = new ClientTopicName(topicName);

        assertEquals("test|test-ns%test-topic", clientTopicName.getRmqTopicName());
        assertEquals("test/test-ns/test-topic", clientTopicName.getPulsarTopicName());

        TopicName topicName1 = TopicName.get("test-topic");
        ClientTopicName clientTopicName1 = new ClientTopicName(topicName1);
        assertEquals("public|default%test-topic", clientTopicName1.getRmqTopicName());
        assertEquals("public/default/test-topic", clientTopicName1.getPulsarTopicName());
    }
}
