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

package com.tencent.tdmq.handlers.rocketmq.producer;

import static org.junit.Assert.assertEquals;

import com.tencent.tdmq.handlers.rocketmq.inner.producer.ClientGroupName;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Test;

public class ClientGroupNameTest {

    @Test
    public void testClientGroupName() {
        String oldGroupName = "test1|InstanceTest%cidTest";

        ClientGroupName clientGroupName = new ClientGroupName(oldGroupName);
        assertEquals("test1|InstanceTest%cidTest", clientGroupName.getRmqGroupName());
        assertEquals("test1/InstanceTest/cidTest", clientGroupName.getPulsarGroupName());
    }

    @Test
    public void testClientGroupNameByTopicName() {
        TopicName topicName = TopicName.get("test/test-ns/test-group");
        ClientGroupName clientGroupName = new ClientGroupName(topicName);

        assertEquals("test|test-ns%test-group", clientGroupName.getRmqGroupName());
        assertEquals("test/test-ns/test-group", clientGroupName.getPulsarGroupName());

        TopicName topicName1 = TopicName.get("test-group");
        ClientGroupName clientGroupName1 = new ClientGroupName(topicName1);
        assertEquals("public|default%test-group", clientGroupName1.getRmqGroupName());
        assertEquals("public/default/test-group", clientGroupName1.getPulsarGroupName());
    }
}
