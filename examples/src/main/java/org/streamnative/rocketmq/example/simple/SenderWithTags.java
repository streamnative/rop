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

package org.streamnative.rocketmq.example.simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * Sender with tags.
 */
public class SenderWithTags {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("test1|InstanceTest5", "pidTest");
        producer.start();
        String[] tags = {"tagA", "tagB"};
        for (int i = 1; i < 100; i++) {
            Message message = new Message("topicTest", tags[i % 2], ("Hello world — " + i).getBytes());
            try {
                SendResult result = producer.send(message);
                System.out.printf("Topic:%s send success, misId is:%s, queueId is: %s%n", message.getTopic(),
                        result.getMsgId(), result.getMessageQueue().getQueueId());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}
