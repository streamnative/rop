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

package com.tencent.rocketmq.example.namespace;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

/**
 * Push consumer with namespace.
 */
public class PushConsumerWithNamespace {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(
                "test1|InstanceTest", "cidTest");
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        defaultMQPushConsumer.subscribe("topicTest", "*");
        AtomicLong count = new AtomicLong(0L);
        defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> {
                System.out.printf("Msg payload: %s, topic is:%s, MsgId is:%s, reconsumeTimes is:%s%n",
                        new String(msg.getBody()), msg.getTopic(), msg.getMsgId(), msg.getReconsumeTimes());
                count.incrementAndGet();
                System.out.println("total ====> " + count.get());
            });
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });

        defaultMQPushConsumer.start();
    }
}