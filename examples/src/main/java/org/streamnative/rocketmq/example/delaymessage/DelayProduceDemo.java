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

package org.streamnative.rocketmq.example.delaymessage;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * Delay producer demo.
 */
public class DelayProduceDemo {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("test-producer-group");
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setNamespace("test-cluster|test-ns");
        producer.start();

        int i = 0;
        while (!Thread.interrupted()) {
            try {
                Message msg = new Message("test-topic",
                        ("No." + ++i + " Hello World").getBytes(StandardCharsets.UTF_8));

                // Delayed messages, in milliseconds (ms),
                // are delivered after the specified delay time (after the current time),
                // for example, the message will be delivered after 10 seconds.
                long delayTime = System.currentTimeMillis() + 10000;
                // Set the time when the message needs to be delivered.
                msg.putUserProperty("__STARTDELIVERTIME", String.valueOf(delayTime));

                SendResult result = producer.send(msg);
                System.out.println("Send delay message: " + i + " result：" + result);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }
    }
}
