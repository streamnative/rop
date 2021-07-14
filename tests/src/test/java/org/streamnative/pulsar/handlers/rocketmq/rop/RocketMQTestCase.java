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

package org.streamnative.pulsar.handlers.rocketmq.rop;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * RocketMQ Test Case.
 */
@Slf4j
public class RocketMQTestCase extends RocketMQTestBase {

    @Test(timeOut = 1000 * 5)
    public void simpleProducerTest() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_group_name");
        String nameSrvAddr = "127.0.0.1:" + getRocketmqBrokerPortList().get(0);
        producer.setNamesrvAddr(nameSrvAddr);
        producer.start();

        for (int i = 0; i < 20; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                Message msg = new Message("TopicTest" /* Topic */,
                        "TagA" /* Tag */,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                /*
                 * Call send message to deliver message to one of brokers.
                 */
                SendResult sendResult = producer.send(msg);
                Assert.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
