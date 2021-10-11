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

package org.streamnative.pulsar.handlers.rocketmq.rop.producer;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQTestBase;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

/**
 * RocketMQ Test Case.
 */
@Slf4j
public class ProducerTestCase extends RocketMQTestBase {

    @Test(timeOut = 1000 * 60)
    @Ignore
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

    @Test(timeOut = 1000 * 60)
    @Ignore
    public void producerWithNamespace() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("test1|InstanceTest", "pidTest");
        String nameSrvAddr = "127.0.0.1:" + getRocketmqBrokerPortList().get(0);
        producer.setNamesrvAddr(nameSrvAddr);
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("topicTest", "TagA", "Hello world".getBytes("GBK"));
            try {
                SendResult sendResult = producer.send(message);
                Assert.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.shutdown();
    }

    @Test(timeOut = 60 * 1000)
    @Ignore
    public void batchProducerTest() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducerGroupName");
        String nameSrvAddr = "127.0.0.1:" + getRocketmqBrokerPortList().get(0);
        producer.setNamesrvAddr(nameSrvAddr);
        producer.start();

        //If you just send messages of no more than 1MiB at a time, it is easy to use batch
        //Messages of the same batch should have: same topic, same waitStoreMsgOK and no schedule support
        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "Tag", "OrderID001", "Hello world 0".getBytes("GBK")));
        messages.add(new Message(topic, "Tag", "OrderID002", "Hello world 1".getBytes("GBK")));
        messages.add(new Message(topic, "Tag", "OrderID003", "Hello world 2".getBytes("GBK")));

        SendResult sendResult = producer.send(messages);
        Assert.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
    }

    @Test(timeOut = 60 * 1000)
    @Ignore
    public void orderProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_producer_name");
        String nameSrvAddr = "127.0.0.1:" + getRocketmqBrokerPortList().get(0);
        producer.setNamesrvAddr(nameSrvAddr);
        producer.start();

        String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            int orderId = i % 10;
            Message msg =
                    new Message("OrderTopic", tags[i % tags.length], "KEY" + i,
                            ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);

            Assert.assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
        }

        producer.shutdown();
    }
}
