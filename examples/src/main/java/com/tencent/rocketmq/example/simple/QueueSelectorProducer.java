package com.tencent.rocketmq.example.simple;

import java.util.List;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

public class QueueSelectorProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("test1|InstanceTest5", "pidTest");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        for (int i = 20000; i < 30000; i++) {
            Message message = new Message("topicTest", "tagTest", ("Hello world — " + i).getBytes());
            try {
                SendResult result = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        return list.get(0);
                    }
                }, null);
                System.out.printf("Topic:%s send success, misId is:%s, queueId is: %s%n", message.getTopic(),
                        result.getMsgId(), result.getMessageQueue().getQueueId());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();
    }
}
