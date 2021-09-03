package org.streamnative.rocketmq.example.delaymessage;

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
                Message msg = new Message("test-topic", ("No." + ++i + " Hello World").getBytes());

                // 延时消息，单位毫秒（ms），在指定延迟时间（当前时间之后）进行投递，例如消息在10秒后投递。
                long delayTime = System.currentTimeMillis() + 10000;
                // 设置消息需要被投递的时间。
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
