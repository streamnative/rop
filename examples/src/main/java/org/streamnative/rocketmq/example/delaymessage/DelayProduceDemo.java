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
