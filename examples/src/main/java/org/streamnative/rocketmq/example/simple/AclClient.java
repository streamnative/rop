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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Acl Client.
 */
public class AclClient {

    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    private static final String ACL_ACCESS_KEY = "eyJrZXlJZCI6InJvY2tldG1xLW13bmI3bWFwMjhqZSIsImFsZyI6IkhTMjU2In0."
            + "eyJzdWIiOiJyb2NrZXRtcS1td25iN21hcDI4amVfdGVzdCJ9.BDOjqqY25a6apnZTMZCqg0I0pxVFcqz7fvZbaTqkf5U";
    private static final String ACL_SECRET_KEY = "rop";

    public static void main(String[] args) throws MQClientException, InterruptedException {
        producer();
        pushConsumer();
//        pullConsumer();
    }

    public static void producer() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("rocketmq-mwnb7map28je|nit", "ProducerGroupName",
                getAclRPCHook());
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            try {
                {
                    Message msg = new Message("nit-topic",
                            "TagA",
                            "OrderID188",
                            "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.shutdown();
    }

    public static void pushConsumer() throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocketmq-mwnb7map28je|nit",
                "please_rename_unique_group_name_5", getAclRPCHook(),
                new AllocateMessageQueueAveragely());
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("nit-topic", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // Wrong time format 2017_0422_221800
        consumer.setConsumeTimestamp("20180422221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                printBody(msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    public static void pullConsumer() throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("rocketmq-mwnb7map28je|nit",
                "please_rename_unique_group_name_6",
                getAclRPCHook());
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.start();

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("nit-topic");
        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the queue: %s%n", mq);
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult =
                            consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n", pullResult);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    printBody(pullResult);
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        consumer.shutdown();
    }

    private static void printBody(PullResult pullResult) {
        printBody(pullResult.getMsgFoundList());
    }

    private static void printBody(List<MessageExt> msg) {
        if (msg == null || msg.size() == 0) {
            return;
        }
        for (MessageExt m : msg) {
            if (m != null) {
                System.out.printf("msgId : %s  body : %s  \r", m.getMsgId(), new String(m.getBody(),
                        StandardCharsets.UTF_8));
            }
        }
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null) {
            return offset;
        }

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }

    static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(ACL_ACCESS_KEY, ACL_SECRET_KEY));
    }

}
