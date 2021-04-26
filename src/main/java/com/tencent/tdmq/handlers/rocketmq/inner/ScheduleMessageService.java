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

package com.tencent.tdmq.handlers.rocketmq.inner;

import com.google.common.base.Preconditions;
import com.tencent.tdmq.handlers.rocketmq.RocketMQServiceConfiguration;
import com.tencent.tdmq.handlers.rocketmq.inner.format.RopEntryFormatter;
import com.tencent.tdmq.handlers.rocketmq.inner.timer.SystemTimer;
import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.MessageExtBrokerInner;

/**
 * Schedule message service.
 */
@Slf4j
public class ScheduleMessageService {

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 1000L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private static final int MAX_FETCH_MESSAGE_NUM = 20;
    private static final int PRODUCER_EXPIRED_TIME_SEC = 60 * 60;
    private static final int PRODUCER_CACHE_SIZE = 200;
    /*  key is delayed level  value is delay timeMillis */
    private final ConcurrentLongHashMap<Long> delayLevelTable;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final RocketMQServiceConfiguration config;
    private final RocketMQBrokerController rocketBroker;
    private final ServiceThread expirationReaper;
    private final Timer timer = new Timer();
    private final ConcurrentHashMap<String, Producer<byte[]>> sendBackProdcuer;
    private String scheduleTopicPrefix;
    private int maxDelayLevel;
    private String[] delayLevelArray;
    private BrokerService pulsarBroker;
    private List<DeliverDelayedMessageTimerTask> deliverDelayedMessageManager;

    public ScheduleMessageService(final RocketMQBrokerController rocketBroker, RocketMQServiceConfiguration config) {
        this.config = config;
        this.rocketBroker = rocketBroker;
        this.scheduleTopicPrefix = config.getRmqScheduleTopic();
        this.delayLevelTable = new ConcurrentLongHashMap<>(config.getMaxDelayLevelNum(), 1);
        this.maxDelayLevel = config.getMaxDelayLevelNum();
        this.parseDelayLevel();
        this.sendBackProdcuer = new ConcurrentHashMap<>();
        this.expirationReaper = new ServiceThread() {
            @Override
            public String getServiceName() {
                return "ScheduleMessageService-expirationReaper-thread";
            }

            @Override
            public void run() {
                log.info(getServiceName() + " service started.");
                Preconditions.checkNotNull(deliverDelayedMessageManager);
                while (!this.isStopped()) {
                    deliverDelayedMessageManager.stream().forEach((i) -> {
                        i.advanceClock(200);
                    });
                }
            }
        };
        this.expirationReaper.setDaemon(true);
    }

    public String getDelayedTopicName(int timeDelayedLevel) {
        String delayedTopicName = ScheduleMessageService.this.scheduleTopicPrefix + CommonUtils.UNDERSCORE_CHAR
                + delayLevelArray[timeDelayedLevel - 1];
        RocketMQTopic rocketMQTopic = RocketMQTopic.getRocketMQMetaTopic(delayedTopicName);
        return rocketMQTopic.getPulsarFullName();
    }

    public String getDelayedTopicConsumerName(int timeDelayedLevel) {
        String delayedTopicName = ScheduleMessageService.this.scheduleTopicPrefix + CommonUtils.UNDERSCORE_CHAR
                + delayLevelArray[timeDelayedLevel - 1];
        RocketMQTopic rocketMQTopic = RocketMQTopic.getRocketMQMetaTopic(delayedTopicName);
        return rocketMQTopic.getOrigNoDomainTopicName() + CommonUtils.UNDERSCORE_CHAR + "consumer";
    }

    public long computeDeliverTimestamp(final long delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }
        return storeTimestamp + 1000;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            this.pulsarBroker = rocketBroker.getBrokerService();
            this.deliverDelayedMessageManager = delayLevelTable.keys().stream()
                    .collect(ArrayList::new, (arr, level) -> {
                        arr.add(new DeliverDelayedMessageTimerTask(level));
                    }, ArrayList::addAll);
            this.deliverDelayedMessageManager
                    .forEach((i) -> this.timer.schedule(i, FIRST_DELAY_TIME, DELAY_FOR_A_WHILE));
            this.expirationReaper.start();
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            expirationReaper.shutdown();
            deliverDelayedMessageManager.forEach(DeliverDelayedMessageTimerTask::close);
            sendBackProdcuer.values().stream().forEach(Producer::closeAsync);
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.config.getMessageDelayLevel();
        try {
            this.delayLevelArray = levelString.split(" ");
            for (int i = 0; i < delayLevelArray.length; i++) {
                String value = delayLevelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);
                int level = i + 1;
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.putIfAbsent(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception, evelString String = {}", levelString, e);
            return false;
        }
        return true;
    }

    class DeliverDelayedMessageTimerTask extends TimerTask {

        private static final int PULL_MESSAGE_TIMEOUT_MS = 200;
        private static final int MAX_BATCH_SIZE = 2000;
        private final PulsarService pulsarService;
        private final long delayLevel;
        private final String delayTopic;
        private final Consumer<byte[]> delayedConsumer;
        private RopEntryFormatter formatter = new RopEntryFormatter();
        private SystemTimer timeoutTimer;

        public DeliverDelayedMessageTimerTask(long delayLevel) {
            this.delayLevel = delayLevel;
            this.delayTopic = getDelayedTopicName((int) delayLevel);
            this.pulsarService = ScheduleMessageService.this.pulsarBroker.pulsar();
            this.timeoutTimer = SystemTimer.builder().executorName("DeliverDelayedMessageTimeWheelExecutor").build();
            try {
                this.delayedConsumer = this.pulsarService.getClient()
                        .newConsumer()
                        .receiverQueueSize(MAX_FETCH_MESSAGE_NUM)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionType(SubscriptionType.Shared)
                        .subscriptionName(getDelayedTopicConsumerName((int) delayLevel))
                        .topic(this.delayTopic)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();
            } catch (Exception e) {
                log.error("create delayed topic[delayLevel={}] consumer error.", delayLevel, e);
                throw new RuntimeException("Create delayed topic error");
            }
        }

        public void close() {
            if (delayedConsumer != null) {
                delayedConsumer.closeAsync();
                timeoutTimer.shutdown();
            }
        }

        public void advanceClock(long timeoutMs) {
            timeoutTimer.advanceClock(timeoutMs);
        }

        @Override
        public void run() {
            try {
                Preconditions.checkNotNull(this.delayedConsumer);
                int i = 0;
                while (i++ < MAX_BATCH_SIZE && timeoutTimer.size() < MAX_BATCH_SIZE && !this.delayedConsumer
                        .isConnected()) {
                    Message<byte[]> message = this.delayedConsumer
                            .receive(PULL_MESSAGE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    if (message == null) {
                        break;
                    }
                    MessageExt messageExt = this.formatter.decodePulsarMessage(message);
                    long deliveryTime = computeDeliverTimestamp(this.delayLevel, messageExt.getStoreTimestamp());
                    long diff = deliveryTime - Instant.now().toEpochMilli();
                    diff = diff < 0 ? 0 : diff;
                    timeoutTimer.add(new com.tencent.tdmq.handlers.rocketmq.inner.timer.TimerTask(diff) {
                        @Override
                        public void run() {
                            try {
                                MessageExtBrokerInner msgInner = messageTimeup(messageExt);
                                if (MixAll.RMQ_SYS_TRANS_HALF_TOPIC.equals(messageExt.getTopic())) {
                                    log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                            messageExt.getTopic(), messageExt);
                                    return;
                                }

                                RocketMQTopic rmqTopic = new RocketMQTopic(msgInner.getTopic());
                                int partitionId = msgInner.getQueueId();
                                String pTopic = rmqTopic.getPartitionName(partitionId);
                                Producer<byte[]> producer = sendBackProdcuer.get(pTopic);
                                if (producer == null || !producer.isConnected()) {
                                    synchronized (sendBackProdcuer) {
                                        if (producer == null || !producer.isConnected()) {
                                            try {
                                                producer = pulsarService.getClient().newProducer()
                                                        .topic(pTopic)
                                                        .producerName(pTopic + "_delayedMessageSender")
                                                        .enableBatching(true)
                                                        .sendTimeout(3000, TimeUnit.MILLISECONDS)
                                                        .create();
                                            } catch (Exception e) {
                                                log.warn("create delayedMessageSender error.", e);
                                            }
                                        }
                                        Producer<byte[]> oldProducer = sendBackProdcuer.put(pTopic, producer);
                                        if (oldProducer != null) {
                                            oldProducer.closeAsync();
                                        }
                                    }
                                }
                                producer.send(formatter.encode(msgInner, 1).get(0));
                                delayedConsumer.acknowledge(message.getMessageId());
                            } catch (Exception ex) {
                                log.warn("create delayedMessageSender error.", ex);
                                delayedConsumer.negativeAcknowledge(message.getMessageId());
                            }
                        }
                    });
                }
            } catch (Exception e) {
                log.warn("DeliverDelayedMessageTimerTask[delayLevel={}] pull message exception.", this.delayLevel,
                        e);
            }
        }

        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
