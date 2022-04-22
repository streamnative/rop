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

package org.streamnative.pulsar.handlers.rocketmq.inner;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;
import static org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils.ROP_MESSAGE_ID;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQServiceConfiguration;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopServerException;
import org.streamnative.pulsar.handlers.rocketmq.inner.exception.RopTopicNotExistsException;
import org.streamnative.pulsar.handlers.rocketmq.inner.format.RopEntryFormatter;
import org.streamnative.pulsar.handlers.rocketmq.inner.timer.SystemTimer;
import org.streamnative.pulsar.handlers.rocketmq.utils.CommonUtils;
import org.streamnative.pulsar.handlers.rocketmq.utils.PulsarUtil;
import org.streamnative.pulsar.handlers.rocketmq.utils.RocketMQTopic;

/**
 * Schedule message service.
 */
@Slf4j
public class ScheduleMessageService {

    private static final long ADVANCE_TIME_INTERVAL = 100L;
    private static final long FIRST_DELAY_TIME = 200L;
    private static final long DELAY_FOR_A_WHILE = 500L;
    private static final int MAX_FETCH_MESSAGE_NUM = 100;
    /*  key is delayed level  value is delay timeMillis */
    private final Map<Integer, Long> delayLevelTable;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final RocketMQServiceConfiguration config;
    private final RocketMQBrokerController rocketBroker;
    private final ScheduledExecutorService timer = Executors.newScheduledThreadPool(8);
    private final ScheduledExecutorService advancedTimer = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, Producer<byte[]>> sendBackProducers;
    private final String scheduleTopicPrefix;
    private String[] delayLevelArray;
    private List<DeliverDelayedMessageTimerTask> deliverDelayedMessageManager;

    public ScheduleMessageService(final RocketMQBrokerController rocketBroker, RocketMQServiceConfiguration config) {
        this.config = config;
        this.rocketBroker = rocketBroker;
        this.scheduleTopicPrefix = config.getRmqScheduleTopic();
        this.delayLevelTable = new HashMap<>(config.getMaxDelayLevelNum());
        this.sendBackProducers = new ConcurrentHashMap<>();
        this.parseDelayLevel();
    }

    public String getDelayedTopicName(int timeDelayedLevel) {
        String delayedTopicName = ScheduleMessageService.this.scheduleTopicPrefix + CommonUtils.UNDERSCORE_CHAR
                + delayLevelArray[timeDelayedLevel - 1];
        RocketMQTopic rocketMQTopic = RocketMQTopic.getRocketMQMetaTopic(delayedTopicName);
        return rocketMQTopic.getPulsarFullName();
    }

    public String getTimingTopicName() {
        String delayedTopicName = ScheduleMessageService.this.scheduleTopicPrefix + CommonUtils.UNDERSCORE_CHAR
                + delayLevelArray[0];
        RocketMQTopic rocketMQTopic = RocketMQTopic.getRocketMQMetaTopic(delayedTopicName);
        return rocketMQTopic.getPulsarFullName();
    }

    public String getDelayedTopicName(int timeDelayedLevel, int partitionId) {
        String delayedTopicName = ScheduleMessageService.this.scheduleTopicPrefix + CommonUtils.UNDERSCORE_CHAR
                + delayLevelArray[timeDelayedLevel - 1];
        RocketMQTopic rocketMQTopic = RocketMQTopic.getRocketMQMetaTopic(delayedTopicName);
        return rocketMQTopic.getPartitionTopicName(partitionId).toString();
    }

    public String getDelayedTopicConsumerName(int timeDelayedLevel) {
        String delayedTopicName = ScheduleMessageService.this.scheduleTopicPrefix + CommonUtils.UNDERSCORE_CHAR
                + delayLevelArray[timeDelayedLevel - 1];
        return delayedTopicName + CommonUtils.UNDERSCORE_CHAR + "consumer";
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }
        return storeTimestamp + 1000;
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            this.createDelayedSubscription();
            this.deliverDelayedMessageManager = delayLevelTable.keySet().stream()
                    .collect(ArrayList::new, (arr, level) -> {
                        arr.add(new DeliverDelayedMessageTimerTask(level));
                    }, ArrayList::addAll);
            this.deliverDelayedMessageManager
                    .forEach(task -> {
                        this.timer
                                .scheduleWithFixedDelay(task, FIRST_DELAY_TIME, DELAY_FOR_A_WHILE,
                                        TimeUnit.MILLISECONDS);
                        this.advancedTimer.scheduleWithFixedDelay(() -> task.advanceClock(ADVANCE_TIME_INTERVAL),
                                FIRST_DELAY_TIME,
                                ADVANCE_TIME_INTERVAL, TimeUnit.MILLISECONDS);
                    });
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            deliverDelayedMessageManager.forEach(DeliverDelayedMessageTimerTask::close);
            sendBackProducers.values().forEach(Producer::closeAsync);
            timer.shutdownNow();
            advancedTimer.shutdownNow();
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
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception, evelString String = {}", levelString, e);
            return false;
        }
        return true;
    }

    class DeliverDelayedMessageTimerTask extends TimerTask {

        private static final int PULL_MESSAGE_TIMEOUT_MS = 1000;
        private static final int SEND_MESSAGE_TIMEOUT_MS = 1000;
        private final RocketMQBrokerController rocketBroker;
        private final int delayLevel;
        private final RopEntryFormatter formatter = new RopEntryFormatter();
        private final SystemTimer timeoutTimer;
        private Consumer<byte[]> delayedConsumer = null;

        public DeliverDelayedMessageTimerTask(int delayLevel) {
            this.delayLevel = delayLevel;
            this.rocketBroker = ScheduleMessageService.this.rocketBroker;
            this.timeoutTimer = SystemTimer.builder().executorName("DeliverDelayedMessageTimeWheelExecutor").build();
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

                createConsumerIfNotExists();
                while (timeoutTimer.size() < config.getMaxScheduleMsgBatchSize()
                        && ScheduleMessageService.this.isStarted()) {
                    Message<byte[]> message = this.delayedConsumer
                            .receive(MAX_FETCH_MESSAGE_NUM, TimeUnit.MILLISECONDS);
                    if (Objects.isNull(message)) {
                        break;
                    }

                    MessageExt messageExt = RopEntryFormatter.decodePulsarMessage(message);
                    String deliverTimeStr = messageExt.getProperties().get(CommonUtils.DELIVER_AT_TIME_PROPERTY_NAME);
                    long deliveryTime = deliverTimeStr == null ? computeDeliverTimestamp(this.delayLevel,
                            messageExt.getStoreTimestamp()) : NumberUtils.toLong(deliverTimeStr);
                    long diff = deliveryTime - Instant.now().toEpochMilli();
                    diff = diff < 0 ? 0 : diff;
                    timeoutTimer
                            .add(new org.streamnative.pulsar.handlers.rocketmq.inner.timer.TimerTask(diff) {
                                @Override
                                public void run() {
                                    try {
                                        MessageExtBrokerInner msgInner = messageTimeup(messageExt);
                                        log.debug("[{}] Retry delayedTime: needDelayMs=[{}],real diff =[{}].",
                                                msgInner.getTopic(),
                                                this.delayMs,
                                                deliveryTime - Instant.now().toEpochMilli());
                                        if (MixAll.RMQ_SYS_TRANS_HALF_TOPIC.equals(messageExt.getTopic())) {
                                            log.error("[BUG] the real topic of schedule msg is {}, "
                                                            + "discard the msg. msg={}",
                                                    messageExt.getTopic(), messageExt);
                                            return;
                                        }

                                        final String ropMsgId = msgInner.getProperties()
                                                .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                                        RocketMQTopic rmqTopic = new RocketMQTopic(msgInner.getTopic());
                                        String pTopic = rmqTopic.getPulsarFullName();
                                        Producer<byte[]> producer = getProducerFromCache(pTopic);
                                        producer.newMessage()
                                                .value(formatter.encode(msgInner, 1).get(0))
                                                .property(ROP_MESSAGE_ID, ropMsgId)
                                                .sendAsync()
                                                .whenCompleteAsync((msgId, ex) -> {
                                                    if (ex == null) {
                                                        /*TODO: how to notify RetryTopic consumer pull message
                                                         * at once. need rpc to notify, for some partition isn't
                                                         * on current broker.
                                                         */
                                                        delayedConsumer.acknowledgeAsync(message);
                                                    } else {
                                                        log.warn("DelayedMessageSender send message to [{}] failed.",
                                                                pTopic);
                                                        delayedConsumer.negativeAcknowledge(message);
                                                    }
                                                });
                                    } catch (RopTopicNotExistsException topicNotExistsException) {
                                        log.warn(
                                                "DelayedMessageSender discard sending message[{}], because: {}.",
                                                message.getMessageId(), topicNotExistsException.getMessage());
                                        delayedConsumer.acknowledgeAsync(message);
                                    } catch (Exception ex) {
                                        log.warn("DelayedMessageSender send message[{}] failed.",
                                                message.getMessageId(), ex);
                                        delayedConsumer.negativeAcknowledge(message);
                                    }
                                }
                            });
                }
            } catch (
                    Exception e) {
                log.warn("DeliverDelayedMessageTimerTask[delayLevel={}] pull message exception.", this.delayLevel,
                        e);
                if (!ScheduleMessageService.this.isStarted()) {
                    Thread.interrupted();
                }
            }
        }

        private Producer<byte[]> getProducerFromCache(String pulsarTopic)
                throws RopServerException, RopTopicNotExistsException {
            AtomicBoolean topicNotExists = new AtomicBoolean(false);
            Producer<byte[]> producer = sendBackProducers.computeIfAbsent(pulsarTopic, key -> {
                log.info("getProducerFromCache [topic={}].", pulsarTopic);
                try {
                    CompletableFuture<PartitionedTopicMetadata> topicMetaFuture = rocketBroker
                            .getBrokerService().fetchPartitionedTopicMetadataAsync(TopicName.get(pulsarTopic));

                    PartitionedTopicMetadata topicMeta = topicMetaFuture.get();
                    if (Objects.nonNull(topicMeta) && topicMeta.partitions > 0) {
                        ProducerBuilder<byte[]> producerBuilder = rocketBroker.getRopBrokerProxy().getPulsarClient()
                                .newProducer()
                                .maxPendingMessages(config.getMaxScheduleMsgBatchSize());

                        return producerBuilder.clone()
                                .topic(pulsarTopic)
                                .producerName(pulsarTopic + "_ScheduleMessageSender_" + System.currentTimeMillis())
                                .enableBatching(false)
                                .blockIfQueueFull(true)
                                .sendTimeout(SEND_MESSAGE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                .create();
                    } else {
                        log.warn("getProducerFromCache failed, for [topic = {}] not exists, and remove.", pulsarTopic);
                        topicNotExists.set(true);
                        return null;
                    }
                } catch (Exception e) {
                    log.warn("getProducerFromCache[topic={}] error.", pulsarTopic, e);
                }
                return null;
            });

            if (Objects.nonNull(producer) && producer.isConnected()) {
                return producer;
            } else if (topicNotExists.get()) {
                throw new RopTopicNotExistsException(String.format("Maybe topic[%s] have been deleted.", pulsarTopic));
            } else {
                if (Objects.nonNull(producer)) {
                    producer.closeAsync();
                }
                sendBackProducers.remove(pulsarTopic);
                throw new RopServerException(
                        "[ScheduleMessageService] getProducerFromCache error, maybe producer is wrong.");
            }
        }

        private void createConsumerIfNotExists() {
            try {
                if (delayedConsumer != null) {
                    return;
                }
                PulsarClientImpl pulsarClient = rocketBroker.getRopBrokerProxy().getPulsarClient();
                log.info("Begin to Create delayed consumer of topic[delayLevel={}], the client config value: [{}]",
                        delayLevel, pulsarClient.getConfiguration());
                this.delayedConsumer = rocketBroker.getRopBrokerProxy().getPulsarClient()
                        .newConsumer()
                        .receiverQueueSize(config.getMaxScheduleMsgBatchSize())
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionType(SubscriptionType.Shared)
                        .subscriptionName(getDelayedTopicConsumerName(delayLevel))
                        .topic(getDelayedTopicName(delayLevel))
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();
                log.info("The client config value: [{}]", pulsarClient.getConfiguration());
            } catch (Exception e) {
                log.error("Create delayed topic[delayLevel={}] consumer error.", delayLevel, e);
                throw new RuntimeException("Create delay consumer error");
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

    private void createDelayedSubscription() throws PulsarServerException {
        PulsarAdmin adminClient = rocketBroker.getBrokerService().pulsar().getAdminClient();
        Preconditions.checkNotNull(adminClient, "pulsar admin client can't be null.");
        IntStream.range(1, delayLevelArray.length + 1).forEach((delayedLevel) -> {
            log.info("createDelayedSubscription for delayedLevel: {}. ", delayedLevel);
            String consumerName = getDelayedTopicConsumerName(delayedLevel);
            String topicName = getDelayedTopicName(delayedLevel);
            PulsarUtil.createSubscriptionIfNotExist(adminClient, topicName, consumerName, MessageId.earliest);
        });
    }
}
