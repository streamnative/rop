package com.tencent.tdmq.handlers.rocketmq.inner;
import com.tencent.tdmq.handlers.rocketmq.RocketMQServiceConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;

@Slf4j
public class ScheduleMessageService {
    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(32);
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private Timer timer;
    @Getter
    private int maxDelayLevel;
    private String[] delayLevelArray;
    private final RocketMQServiceConfiguration config;
    private final RocketMQBrokerController rocketBroker;
    private final BrokerService pulsarBroker;

    public ScheduleMessageService(final RocketMQBrokerController rocketBroker, RocketMQServiceConfiguration config) {
        this.rocketBroker = rocketBroker;
        this.pulsarBroker = rocketBroker.getBrokerService();
        this.config = config;
    }

   /* public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            this.parseDelayLevel();
            this.timer = new Timer("ScheduleMessageTimerThread", true);
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    this.timer.schedule(
                            new DeliverDelayedMessageTimerTask(
                                    level, offset), FIRST_DELAY_TIME);
                }
            }

            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            // TODO：  ScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer) {
                this.timer.cancel();
            }
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
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
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
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

        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(
                        new DeliverDelayedMessageTimerTask(
                                this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {
            long result = deliverTimestamp;
            long maxTimestamp = now + delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }
            return result;
        }

        public void executeOnTimeup() {
            ConsumeQueue cq =
                    this.defaultMessageStore
                            .findConsumeQueue(SCHEDULE_TOPIC,
                                    delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error(
                                            "[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                            tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog()
                                            .pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) {
                                MessageExt msgExt =
                                        this.defaultMessageStore
                                                .lookMessageByOffset(
                                                        offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        if (MixAll.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            log.error(
                                                    "[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                                    msgInner.getTopic(), msgInner);
                                            continue;
                                        }
                                        PutMessageResult putMessageResult =
                                                this.writeMessageStore
                                                        .putMessage(msgInner);

                                        if (putMessageResult != null
                                                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                    "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                    msgExt.getTopic(), msgExt.getMsgId());
                                            timer.schedule(
                                                    new DeliverDelayedMessageTimerTask(
                                                            this.delayLevel,
                                                            nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this
                                                    .updateOffset(this.delayLevel,
                                                            nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        *//*
                                         * XXX: warn and notify me
                                         *//*
                                        log.error(
                                                "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                        + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                        + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                ScheduleMessageService.this.timer.schedule(
                                        new DeliverDelayedMessageTimerTask(
                                                this.delayLevel, nextOffset),
                                        countdown);
                                ScheduleMessageService.this
                                        .updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(
                                new DeliverDelayedMessageTimerTask(
                                        this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this
                                .updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                                + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            ScheduleMessageService.this.timer.schedule(
                    new DeliverDelayedMessageTimerTask(
                            this.delayLevel,
                            failScheduleOffset), DELAY_FOR_A_WHILE);
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
    }*/
}
