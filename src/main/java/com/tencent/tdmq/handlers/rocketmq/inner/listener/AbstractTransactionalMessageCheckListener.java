package com.tencent.tdmq.handlers.rocketmq.inner.listener;

import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import io.netty.channel.Channel;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;

@Slf4j
public abstract class AbstractTransactionalMessageCheckListener {

    //queue nums of topic TRANS_CHECK_MAX_TIME_TOPIC
    protected final static int TCMT_QUEUE_NUMS = 1;
    private static ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Transaction-msg-check-thread");
            return thread;
        }
    }, new CallerRunsPolicy());
    protected final Random random = new Random(System.currentTimeMillis());
    private RocketMQBrokerController brokerController;

    public AbstractTransactionalMessageCheckListener() {
    }

    public AbstractTransactionalMessageCheckListener(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void sendCheckMessage(MessageExt msgExt) throws Exception {
        CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
        checkTransactionStateRequestHeader.setCommitLogOffset(msgExt.getCommitLogOffset());
        checkTransactionStateRequestHeader.setOffsetMsgId(msgExt.getMsgId());
        checkTransactionStateRequestHeader
                .setMsgId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        checkTransactionStateRequestHeader.setTransactionId(checkTransactionStateRequestHeader.getMsgId());
        checkTransactionStateRequestHeader.setTranStateTableOffset(msgExt.getQueueOffset());
        msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgExt.setStoreSize(0);
        String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        Channel channel = brokerController.getProducerManager().getAvaliableChannel(groupId);
        if (channel != null) {
            brokerController.getBroker2Client()
                    .checkProducerTransactionState(groupId, channel, checkTransactionStateRequestHeader, msgExt);
        } else {
            log.warn("Check transaction failed, channel is null. groupId={}", groupId);
        }
    }

    public void resolveHalfMsg(final MessageExt msgExt) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendCheckMessage(msgExt);
                } catch (Exception e) {
                    log.error("Send check message error!", e);
                }
            }
        });
    }

    public RocketMQBrokerController getBrokerController() {
        return brokerController;
    }

    /**
     * Inject brokerController for this listener
     *
     * @param brokerController
     */
    public void setBrokerController(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void shutDown() {
        executorService.shutdown();
    }

    /**
     * In order to avoid check back unlimited, we will discard the message that have been checked more than a certain
     * number of times.
     *
     * @param msgExt Message to be discarded.
     */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
