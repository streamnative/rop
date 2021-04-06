package com.tencent.tdmq.handlers.rocketmq.inner;

import com.tencent.tdmq.handlers.rocketmq.inner.listener.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;

public interface TransactionalMessageService {

    /**
     * Process prepare message, in common, we should put this message to storage service.
     *
     * @param messageInner Prepare(Half) message.
     * @return Prepare message storage result.
     */
    PutMessageResult prepareMessage(MessageExtBrokerInner messageInner);

    /**
     * Delete prepare message when this message has been committed or rolled back.
     *
     * @param messageExt
     */
    boolean deletePrepareMessage(MessageExt messageExt);

    /**
     * Invoked to process commit prepare message.
     *
     * @param requestHeader Commit message request header.
     * @return Operate result contains prepare message and relative error code.
     */
    OperationResult commitMessage(EndTransactionRequestHeader requestHeader);

    /**
     * Invoked to roll back prepare message.
     *
     * @param requestHeader Prepare message request header.
     * @return Operate result contains prepare message and relative error code.
     */
    OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader);

    /**
     * Traverse uncommitted/unroll back half message and send check back request to producer to obtain transaction
     * status.
     *
     * @param transactionTimeout The minimum time of the transactional message to be checked firstly, one
     *         message only
     *         exceed this time interval that can be checked.
     * @param transactionCheckMax The maximum number of times the message was checked, if exceed this value,
     *         this
     *         message will be discarded.
     * @param listener When the message is considered to be checked or discarded, the relative method of this
     *         class will
     *         be invoked.
     */
    void check(long transactionTimeout, int transactionCheckMax, AbstractTransactionalMessageCheckListener listener);

    /**
     * Open transaction service.
     *
     * @return If open success, return true.
     */
    boolean open();

    /**
     * Close transaction service.
     */
    void close();
}
