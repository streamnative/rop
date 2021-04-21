/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tdmq.handlers.rocketmq.inner;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.ServiceThread;

@Slf4j
public class TransactionalMessageCheckService extends ServiceThread {

    private RocketMQBrokerController brokerController;

    public TransactionalMessageCheckService(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public String getServiceName() {
        return TransactionalMessageCheckService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("Start transaction check service thread!");
        long checkInterval = brokerController.getServerConfig().getTransactionCheckInterval();
        while (!this.isStopped()) {
            this.waitForRunning(checkInterval);
        }
        log.info("End transaction check service thread!");
    }

    @Override
    protected void onWaitEnd() {
        long timeout = brokerController.getServerConfig().getTransactionTimeOut();
        int checkMax = brokerController.getServerConfig().getTransactionCheckMax();
        long begin = System.currentTimeMillis();
        log.info("Begin to check prepare message, begin time:{}", begin);
        this.brokerController.getTransactionalMessageService()
                .check(timeout, checkMax, this.brokerController.getTransactionalMessageCheckListener());
        log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
    }

}

