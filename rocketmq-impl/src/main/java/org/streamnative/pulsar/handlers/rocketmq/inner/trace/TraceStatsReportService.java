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

package org.streamnative.pulsar.handlers.rocketmq.inner.trace;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.streamnative.pulsar.handlers.rocketmq.inner.RocketMQBrokerController;
import org.testng.util.Strings;

/**
 * Trace stats report service.
 */
@Slf4j
public class TraceStatsReportService implements Runnable {

    private final AtomicLong writeDiskCounter = new AtomicLong(0);
    private final AtomicBoolean diskCanWrite = new AtomicBoolean(true);

    private ScheduledFuture<?> scheduledReportExecutor;
    private File logDir;

    public void boot() {
        scheduledReportExecutor = Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this, 30, 30, TimeUnit.SECONDS);
    }

    public void shutdown() {
        scheduledReportExecutor.cancel(true);
    }


    public void logWriteToDisk(long count) {
        writeDiskCounter.addAndGet(count);
    }

    public boolean canWriteDisk() {
        return this.diskCanWrite.get();
    }

    @Override
    public void run() {
        try {
            if (logDir == null && Strings.isNullOrEmpty(RocketMQBrokerController.ropTraceLogDir())) {
                logDir = new File(RocketMQBrokerController.ropTraceLogDir());
            }
            if (logDir != null) {
                int remainDiskPercentage = (int) ((((double) logDir.getFreeSpace()) / logDir.getTotalSpace()) * 100);
                log.info("RoP trace remaining disk usage:{}%", remainDiskPercentage);
                if (remainDiskPercentage < 10) {
                    diskCanWrite.set(false);
                    log.warn("RoP trace stop writing disk due to free space is low.");
                } else {
                    if (diskCanWrite.compareAndSet(false, true)) {
                        log.info("RoP trace restore writing.");
                    }
                }
            }
        } catch (Exception e) {
            log.info("RoP trace disk check error.", e);
        }
    }
}
