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
package org.streamnative.pulsar.handlers.rocketmq.inner.bean;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * Rop consume stats.
 */
public class RopConsumeStats extends RemotingSerializable {
    private HashMap<MessageQueue, RopOffsetWrapper> offsetTable = new HashMap<MessageQueue, RopOffsetWrapper>();
    private double consumeTps = 0;

    public long computeTotalDiff() {
        long diffTotal = 0L;

        Iterator<Entry<MessageQueue, RopOffsetWrapper>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, RopOffsetWrapper> next = it.next();
            long diff = next.getValue().getBrokerOffset() - next.getValue().getConsumerOffset();
            diffTotal += diff;
        }

        return diffTotal;
    }

    public HashMap<MessageQueue, RopOffsetWrapper> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(HashMap<MessageQueue, RopOffsetWrapper> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public double getConsumeTps() {
        return consumeTps;
    }

    public void setConsumeTps(double consumeTps) {
        this.consumeTps = consumeTps;
    }
}
