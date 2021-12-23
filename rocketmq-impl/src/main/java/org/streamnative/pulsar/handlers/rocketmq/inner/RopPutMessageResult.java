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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * Rop put message result.
 */
public class RopPutMessageResult extends org.apache.rocketmq.store.PutMessageResult {

    private final List<PutMessageResult> putMessageResults = new ArrayList<>();

    public RopPutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        super(putMessageStatus, appendMessageResult);
    }

    public void addPutMessageId(PutMessageResult putMessageResult) {
        putMessageResults.add(putMessageResult);
    }

    public void addPutMessageIds(List<PutMessageResult> putMessageResults) {
        this.putMessageResults.addAll(putMessageResults);
    }

    public List<PutMessageResult> getPutMessageResults() {
        return putMessageResults;
    }
}
