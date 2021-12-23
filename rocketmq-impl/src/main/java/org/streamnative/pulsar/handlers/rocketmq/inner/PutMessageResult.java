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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Put message result.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PutMessageResult {

    private String msgId;
    private String offsetMsgId;
    private String pulsarMsgId;
    private Integer partition;
    private Long offset;

    private String msgKey;
    private String msgTag;

    public PutMessageResult(String magId, String pulsarMsgId, long offset, String msgKey, String msgTag) {
        this.msgId = magId;
        this.pulsarMsgId = pulsarMsgId;
        this.offset = offset;
        this.msgKey = msgKey;
        this.msgTag = msgTag;
    }

}
