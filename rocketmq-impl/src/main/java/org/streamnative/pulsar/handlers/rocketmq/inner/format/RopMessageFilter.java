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

package org.streamnative.pulsar.handlers.rocketmq.inner.format;

import java.nio.ByteBuffer;
import java.util.function.Predicate;
import org.apache.pulsar.client.api.Message;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * Rop message filter.
 */
public class RopMessageFilter implements Predicate<Message> {

    protected final SubscriptionData subscriptionData;

    public RopMessageFilter(SubscriptionData subscriptionData) {
        this.subscriptionData = subscriptionData;
    }

    @Override
    public boolean test(Message message) {
        if (this.subscriptionData != null && message != null
                && ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }

            byte[] body = message.getData();
            Long tagsCode = null;
            if (body != null && body.length >= 8) {
                tagsCode = ByteBuffer.wrap(body).getLong();
            }
            return (tagsCode != null) && subscriptionData.getCodeSet().contains(tagsCode.intValue());
        }
        return true;
    }
}
