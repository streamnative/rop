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

package com.tencent.tdmq.handlers.rocketmq.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;

/**
 * Pulsar utils class.
 */
public class PulsarUtil {

    public static InitialPosition parseSubPosition(SubscriptionInitialPosition subPosition) {
        switch (subPosition) {
            case Latest:
                return InitialPosition.Latest;
            case Earliest:
                return InitialPosition.Earliest;
            default:
                return InitialPosition.Latest;
        }
    }

    public static SubType parseSubType(SubscriptionType subType) {
        switch (subType) {
            case Exclusive:
                return SubType.Exclusive;
            case Shared:
                return SubType.Shared;
            case Failover:
                return SubType.Failover;
            case Key_Shared:
                return SubType.Key_Shared;
            default:
                return SubType.Exclusive;
        }
    }

    public static List<KeyValue> convertFromStringMap(Map<String, String> stringMap) {
        List<KeyValue> keyValueList = new ArrayList<>();
        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            KeyValue build = KeyValue.newBuilder()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue())
                    .build();

            keyValueList.add(build);
        }

        return keyValueList;
    }


}
