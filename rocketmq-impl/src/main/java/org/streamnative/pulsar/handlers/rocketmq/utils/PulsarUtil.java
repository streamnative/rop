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

package org.streamnative.pulsar.handlers.rocketmq.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;

/**
 * Pulsar utils class.
 */
public class PulsarUtil {

    public static final Pattern BROKER_ADDER_PAT = Pattern.compile("([^/:]+:)(\\d+)");

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

    public static String getBrokerHost(String brokerAddress) {
        // eg: pulsar://127.0.0.1:6650
        if (null == brokerAddress) {
            return Strings.EMPTY;
        }
        Matcher matcher = BROKER_ADDER_PAT.matcher(brokerAddress);
        if (matcher.find()) {
            return matcher.group(1).replaceAll(":", Strings.EMPTY);
        }
        return brokerAddress;
    }
}
