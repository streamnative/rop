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
 * @author xiaolongran@tencent.com
 * @date 2021/4/14 3:06 下午
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

    public static List<KeyValue> ConvertFromStringMap(Map<String, String> stringMap) {
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
