package com.tencent.tdmq.handlers.rocketmq.inner.producer;

import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString
public class ClientTopicName {
    private final String rmqTopicName;
    private final String pulsarTopicName;

    public ClientTopicName(String rmqTopicName) {
        this.rmqTopicName = rmqTopicName;
        this.pulsarTopicName = CommonUtils.tdmqGroupName(this.rmqTopicName);
    }
}
