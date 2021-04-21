package com.tencent.tdmq.handlers.rocketmq.inner.producer;

import com.tencent.tdmq.handlers.rocketmq.utils.CommonUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString
public class ClientGroupName {
    private final String rmqGroupName;
    private final String pulsarGroupName;

    public ClientGroupName(String rmqGroupName) {
        this.rmqGroupName = rmqGroupName;
        this.pulsarGroupName = CommonUtils.tdmqGroupName(this.rmqGroupName);
    }
}
