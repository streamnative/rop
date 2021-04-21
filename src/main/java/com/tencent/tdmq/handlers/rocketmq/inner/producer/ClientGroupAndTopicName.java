package com.tencent.tdmq.handlers.rocketmq.inner.producer;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString
public class ClientGroupAndTopicName {
    private final ClientGroupName clientGroupName;
    private final ClientTopicName clientTopicName;

    public ClientGroupAndTopicName(String rmqGroupName,
            String rmqTopicName) {
        this.clientGroupName = new ClientGroupName(rmqGroupName);
        this.clientTopicName = new ClientTopicName(rmqTopicName);
    }
}
