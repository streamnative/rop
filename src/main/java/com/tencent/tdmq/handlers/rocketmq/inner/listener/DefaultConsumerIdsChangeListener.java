package com.tencent.tdmq.handlers.rocketmq.inner.listener;

import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import io.netty.channel.Channel;
import java.util.List;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;

public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {

    private final RocketMQBrokerController brokerController;

    public DefaultConsumerIdsChangeListener(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                if (args == null || args.length < 1) {
                    return;
                }
                List<Channel> channels = (List<Channel>) args[0];
                if (channels != null && brokerController.getServerConfig().isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channels) {
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                    }
                }
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }
}

