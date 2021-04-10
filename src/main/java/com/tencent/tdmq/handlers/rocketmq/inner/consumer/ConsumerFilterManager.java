package com.tencent.tdmq.handlers.rocketmq.inner.consumer;

import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager.FilterDataMapByTopic;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/4/9 3:41 下午
 */
@Slf4j
public class ConsumerFilterManager {

    private static final long MS_24_HOUR = 24 * 3600 * 1000;
    private transient RocketMQBrokerController brokerController;

    private ConcurrentMap<String/*Topic*/, FilterDataMapByTopic>
            filterDataByTopic = new ConcurrentHashMap<String/*Topic*/, FilterDataMapByTopic>(
            256);

    public ConsumerFilterManager(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        // then set bit map length of store config.
//        brokerController.getMessageStoreConfig().setBitMapLengthConsumeQueueExt(
//                this.bloomFilter.getM()
//        );
    }

    public void register(final String consumerGroup, final Collection<SubscriptionData> subList) {
        for (SubscriptionData subscriptionData : subList) {
            register(
                    subscriptionData.getTopic(),
                    consumerGroup,
                    subscriptionData.getSubString(),
                    subscriptionData.getExpressionType(),
                    subscriptionData.getSubVersion()
            );
        }

        // make illegal topic dead.
        Collection<ConsumerFilterData> groupFilterData = getByGroup(consumerGroup);

        Iterator<ConsumerFilterData> iterator = groupFilterData.iterator();
        while (iterator.hasNext()) {
            ConsumerFilterData filterData = iterator.next();

            boolean exist = false;
            for (SubscriptionData subscriptionData : subList) {
                if (subscriptionData.getTopic().equals(filterData.getTopic())) {
                    exist = true;
                    break;
                }
            }

            if (!exist && !filterData.isDead()) {
                filterData.setDeadTime(System.currentTimeMillis());
                log.info("Consumer filter changed: {}, make illegal topic dead:{}", consumerGroup, filterData);
            }
        }
    }

    public boolean register(final String topic, final String consumerGroup, final String expression,
            final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return false;
        }

        if (expression == null || expression.length() == 0) {
            return false;
        }

        FilterDataMapByTopic filterDataMapByTopic = this.filterDataByTopic
                .get(topic);

        if (filterDataMapByTopic == null) {
            FilterDataMapByTopic temp = new FilterDataMapByTopic(topic);
            FilterDataMapByTopic prev = this.filterDataByTopic.putIfAbsent(topic, temp);
            filterDataMapByTopic = prev != null ? prev : temp;
        }

        return filterDataMapByTopic.register(consumerGroup, expression, type, null, clientVersion);
    }

    public void unRegister(final String consumerGroup) {
        for (String topic : filterDataByTopic.keySet()) {
            this.filterDataByTopic.get(topic).unRegister(consumerGroup);
        }
    }

    public Collection<ConsumerFilterData> getByGroup(final String consumerGroup) {
        Collection<ConsumerFilterData> ret = new HashSet<ConsumerFilterData>();

        Iterator<FilterDataMapByTopic> topicIterator = this.filterDataByTopic
                .values().iterator();
        while (topicIterator.hasNext()) {
            FilterDataMapByTopic filterDataMapByTopic = topicIterator
                    .next();

            Iterator<ConsumerFilterData> filterDataIterator = filterDataMapByTopic.getGroupFilterData().values()
                    .iterator();

            while (filterDataIterator.hasNext()) {
                ConsumerFilterData filterData = filterDataIterator.next();

                if (filterData.getConsumerGroup().equals(consumerGroup)) {
                    ret.add(filterData);
                }
            }
        }

        return ret;
    }
}
