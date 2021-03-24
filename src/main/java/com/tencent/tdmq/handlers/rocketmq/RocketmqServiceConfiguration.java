package com.tencent.tdmq.handlers.rocketmq;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/3/24 1:20 下午
 */

/**
 * 这个类主要用来继承 pulsar broker 的 ServiceConfiguration 类，实现配置的注入
 */

@Getter
@Setter
public class RocketmqServiceConfiguration extends ServiceConfiguration {

    @Category
    private static final String CATEGORY_ROCKETMQ = "RocketMQ on Pulsar";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "Rocketmq on Pulsar Broker tenant"
    )
    private String rocketmqTenant = "public";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "The tenant used for storing Rocketmq metadata topics"
    )
    private String rocketmqMetadataTenant = "public";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "Rocketmq on Pulsar Broker namespace"
    )
    private String rocketmqNamespace = "default";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            required = true,
            doc = "The namespace used for storing rocket metadata topics"
    )
    private String rocketmqMetadataNamespace = "__rocketmq";

    @FieldContext(
            category = CATEGORY_ROCKETMQ,
            doc = "Comma-separated list of URIs we will listen on and the listener names.\n"
                    + "e.g. PLAINTEXT://localhost:9096.\n"
                    + "If hostname is not set, bind to the default interface."
    )
    private String rocketmqListeners = "rocketmq://127.0.0.1:9096";
}
