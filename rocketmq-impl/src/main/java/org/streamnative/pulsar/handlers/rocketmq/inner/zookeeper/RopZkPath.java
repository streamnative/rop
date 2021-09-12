package org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper;

public class RopZkPath {

    public static final String ropPath = "/rop";

    public static final String coordinatorPath = ropPath + "/coordinator";

    public static final String brokerPath = ropPath + "/brokers";

    public static final String topicBasePath = ropPath + "/topics";

    public static final String topicBasePathMatch = ropPath + "/topics/%s";
    public static final String tenantBasePathMatch = ropPath + "/topics/%s";
    public static final String namespacesBasePathMatch = ropPath + "/topics/%s";

    public static final String groupBasePath = ropPath + "/groups";

    public static final String groupBasePathMatch = ropPath + "/groups/%s";

}
