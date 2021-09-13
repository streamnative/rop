package org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper;

public class RopZkPath {

    public static final String ropPath = "/rop";

    public static final String coordinatorPath = ropPath + "/coordinator";

    public static final String brokerPath = ropPath + "/brokers";

    public static final String topicBasePath = ropPath + "/topics";

    public static final String topicBasePathMatch = topicBasePath + "/%s";

    public static final String groupBasePath = ropPath + "/groups";

    public static final String groupBasePathMatch = groupBasePath + "/%s";

}
