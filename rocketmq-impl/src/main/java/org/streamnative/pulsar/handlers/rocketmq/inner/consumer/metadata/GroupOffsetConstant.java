package org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata;

public final class GroupOffsetConstant {
    public static final int GROUP_OFFSET_FORMAT_VERSION_POS = 0;
    public static final int GROUP_OFFSET_VALUE_OFFSET_POS = 2;
    public static final int GROUP_OFFSET_VALUE_COMMIT_TIMESTAMP_POS = GROUP_OFFSET_VALUE_OFFSET_POS + 8;
    public static final int GROUP_OFFSET_VALUE_EXPIRE_TIMESTAMP_POS = GROUP_OFFSET_VALUE_COMMIT_TIMESTAMP_POS + 8;

    public static final int GROUP_OFFSET_KEY_TAG_POS = 2;
    public static final int GROUP_OFFSET_KEY_PARTITION_POS = GROUP_OFFSET_KEY_TAG_POS + 2;
    public static final int GROUP_OFFSET_KEY_RETRY_QUEUE_NUM_POS = GROUP_OFFSET_KEY_PARTITION_POS + 4;
    public static final int GROUP_OFFSET_KEY_RETRY_MAX_TIMES_POS = GROUP_OFFSET_KEY_RETRY_QUEUE_NUM_POS + 4;
    public static final int GROUP_OFFSET_KEY_BROKER_ID_POS = GROUP_OFFSET_KEY_RETRY_MAX_TIMES_POS + 4;
    public static final int GROUP_OFFSET_KEY_BROKER_SELECTED_POS = GROUP_OFFSET_KEY_BROKER_ID_POS + 8;
    public static final int GROUP_OFFSET_KEY_GRP_NAME_LEN_POS = GROUP_OFFSET_KEY_BROKER_SELECTED_POS + 8;
    public static final int GROUP_OFFSET_KEY_TOPIC_NAME_LEN_POS = GROUP_OFFSET_KEY_GRP_NAME_LEN_POS + 4;
    public static final int GROUP_OFFSET_KEY_TOTAL_HEAD_LEN = GROUP_OFFSET_KEY_TOPIC_NAME_LEN_POS + 4;

}
