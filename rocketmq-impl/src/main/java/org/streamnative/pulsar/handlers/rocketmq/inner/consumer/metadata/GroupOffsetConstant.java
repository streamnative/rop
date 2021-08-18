/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streamnative.pulsar.handlers.rocketmq.inner.consumer.metadata;

public final class GroupOffsetConstant {
    //group meta fields index
    public static final int GROUP_META_VERSION_POS = 0;
    public static final int GROUP_META_KEY_TYPE_POS = GROUP_META_VERSION_POS + 2;
    public static final int GROUP_META_KEY_GRP_NAME_LEN_POS = GROUP_META_KEY_TYPE_POS + 4;
    public static final int GROUP_META_KEY_TOTAL_HEAD_LEN = GROUP_META_KEY_GRP_NAME_LEN_POS + 4;

    //group offset key fields index
    public static final int GROUP_OFFSET_KEY_PARTITION_POS = 0;
    public static final int GROUP_OFFSET_KEY_TOPIC_NAME_LEN_POS = GROUP_OFFSET_KEY_PARTITION_POS + 4;
    public static final int GROUP_OFFSET_KEY_TOTAL_HEAD_LEN = GROUP_OFFSET_KEY_TOPIC_NAME_LEN_POS + 4;

    //group offset value fields index
    public static final int GROUP_OFFSET_VALUE_VERSION_POS = 0;
    public static final int GROUP_OFFSET_VALUE_OFFSET_POS = GROUP_OFFSET_VALUE_VERSION_POS + 2;
    public static final int GROUP_OFFSET_VALUE_COMMIT_TIMESTAMP_POS = GROUP_OFFSET_VALUE_OFFSET_POS + 8;
    public static final int GROUP_OFFSET_VALUE_EXPIRE_TIMESTAMP_POS = GROUP_OFFSET_VALUE_COMMIT_TIMESTAMP_POS + 8;
    public static final int GROUP_OFFSET_VALUE_TOTAL_LEN = GROUP_OFFSET_VALUE_EXPIRE_TIMESTAMP_POS + 8;

    //subscription value fields index
    public static final int GROUP_SUBSCRIPTION_FORMAT_VERSION_POS = 0;
    public static final int GROUP_SUBSCRIPTION_KEY_TAG_POS = GROUP_SUBSCRIPTION_FORMAT_VERSION_POS + 2;
    public static final int GROUP_SUBSCRIPTION_KEY_RETRY_QUEUE_NUM_POS = GROUP_SUBSCRIPTION_KEY_TAG_POS + 2;
    public static final int GROUP_SUBSCRIPTION_KEY_RETRY_MAX_TIMES_POS = GROUP_SUBSCRIPTION_KEY_RETRY_QUEUE_NUM_POS + 4;
    public static final int GROUP_SUBSCRIPTION_KEY_BROKER_ID_POS = GROUP_SUBSCRIPTION_KEY_RETRY_MAX_TIMES_POS + 4;
    public static final int GROUP_SUBSCRIPTION_KEY_BROKER_SELECTED_POS = GROUP_SUBSCRIPTION_KEY_BROKER_ID_POS + 8;
    public static final int GROUP_SUBSCRIPTION_KEY_GRP_NAME_LEN_POS = GROUP_SUBSCRIPTION_KEY_BROKER_SELECTED_POS + 8;
    public static final int GROUP_SUBSCRIPTION_KEY_TOTAL_HEAD_LEN = GROUP_SUBSCRIPTION_KEY_GRP_NAME_LEN_POS + 4;

}
