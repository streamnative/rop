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
