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

/**
 * Rop path for zookeeper.
 */
public class RopZkPath {

    public static final String ROP_PATH = "/rop";

    public static final String COORDINATOR_PATH = ROP_PATH + "/coordinator";

    public static final String BROKER_PATH = ROP_PATH + "/brokers";

    public static final String TOPIC_BASE_PATH = ROP_PATH + "/topics";

    public static final String TOPIC_BASE_PATH_MATCH = TOPIC_BASE_PATH + "/%s";

    public static final String GROUP_BASE_PATH = ROP_PATH + "/groups";

    public static final String GROUP_BASE_PATH_MATCH = GROUP_BASE_PATH + "/%s";

}
