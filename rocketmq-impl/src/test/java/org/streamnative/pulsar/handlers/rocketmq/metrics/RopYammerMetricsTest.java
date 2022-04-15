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

package org.streamnative.pulsar.handlers.rocketmq.metrics;

import junit.framework.TestCase;
import org.junit.Assert;

/**
 * Validate RopYammerMetrics.
 */
public class RopYammerMetricsTest extends TestCase {

    public void testGetTopicFromScope() {
        String scope = "{cluster=\"ropCluster\",topic=\"rocketmq-xxx/ns-xxx/topic-xxx-partition-0\","
                + "group=\"rocketmq-xxx/ns-xxx/group-xxx\"}";
        Assert.assertEquals("rocketmq-xxx/ns-xxx/topic-xxx-partition-0", RopYammerMetrics.getTopicFromScope(scope));
    }
}