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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * Rop Cluster Content Test.
 */
@Slf4j
public class RopClusterContentTest {

    public static final String CLUSTER_NAME = "cluster1";

    @Test
    public void testEquals() {
        RopClusterContent clusterContent1 = new RopClusterContent();
        clusterContent1.setClusterName(CLUSTER_NAME);
        Map<String, List<String>> groupBrokers1 = new HashMap<>();
        clusterContent1.setBrokerCluster(groupBrokers1);
        List<String> brokers = groupBrokers1.compute("broker-0", (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = new ArrayList<>();
            }
            return oldValue;
        });
        brokers.add("1.1.1.1:9876");
        brokers.add("1.1.1.2:9876");

        RopClusterContent clusterContent2 = new RopClusterContent();
        clusterContent2.setClusterName(CLUSTER_NAME);
        Map<String, List<String>> groupBrokers2 = new HashMap<>();
        clusterContent2.setBrokerCluster(groupBrokers2);
        List<String> brokers2 = groupBrokers2.compute("broker-0", (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = new ArrayList<>();
            }
            return oldValue;
        });
        brokers2.add("1.1.1.1:9876");
        brokers2.add("1.1.1.2:9876");

        assertEquals(clusterContent1, clusterContent2);
    }

    @Test
    public void testEquals2() {
        RopClusterContent clusterContent1 = new RopClusterContent();
        clusterContent1.setClusterName(CLUSTER_NAME);
        Map<String, List<String>> groupBrokers1 = new HashMap<>();
        clusterContent1.setBrokerCluster(groupBrokers1);
        List<String> brokers = groupBrokers1.compute("broker-0", (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = new ArrayList<>();
            }
            return oldValue;
        });
        brokers.add("1.1.1.3:9876");
        brokers.add("1.1.1.2:9876");
        brokers.add("1.1.1.1:9876");

        RopClusterContent clusterContent2 = new RopClusterContent();
        clusterContent2.setClusterName(CLUSTER_NAME);
        Map<String, List<String>> groupBrokers2 = new HashMap<>();
        clusterContent2.setBrokerCluster(groupBrokers2);
        List<String> brokers2 = groupBrokers2.compute("broker-0", (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = new ArrayList<>();
            }
            return oldValue;
        });
        brokers2.add("1.1.1.3:9876");
        brokers2.add("1.1.1.2:9876");
        brokers2.add("1.1.1.1:9876");

        assertEquals(clusterContent1.getClusterName(), clusterContent2.getClusterName());
        assertEquals(clusterContent1.getBrokerCluster().size(), clusterContent2.getBrokerCluster().size());
        assertEquals(clusterContent1, clusterContent2);
    }

    @Test
    public void testNotEquals1() {
        RopClusterContent clusterContent1 = new RopClusterContent();
        clusterContent1.setClusterName(CLUSTER_NAME + "1");
        Map<String, List<String>> groupBrokers1 = new HashMap<>();
        clusterContent1.setBrokerCluster(groupBrokers1);
        List<String> brokers = groupBrokers1.compute("broker-0", (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = new ArrayList<>();
            }
            return oldValue;
        });
        brokers.add("1.1.1.1:9876");
        brokers.add("1.1.1.2:9876");
        brokers.add("1.1.1.1:9876");

        RopClusterContent clusterContent2 = new RopClusterContent();
        clusterContent2.setClusterName(CLUSTER_NAME + "2");
        Map<String, List<String>> groupBrokers2 = new HashMap<>();
        clusterContent2.setBrokerCluster(groupBrokers2);
        List<String> brokers2 = groupBrokers2.compute("broker-0", (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = new ArrayList<>();
            }
            return oldValue;
        });
        brokers2.add("1.1.1.2:9876");
        brokers2.add("1.1.1.1:9876");
        assertNotEquals(clusterContent1, clusterContent2);
    }

    @Test
    public void testJson() throws JsonProcessingException {
        RopClusterContent clusterContent2 = new RopClusterContent();
        clusterContent2.setClusterName(CLUSTER_NAME + "2");
        Map<String, List<String>> groupBrokers2 = new HashMap<>();
        clusterContent2.setBrokerCluster(groupBrokers2);
        List<String> brokers2 = groupBrokers2.compute("broker-0", (k, oldValue) -> {
            if (oldValue == null) {
                oldValue = new ArrayList<>();
            }
            return oldValue;
        });
        brokers2.add("1.1.1.2:9876");
        brokers2.add("1.1.1.1:9876");

        ObjectMapper test = new ObjectMapper();
        String s = test.writeValueAsString(clusterContent2);

        log.info(s);
        RopClusterContent clusterContent3 = test.readValue(s, RopClusterContent.class);
        assertEquals(clusterContent2, clusterContent3);
    }
}