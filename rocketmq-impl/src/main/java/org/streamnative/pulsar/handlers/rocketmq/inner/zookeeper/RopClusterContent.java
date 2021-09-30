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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Rop broker cluster info.
 */
@Slf4j
@Data
@EqualsAndHashCode
@ToString
public class RopClusterContent {

    private String clusterName;
    private Map<String/*BrokerTag*/, List<String>/*ip:port*/> brokerCluster;

    public Map<String, List<Integer>> createTopicRouteMap(int partitionNum) {
        Preconditions
                .checkArgument(brokerCluster != null && brokerCluster.size() > 0, "Rop Cluster config haven't be set.");
        Preconditions.checkArgument(partitionNum > 0, "the num of top partition must be more than zero.");
        int brokerGroupNum = brokerCluster.entrySet().size();
        int size = partitionNum / brokerGroupNum;
        int res = partitionNum % brokerGroupNum;
        Map<String, Integer> assignedMap = new HashMap<>();
        for (String brokerTag : brokerCluster.keySet()) {
            assignedMap.put(brokerTag, size + ((res--) > 0 ? 1 : 0));
        }
        Map<String, List<Integer>> result = new HashMap<>(brokerGroupNum);

        int total = 0;
        List<Entry<String, Integer>> shuffledAssignedEntries = assignedMap.entrySet().stream()
                .collect(Collectors.toList());
        Collections.shuffle(shuffledAssignedEntries);
        for (Entry<String, Integer> entry : shuffledAssignedEntries) {
            for (int i = 0; i < entry.getValue(); i++) {
                List<Integer> tempList = result.computeIfAbsent(entry.getKey(), k -> new ArrayList(entry.getValue()));
                tempList.add(i + total);
            }
            total += entry.getValue();
        }
        return result;
    }
}
