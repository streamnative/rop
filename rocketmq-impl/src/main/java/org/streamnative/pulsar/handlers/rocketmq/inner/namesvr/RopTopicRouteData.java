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

package org.streamnative.pulsar.handlers.rocketmq.inner.namesvr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

/**
 * Rop topic route data.
 */
public class RopTopicRouteData extends TopicRouteData {

    Map<Integer, String> partitionRouteInfos;

    public Map<Integer, String> getPartitionRouteInfos() {
        return partitionRouteInfos;
    }

    public void setPartitionRouteInfos(Map<Integer, String> partitionRouteInfos) {
        this.partitionRouteInfos = partitionRouteInfos;
    }

    @Override
    public TopicRouteData cloneTopicRouteData() {
        RopTopicRouteData topicRouteData = new RopTopicRouteData();
        topicRouteData.setQueueDatas(new ArrayList());
        topicRouteData.setBrokerDatas(new ArrayList());
        topicRouteData.setFilterServerTable(new HashMap());
        topicRouteData.setOrderTopicConf(this.getOrderTopicConf());
        if (this.getQueueDatas() != null) {
            topicRouteData.getQueueDatas().addAll(this.getQueueDatas());
        }
        if (this.getBrokerDatas() != null) {
            topicRouteData.getBrokerDatas().addAll(this.getBrokerDatas());
        }
        if (this.getFilterServerTable() != null) {
            topicRouteData.getFilterServerTable().putAll(this.getFilterServerTable());
        }
        if (this.getPartitionRouteInfos() != null) {
            topicRouteData.getPartitionRouteInfos().putAll(this.getPartitionRouteInfos());
        }
        return topicRouteData;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (this.partitionRouteInfos == null ? 0 : this.partitionRouteInfos.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEquals = super.equals(obj);
        if (isEquals) {
            RopTopicRouteData other = (RopTopicRouteData) obj;
            if (this.partitionRouteInfos == null) {
                if (other.partitionRouteInfos != null) {
                    isEquals = false;
                }
            } else if (!this.partitionRouteInfos.equals(other.partitionRouteInfos)) {
                isEquals = false;
            }
        }
        return isEquals;
    }

    @Override
    public String toString() {
        return super.toString() + ", [partitionRouteInfos=" + this.partitionRouteInfos + "]";
    }
}
