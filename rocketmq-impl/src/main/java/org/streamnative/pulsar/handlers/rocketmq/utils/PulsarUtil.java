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

package org.streamnative.pulsar.handlers.rocketmq.utils;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQServiceConfiguration;

/**
 * Pulsar utils class.
 */
@Slf4j
public class PulsarUtil {

    public static final int MAX_COMPACTION_THRESHOLD = 100 * 1024 * 1024;
    public static final Pattern BROKER_ADDER_PAT = Pattern.compile("([^/:]+):(\\d+)");

    public static InitialPosition parseSubPosition(SubscriptionInitialPosition subPosition) {
        switch (subPosition) {
            case Earliest:
                return InitialPosition.Earliest;
            case Latest:
            default:
                return InitialPosition.Latest;
        }
    }

    public static SubType parseSubType(SubscriptionType subType) {
        switch (subType) {
            case Shared:
                return SubType.Shared;
            case Failover:
                return SubType.Failover;
            case Key_Shared:
                return SubType.Key_Shared;
            case Exclusive:
            default:
                return SubType.Exclusive;
        }
    }

    public static List<KeyValue> convertFromStringMap(Map<String, String> stringMap) {
        List<KeyValue> keyValueList = new ArrayList<>();
        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            KeyValue build = KeyValue.newBuilder()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue())
                    .build();

            keyValueList.add(build);
        }

        return keyValueList;
    }

    public static String getBrokerHost(String brokerAddress) {
        // eg: pulsar://127.0.0.1:6650
        if (Strings.isBlank(brokerAddress)) {
            return Strings.EMPTY;
        }
        Matcher matcher = BROKER_ADDER_PAT.matcher(brokerAddress);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return brokerAddress;
    }

    public static void createOffsetMetadataIfMissing(PulsarAdmin pulsarAdmin,
            ClusterData clusterData,
            RocketMQServiceConfiguration conf)
            throws PulsarAdminException {
        createRopMetadataIfMissing(pulsarAdmin, clusterData, RocketMQTopic.getGroupMetaTopic(), conf,
                conf.getOffsetsTopicNumPartitions());
    }

    /**
     * This method creates the Rop metadata tenant and namespace if they are not currently present.
     * <ul>
     * <li>If the cluster does not exist this method will throw a PulsarServerException.NotFoundException</li>
     * <li>If the tenant does not exist it will be created</li>
     * <li>If the tenant exists but the allowedClusters list does not include the cluster this method will
     * add the cluster to the allowedClusters list</li>
     * <li>If the namespace does not exist it will be created</li>
     * <li>If the namespace exists but the replicationClusters list does not include the cluster this method
     * will add the cluster to the replicationClusters list</li>
     * <li>If the offset topic does not exist it will be created</li>
     * <li>If the offset topic exists but some partitions are missing, the missing partitions will be created</li>
     * </ul>
     */
    private static void createRopMetadataIfMissing(PulsarAdmin pulsarAdmin,
            ClusterData clusterData,
            RocketMQTopic groupMetaTopic,
            RocketMQServiceConfiguration conf,
            int partitionNum)
            throws PulsarAdminException {
        String cluster = conf.getClusterName();
        String ropMetadataTenant = RocketMQTopic.getMetaTenant();
        String ropMetadataNamespace = RocketMQTopic.getMetaTenant() + "/" + RocketMQTopic.getMetaNamespace();

        boolean clusterExists = false;
        boolean tenantExists = false;
        boolean namespaceExists = false;
        boolean offsetsTopicExists = false;

        try {
            Clusters clusters = pulsarAdmin.clusters();
            if (!clusters.getClusters().contains(cluster)) {
                try {
                    pulsarAdmin.clusters().createCluster(cluster, clusterData);
                } catch (PulsarAdminException e) {
                    if (e instanceof ConflictException) {
                        log.info("Attempted to create cluster {} however it was created concurrently.", cluster);
                    } else {
                        // Re-throw all other exceptions
                        throw e;
                    }
                }
            } else {
                ClusterData configuredClusterData = clusters.getCluster(cluster);
                log.info("Cluster {} found: {}", cluster, configuredClusterData);
            }
            clusterExists = true;

            // Check if the metadata tenant exists and create it if not
            Tenants tenants = pulsarAdmin.tenants();
            if (!tenants.getTenants().contains(ropMetadataTenant)) {
                log.info("Tenant: {} does not exist, creating it ...", ropMetadataTenant);
                tenants.createTenant(ropMetadataTenant,
                        new TenantInfo(conf.getSuperUserRoles(), Collections.singleton(cluster)));
            } else {
                TenantInfo ropMetadataTenantInfo = tenants.getTenantInfo(ropMetadataTenant);
                Set<String> allowedClusters = ropMetadataTenantInfo.getAllowedClusters();
                if (!allowedClusters.contains(cluster)) {
                    log.info("Tenant: {} exists but cluster: {} is not in the allowedClusters list, updating it ...",
                            ropMetadataTenant, cluster);
                    allowedClusters.add(cluster);
                    tenants.updateTenant(ropMetadataTenant, ropMetadataTenantInfo);
                }
            }
            tenantExists = true;

            // Check if the metadata namespace exists and create it if not
            Namespaces namespaces = pulsarAdmin.namespaces();
            if (!namespaces.getNamespaces(ropMetadataTenant).contains(ropMetadataNamespace)) {
                log.info("Namespaces: {} does not exist in tenant: {}, creating it ...",
                        ropMetadataNamespace, ropMetadataTenant);
                Set<String> replicationClusters = Sets.newHashSet(cluster);
                namespaces.createNamespace(ropMetadataNamespace, replicationClusters);
                namespaces.setNamespaceReplicationClusters(ropMetadataNamespace, replicationClusters);
            } else {
                List<String> replicationClusters = namespaces.getNamespaceReplicationClusters(ropMetadataNamespace);
                if (!replicationClusters.contains(cluster)) {
                    log.info("Namespace: {} exists but cluster: {} is not in the replicationClusters list,"
                            + "updating it ...", ropMetadataNamespace, cluster);
                    Set<String> newReplicationClusters = Sets.newHashSet(replicationClusters);
                    newReplicationClusters.add(cluster);
                    namespaces.setNamespaceReplicationClusters(ropMetadataNamespace, newReplicationClusters);
                }
            }
            // set namespace config if namespace existed
            int retentionMinutes = (int) conf.getOffsetsRetentionMinutes();
            RetentionPolicies retentionPolicies = namespaces.getRetention(ropMetadataNamespace);
            if (retentionPolicies == null || retentionPolicies.getRetentionTimeInMinutes() != retentionMinutes) {
                namespaces.setRetention(ropMetadataNamespace,
                        new RetentionPolicies((int) conf.getOffsetsRetentionMinutes(), -1));
            }

            Long compactionThreshold = namespaces.getCompactionThreshold(ropMetadataNamespace);
            if (compactionThreshold != null && compactionThreshold != MAX_COMPACTION_THRESHOLD) {
                namespaces.setCompactionThreshold(ropMetadataNamespace, MAX_COMPACTION_THRESHOLD);
            }

            int targetMessageTTL = conf.getOffsetsMessageTTL();
            Integer messageTTL = namespaces.getNamespaceMessageTTL(ropMetadataNamespace);
            if (messageTTL == null || messageTTL != targetMessageTTL) {
                namespaces.setNamespaceMessageTTL(ropMetadataNamespace, targetMessageTTL);
            }

            namespaceExists = true;

            // Check if the offsets topic exists and create it if not
            createTopicIfNotExist(pulsarAdmin, groupMetaTopic.getPulsarFullName(), partitionNum);
            offsetsTopicExists = true;
        } catch (PulsarAdminException e) {
            if (e instanceof ConflictException) {
                log.info("Resources concurrent creating and cause e: ", e);
                return;
            }

            log.error("Failed to successfully initialize Rop Metadata {}",
                    ropMetadataNamespace, e);
            throw e;
        } finally {
            log.info("Current state of Rop metadata, cluster: {} exists: {}, tenant: {} exists: {},"
                            + " namespace: {} exists: {}, topic: {} exists: {}",
                    cluster, clusterExists, ropMetadataTenant, tenantExists, ropMetadataNamespace, namespaceExists,
                    groupMetaTopic.getPulsarFullName(), offsetsTopicExists);
        }
    }

    private static void createTopicIfNotExist(final PulsarAdmin admin,
            final String topic,
            final int numPartitions) throws PulsarAdminException {
        try {
            admin.topics().createPartitionedTopic(topic, numPartitions);
        } catch (PulsarAdminException.ConflictException e) {
            log.info("Resources concurrent creating for topic : {}, caused by : {}", topic, e.getMessage());
        }
        try {
            // Ensure all partitions are created
            admin.topics().createMissedPartitions(topic);
        } catch (PulsarAdminException ignored) {
        }
    }

    public static void createSubscriptionIfNotExist(final PulsarAdmin admin,
            final String topic,
            final String subscriptionName,
            final MessageId messageId) {
        try {
            if(!admin.topics().getSubscriptions(topic).contains(subscriptionName)) {
                MessageId startPos = messageId == null ? MessageId.earliest : messageId;
                admin.topics().createSubscription(topic, subscriptionName, startPos);
            }
        } catch (PulsarAdminException e) {
            log.info("Resources creating for subscription:{} of topic : {}, caused by : {}", subscriptionName, topic,
                    e.getMessage());
        }
    }
}
