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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.streamnative.pulsar.handlers.rocketmq.RocketMQServiceConfiguration;
import org.streamnative.pulsar.handlers.rocketmq.inner.zookeeper.RopClusterContent;

/**
 * Pulsar utils class.
 */
@Slf4j
public class PulsarUtil {

    public static final int MAX_COMPACTION_THRESHOLD = 100 * 1024 * 1024;
    public static final Pattern BROKER_ADDER_PAT = Pattern.compile("([^/:]+):(\\d+)");
    private static final String BROKER_TAG_PREFIX = "broker-";

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
        createRopMetadataIfMissing(pulsarAdmin, clusterData, RocketMQTopic.getGroupMetaOffsetTopic(), conf,
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
        String ropMetadataTenant = conf.getRocketmqMetadataTenant();
        String ropMetadataNamespace = conf.getRocketmqMetadataTenant() + "/" + conf.getRocketmqMetadataNamespace();

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
                        new TenantInfoImpl(conf.getSuperUserRoles(), Collections.singleton(cluster)));
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

            int targetMessageTTL = conf.getOffsetsMessageTTL();
            Integer messageTTL = namespaces.getNamespaceMessageTTL(ropMetadataNamespace);
            if (messageTTL == null || messageTTL != targetMessageTTL) {
                namespaces.setNamespaceMessageTTL(ropMetadataNamespace, targetMessageTTL);
            }

            namespaces.setAutoTopicCreation(ropMetadataNamespace, AutoTopicCreationOverride.builder()
                    .allowAutoTopicCreation(true)
                    .topicType(TopicType.PARTITIONED.toString())
                    .defaultNumPartitions(1)
                    .build());

            namespaceExists = true;

            createOffsetNamespaceIfNotExist(ropMetadataTenant, cluster, pulsarAdmin, conf);

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

    private static void createOffsetNamespaceIfNotExist(String ropMetadataTenant, String cluster,
            PulsarAdmin pulsarAdmin, RocketMQServiceConfiguration conf) throws PulsarAdminException {
        String ropOffsetNamespace = ropMetadataTenant + "/" + RocketMQTopic.getOffsetNamespace();

        // Check if the metadata namespace exists and create it if not
        Namespaces namespaces = pulsarAdmin.namespaces();
        if (!namespaces.getNamespaces(ropMetadataTenant).contains(ropOffsetNamespace)) {
            log.info("Namespaces: {} does not exist in tenant: {}, creating it ...",
                    ropOffsetNamespace, ropMetadataTenant);
            Set<String> replicationClusters = Sets.newHashSet(cluster);
            namespaces.createNamespace(ropOffsetNamespace, replicationClusters);
            namespaces.setNamespaceReplicationClusters(ropOffsetNamespace, replicationClusters);
        } else {
            List<String> replicationClusters = namespaces.getNamespaceReplicationClusters(ropOffsetNamespace);
            if (!replicationClusters.contains(cluster)) {
                log.info("Namespace: {} exists but cluster: {} is not in the replicationClusters list,"
                        + "updating it ...", ropOffsetNamespace, cluster);
                Set<String> newReplicationClusters = Sets.newHashSet(replicationClusters);
                newReplicationClusters.add(cluster);
                namespaces.setNamespaceReplicationClusters(ropOffsetNamespace, newReplicationClusters);
            }
        }
        // set namespace config if namespace existed
        int retentionMinutes = (int) conf.getOffsetsRetentionMinutes();
        RetentionPolicies retentionPolicies = namespaces.getRetention(ropOffsetNamespace);
        if (retentionPolicies == null) {
            namespaces.setRetention(ropOffsetNamespace,
                    new RetentionPolicies(retentionMinutes, -1));
        }

//        Long compactionThreshold = namespaces.getCompactionThreshold(ropOffsetNamespace);
//        if (compactionThreshold == null || compactionThreshold != MAX_COMPACTION_THRESHOLD) {
//            namespaces.setCompactionThreshold(ropOffsetNamespace, MAX_COMPACTION_THRESHOLD);
//        }

        int targetMessageTTL = conf.getOffsetsMessageTTL();
        Integer messageTTL = namespaces.getNamespaceMessageTTL(ropOffsetNamespace);
        if (messageTTL == null) {
            namespaces.setNamespaceMessageTTL(ropOffsetNamespace, targetMessageTTL);
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
            if (!admin.topics().getSubscriptions(topic).contains(subscriptionName)) {
                MessageId startPos = messageId == null ? MessageId.earliest : messageId;
                admin.topics().createSubscription(topic, subscriptionName, startPos);
            }
        } catch (PulsarAdminException e) {
            log.info("Resources creating for subscription:{} of topic : {}, caused by : {}", subscriptionName, topic,
                    e.getMessage());
        }
    }

    public static String getNoDomainTopic(TopicName topicName) {
        return topicName.getPartitionedTopicName().replace(topicName.getDomain().value() + "://", "");
    }

    public static Map<String, List<String>> genBrokerGroupData(List<String> brokers, int repFactor) {
        Preconditions.checkArgument(brokers != null && !brokers.isEmpty());
        Preconditions.checkArgument(repFactor > 0);
        int uniqBrokersNum = brokers.size();
        int groupNum = (uniqBrokersNum - 1) / repFactor;
        Map<String, List<String>> result = new HashMap<>();
        for (int i = 0; i <= groupNum; i++) {
            String brokerTag = BROKER_TAG_PREFIX + i;
            for (int j = 0; j < repFactor && (j + i * repFactor) < uniqBrokersNum; j++) {
                List<String> brokerList = result.computeIfAbsent(brokerTag, k -> new ArrayList<>(repFactor));
                brokerList.add(brokers.get(j + i * repFactor));
            }
        }
        return result;
    }

    public static boolean autoExpanseBrokerGroupData(RopClusterContent clusterContent,
            List<String> activeBrokers, int repFactor) {
        Preconditions.checkNotNull(clusterContent);
        Preconditions.checkArgument(activeBrokers != null && !activeBrokers.isEmpty());
        Preconditions.checkArgument(repFactor > 0);
        Map<String, List<String>> brokerCluster = clusterContent.getBrokerCluster();
        List<String> brokerTags = brokerCluster.keySet().stream().sorted().collect(Collectors.toList());
        List<String> allBrokers = new ArrayList<>();
        for (String brokerTag : brokerTags) {
            allBrokers.addAll(brokerCluster.get(brokerTag));
        }

        if (!allBrokers.containsAll(activeBrokers)) {
            activeBrokers.removeAll(allBrokers);
            allBrokers.addAll(activeBrokers);
            clusterContent.setBrokerCluster(genBrokerGroupData(allBrokers, repFactor));
            return true;
        }
        return false;
    }

    public static int getTopicPartitionNum(BrokerService brokerService, String pulsarTopic, int defaultValue) {
        Preconditions.checkNotNull(brokerService);
        Preconditions.checkArgument(Strings.isNotBlank(pulsarTopic));
        CompletableFuture<PartitionedTopicMetadata> topicMetaFuture = brokerService
                .fetchPartitionedTopicMetadataAsync(TopicName.get(pulsarTopic));
        try {
            PartitionedTopicMetadata topicMeta = topicMetaFuture.get();
            return topicMeta.partitions;
        } catch (Exception e) {
            log.warn("getTopicPartitionNum topic:{} error.", pulsarTopic, e);
        }
        return defaultValue;
    }
}
