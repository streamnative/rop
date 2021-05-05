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

package com.tencent.tdmq.handlers.rocketmq.inner.namesvr;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rocketmq.common.constant.PermName.PERM_READ;
import static org.apache.rocketmq.common.constant.PermName.PERM_WRITE;
import static org.apache.rocketmq.common.protocol.RequestCode.GET_ROUTEINTO_BY_TOPIC;

import com.google.common.collect.Maps;
import com.tencent.tdmq.handlers.rocketmq.RocketMQServiceConfiguration;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.testng.collections.Sets;

/**
 * Nameserver processor.
 */
@Slf4j
public class NameserverProcessor implements NettyRequestProcessor {

    public static final Pattern BROKER_ADDER_PAT = Pattern.compile("([^/:]+:)(\\d+)");
    private final RocketMQBrokerController brokerController;
    private final RocketMQServiceConfiguration config;
    private final int defaultNumPartitions;
    private final MQTopicManager mqTopicManager;
    private final int servicePort = 9876;

    public NameserverProcessor(RocketMQBrokerController brokerController) {
        this.brokerController = brokerController;
        this.config = brokerController.getServerConfig();
        this.defaultNumPartitions = config.getDefaultNumPartitions();
        this.mqTopicManager = brokerController.getTopicConfigManager();
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws Exception {
        if (ctx != null) {
            log.debug("receive request, {} {} {}",
                    request.getCode(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    request);
        }

        switch (request.getCode()) {
            case RequestCode.PUT_KV_CONFIG:
            case RequestCode.GET_KV_CONFIG:
            case RequestCode.DELETE_KV_CONFIG:
            case RequestCode.QUERY_DATA_VERSION:
                // TODO return queryBrokerTopicConfig(ctx, request);
            case RequestCode.REGISTER_BROKER:
            case RequestCode.UNREGISTER_BROKER:
            case GET_ROUTEINTO_BY_TOPIC:
                // TODO return this.getRouteInfoByTopic(ctx, request);
                return handleTopicMetadata(ctx, request);
            case RequestCode.GET_BROKER_CLUSTER_INFO:  // 需要 | 管控端需要
                return this.getBrokerClusterInfo(ctx, request);
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                // return getAllTopicListFromNameserver(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                //return deleteTopicInNamesrv(ctx, request);
            case RequestCode.GET_KVLIST_BY_NAMESPACE:
            case RequestCode.GET_TOPICS_BY_CLUSTER:
                //return this.getTopicsByCluster(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
                //return this.getSystemTopicListFromNs(ctx, request);
            case RequestCode.GET_UNIT_TOPIC_LIST:
                //return this.getUnitTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
                //return this.getHasUnitSubTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                //return this.getHasUnitSubUnUnitTopicList(ctx, request);
            case RequestCode.UPDATE_NAMESRV_CONFIG:
            case RequestCode.GET_NAMESRV_CONFIG:
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    protected RemotingCommand handleTopicMetadata(ChannelHandlerContext ctx, RemotingCommand request)
            throws Exception {
        checkNotNull(request);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
                (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);
        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDatas = new ArrayList<>();
        List<QueueData> queueDatas = new ArrayList<>();
        topicRouteData.setBrokerDatas(brokerDatas);
        topicRouteData.setQueueDatas(queueDatas);

        String clusterName = config.getClusterName();

        // 如果主题名和clusterName相同，返回集群中任意一个节点到客户端。这里为了兼容客户端创建主题操作
        if (clusterName.equals(requestHeader.getTopic())) {
            try {
                PulsarAdmin adminClient = brokerController.getBrokerService().pulsar().getAdminClient();
                List<String> brokers = adminClient.brokers().getActiveBrokers(clusterName);
                String randomBroker = brokers.get(new Random().nextInt(brokers.size()));
                String rmqBrokerAddress =
                        parseBrokerAddress(randomBroker, brokerController.getRemotingServer().getPort());
                BrokerData brokerData = new BrokerData();
                HashMap<Long, String> brokerAddrs = Maps.newHashMap();
                brokerAddrs.put(0L, rmqBrokerAddress);
                brokerData.setBrokerAddrs(brokerAddrs);
                brokerDatas.add(brokerData);
                byte[] content = topicRouteData.encode();
                response.setBody(content);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            } catch (Exception e) {
                log.error("Cluster [{}] get route info failed", clusterName, e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(null);
                return response;
            }
        }

        // 根据传入的请求获取指定的topic
        String requestTopic = requestHeader.getTopic();
        if (Strings.isNotBlank(requestTopic)) {
            RocketMQTopic mqTopic =
                    requestTopic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC) ? RocketMQTopic
                            .getRocketMQMetaTopic(requestTopic)
                            : new RocketMQTopic(requestTopic);
            Map<Integer, InetSocketAddress> topicBrokerAddr = mqTopicManager
                    .getTopicBrokerAddr(mqTopic.getPulsarTopicName());
            if (topicBrokerAddr != null && topicBrokerAddr.size() > 0) {
                topicBrokerAddr.forEach((i, addr) -> {
                    String ownerBrokerAddress = addr.toString();
                    String hostName = addr.getHostName();
                    String brokerAddress = parseBrokerAddress(ownerBrokerAddress,
                            brokerController.getRemotingServer().getPort());

                    HashMap<Long, String> brokerAddrs = new HashMap<>();
                    brokerAddrs.put(0L, brokerAddress);
                    BrokerData brokerData = new BrokerData(clusterName, hostName, brokerAddrs);
                    brokerDatas.add(brokerData);
                    topicRouteData.setBrokerDatas(brokerDatas);

                    QueueData queueData = new QueueData();
                    queueData.setBrokerName(hostName);
                    queueData.setReadQueueNums(topicBrokerAddr.size());
                    queueData.setWriteQueueNums(topicBrokerAddr.size());
                    queueData.setPerm(PERM_WRITE | PERM_READ);
                    queueDatas.add(queueData);
                    topicRouteData.setQueueDatas(queueDatas);

                });
                byte[] content = topicRouteData.encode();
                response.setBody(content);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
                + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    public String parseBrokerAddress(String brokerAddress, int port) {
        // pulsar://localhost:6650
        if (null == brokerAddress) {
            log.error("The brokerAddress is null, please check.");
            return "";
        }
        Matcher matcher = BROKER_ADDER_PAT.matcher(brokerAddress);
        String result = brokerAddress;
        if (matcher.find()) {
            result = matcher.group(1) + servicePort;
        }
        return result;
    }

    public String getBrokerHost(String brokerAddress) {
        // pulsar://localhost:6650
        if (null == brokerAddress) {
            log.error("The brokerAddress is null, please check.");
            return "";
        }
        Matcher matcher = BROKER_ADDER_PAT.matcher(brokerAddress);
        if (matcher.find()) {
            return matcher.group(1).replaceAll(":", "");
        }
        return brokerAddress;
    }

    /**
     * 获取broker集群信息，当前认为一个broker物理集群中只有一个broker集群，这里只返回一个broker集群中的一个节点.
     *
     * <p>这里为了兼容客户端主题删除逻辑</p>
     * <p>rocketmq中删除主题客户端会依次请求broker集群下全部broker节点执行主题删除，pulsar中只需要到一台节点上执行删除主题操作即可</p>
     */
    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String clusterName = config.getClusterName();
        try {
            PulsarAdmin adminClient = brokerController.getBrokerService().pulsar().getAdminClient();
            List<String> brokers = adminClient.brokers().getActiveBrokers(clusterName);
            // 随机获取一个broker节点
            String randomBroker = brokers.get(new Random().nextInt(brokers.size()));
            // 组装成rmq broker节点地址
            String rmqBrokerAddress = parseBrokerAddress(randomBroker, brokerController.getRemotingServer().getPort());

            HashMap<String, BrokerData> brokerAddrTable = Maps.newHashMap();
            HashMap<Long, String> brokerAddrs = Maps.newHashMap();
            brokerAddrs.put(0L, rmqBrokerAddress);
            brokerAddrTable
                    .put(rmqBrokerAddress, new BrokerData(clusterName, getBrokerHost(randomBroker), brokerAddrs));

            ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
            clusterInfoSerializeWrapper.setBrokerAddrTable(brokerAddrTable);
            HashMap<String, Set<String>> clusterAddrTable = Maps.newHashMap();
            clusterAddrTable.put(clusterName, Sets.newHashSet(rmqBrokerAddress));
            clusterInfoSerializeWrapper.setClusterAddrTable(clusterAddrTable);

            response.setBody(clusterInfoSerializeWrapper.encode());

            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        } catch (Exception e) {
            log.error("ClusterName [{}] getBrokerClusterInfo failed", clusterName, e);
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark(null);
        return response;
    }
}
