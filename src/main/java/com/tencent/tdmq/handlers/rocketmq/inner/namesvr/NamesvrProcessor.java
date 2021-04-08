package com.tencent.tdmq.handlers.rocketmq.inner.namesvr;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rocketmq.common.protocol.RequestCode.GET_ROUTEINTO_BY_TOPIC;

import com.tencent.tdmq.handlers.rocketmq.RocketMQServiceConfiguration;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.utils.Random;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

@Slf4j
public class NamesvrProcessor implements NettyRequestProcessor {

    private final String rocketMQServerPort = "9876";

    private final RocketMQBrokerController brokerController;
    private final RocketMQServiceConfiguration config;
    private final int defaultNumPartitions;
    private final MQTopicManager mqTopicManager;

    public NamesvrProcessor(RocketMQBrokerController brokerController) throws PulsarServerException {
        this.brokerController = brokerController;
        this.config = brokerController.getServerConfig();
        this.defaultNumPartitions = config.getDefaultNumPartitions();
        this.mqTopicManager = brokerController.getMqTopicManager();
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
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                // return this.getBrokerClusterInfo(ctx, request);
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

        Lookup lookupService = this.brokerController.getBrokerService().pulsar().getAdminClient().lookups();
        PulsarAdmin adminClient = this.brokerController.getBrokerService().getPulsar().getAdminClient();
        List<String> clusters = adminClient.clusters().getClusters();

        // 根据传入的请求获取指定的topic
        String requestTopic = requestHeader.getTopic();
        if (Strings.isNotBlank(requestTopic)) {
            RocketMQTopic mqTopic = new RocketMQTopic(requestTopic);
            PartitionedTopicMetadata pTopicMeta = null;

            pTopicMeta = mqTopicManager.getPartitionedTopicMetadata(mqTopic.getFullName());
            if (pTopicMeta.partitions > 0) {
                for (int i = 0; i < pTopicMeta.partitions; i++) {
                    String ownerBrokerAddress = lookupService.lookupTopic(mqTopic.getPartitionName(i));
                    String brokerAddress = parseBrokerAddress(ownerBrokerAddress, rocketMQServerPort);
                    Long brokerID = Random.randomLong(8);
                    HashMap<Long, String> brokerAddrs = new HashMap<>();
                    brokerAddrs.put(brokerID, brokerAddress);

                    BrokerData brokerData = new BrokerData(clusters.get(0), ownerBrokerAddress, brokerAddrs);
                    brokerDatas.add(brokerData);
                    topicRouteData.setBrokerDatas(brokerDatas);

                    QueueData queueData = new QueueData();
                    queueData.setBrokerName(brokerAddress);
                    queueData.setReadQueueNums(pTopicMeta.partitions);
                    queueData.setWriteQueueNums(pTopicMeta.partitions);
                    queueDatas.add(queueData);
                    topicRouteData.setQueueDatas(queueDatas);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Request {}: Topic {} has single partition, "
                                    + "auto create partitioned topic",
                            ctx.channel(), request, mqTopic);
                }

                String ownerBrokerAddress = lookupService.lookupTopic(mqTopic.getFullName());
                String brokerAddress = parseBrokerAddress(ownerBrokerAddress, rocketMQServerPort);
                Long brokerID = Random.randomLong(8);
                HashMap<Long, String> brokerAddrs = new HashMap<>();
                brokerAddrs.put(brokerID, brokerAddress);

                BrokerData brokerData = new BrokerData(clusters.get(0), ownerBrokerAddress, brokerAddrs);
                brokerDatas.add(brokerData);
                topicRouteData.setBrokerDatas(brokerDatas);

                QueueData queueData = new QueueData();
                queueData.setBrokerName(brokerAddress);
                queueData.setReadQueueNums(pTopicMeta.partitions);
                queueData.setWriteQueueNums(pTopicMeta.partitions);
                queueDatas.add(queueData);
                topicRouteData.setQueueDatas(queueDatas);
            }

            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
                + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    public String parseBrokerAddress(String brokerAddress, String port) {
        // pulsar://localhost:6650
        if (null == brokerAddress) {
            log.error("The brokerAddress is null, please check.");
            return "";
        }

        String subStr = brokerAddress.substring(9);
        String[] parts = StringUtils.split(subStr, ':');
        String ipString = parts[0];
        return ipString + ":" + port;
    }
}
