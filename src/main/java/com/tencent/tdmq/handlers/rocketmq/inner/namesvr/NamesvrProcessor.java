package com.tencent.tdmq.handlers.rocketmq.inner.namesvr;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rocketmq.common.constant.PermName.PERM_READ;
import static org.apache.rocketmq.common.constant.PermName.PERM_WRITE;
import static org.apache.rocketmq.common.protocol.RequestCode.GET_ROUTEINTO_BY_TOPIC;

import com.tencent.tdmq.handlers.rocketmq.RocketMQServiceConfiguration;
import com.tencent.tdmq.handlers.rocketmq.inner.RocketMQBrokerController;
import com.tencent.tdmq.handlers.rocketmq.utils.Random;
import com.tencent.tdmq.handlers.rocketmq.utils.RocketMQTopic;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.rocketmq.common.Pair;
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

    private int servicePort = 9876;
    private final RocketMQBrokerController brokerController;
    private final RocketMQServiceConfiguration config;
    private final int defaultNumPartitions;
    private final MQTopicManager mqTopicManager;

    public NamesvrProcessor(RocketMQBrokerController brokerController) {
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
        topicRouteData.setBrokerDatas(brokerDatas);
        topicRouteData.setQueueDatas(queueDatas);

        String clusterName = config.getClusterName();
        // 根据传入的请求获取指定的topic
        String requestTopic = requestHeader.getTopic();
        if (Strings.isNotBlank(requestTopic)) {
            RocketMQTopic mqTopic = new RocketMQTopic(requestTopic);
            Map<Integer,  InetSocketAddress> topicBrokerAddr = mqTopicManager
                    .getTopicBrokerAddr(mqTopic.getPulsarTopicName());
            int partitionNum = topicBrokerAddr.size();
            if (partitionNum > 0) {
                mqTopicManager.getTopicBrokerAddr(mqTopic.getPulsarTopicName()).forEach((i, addr) -> {
                    String ownerBrokerAddress = addr.toString();
                    String hostName = addr.getHostName();
                    String brokerAddress = parseBrokerAddress(ownerBrokerAddress, brokerController.getRemotingServer().getPort());
                   // long brokerID = Math.abs(addr.hashCode());

                    HashMap<Long, String> brokerAddrs = new HashMap<>();
                    brokerAddrs.put(0L, brokerAddress);
                    BrokerData brokerData = new BrokerData(clusterName, hostName, brokerAddrs);
                    brokerDatas.add(brokerData);
                    topicRouteData.setBrokerDatas(brokerDatas);

                    QueueData queueData = new QueueData();
                    queueData.setBrokerName(hostName);
                    queueData.setReadQueueNums(partitionNum);
                    queueData.setWriteQueueNums(partitionNum);
                    queueData.setPerm(PERM_WRITE|PERM_READ);
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

    public static final Pattern BROKER_ADDR_PAT = Pattern.compile("([^/:]+:)(\\d+)");

    public String parseBrokerAddress(String brokerAddress, int port) {
        // pulsar://localhost:6650
        if (null == brokerAddress) {
            log.error("The brokerAddress is null, please check.");
            return "";
        }
        Matcher matcher = BROKER_ADDR_PAT.matcher(brokerAddress);
        String result = brokerAddress;
        if (matcher.find()) {
            result = matcher.group(1) + servicePort;
        }
        return result;
    }
}
