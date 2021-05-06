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

package com.tencent.tdmq.handlers.rocketmq.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pulsar.common.lookup.data.LookupData;

/**
 * Rest utils class.
 */
@Deprecated
@Slf4j
public class RestUtils {

    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createDefault();
    private static final RequestConfig REQUEST_CONFIG = RequestConfig.custom()
            .setConnectTimeout(5000)
            .setConnectionRequestTimeout(5000)
            .setSocketTimeout(5000)
            .build();


    public static String getBrokerAddr(String topic, String listenerName) {
        Map<String, String> headers = Maps.newHashMap();
        String url = String
                .format("http://127.0.0.1:8080/lookup/v2/topic/persistent/%s?listenerName=%s", topic, listenerName);
        LookupData lookupData = JSON.parseObject(RestUtils.get(url, headers), LookupData.class);
        String brokerUrl = lookupData.getBrokerUrl();
        brokerUrl = brokerUrl.replaceAll("pulsar://", "");
        return brokerUrl;
    }

    public static String get(String uri, Map<String, String> headers) {
        // 创建httpGet远程连接实例
        HttpGet httpGet = new HttpGet(uri);
        httpGet.setConfig(REQUEST_CONFIG);
        // 设置请求头信息，鉴权
        headers.forEach(httpGet::setHeader);

        try {
            try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet)) {
                if (success(response.getStatusLine().getStatusCode())) {
                    // 通过返回对象获取返回数据
                    HttpEntity entity = response.getEntity();
                    // 通过EntityUtils中的toString方法将结果转换为字符串
                    return EntityUtils.toString(entity);
                }
            }
        } catch (Exception e) {
            log.error("RestUtils get [{}], headers: [{}]", uri, headers, e);
        }

        return "";
    }

    private static boolean success(int code) {
        return code == 200 || code == 202;
    }

}
