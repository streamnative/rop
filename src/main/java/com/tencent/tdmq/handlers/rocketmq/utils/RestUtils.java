package com.tencent.tdmq.handlers.rocketmq.utils;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

@Slf4j
public class RestUtils {

    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createDefault();
    private static final RequestConfig REQUEST_CONFIG = RequestConfig.custom().setConnectTimeout(5000)
            .setConnectionRequestTimeout(5000)
            .setSocketTimeout(5000)
            .build();


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
