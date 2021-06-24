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

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * Pulsar Admin Rest Service.
 */
@Slf4j
public class PulsarAdminRestService {

    public static String createSecretKey(String tenant, String token) {
        final String adminURL = "http://9.208.3.175:8080/admin/v2";
        final String algorithm = "HS256";
        OkHttpClient client = new OkHttpClient();
        String createSecretKeyUrl = adminURL + "/brokers/security/secretKey/" + algorithm;

        log.info("[createSecretKey] The request URL for create secret key is: {}", createSecretKeyUrl);

        String authValue = "Bearer " + token;
        Request request = new Request
                .Builder()
                .url(createSecretKeyUrl)
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", authValue)
                .get()
                .build();

        try {
            Response response = client.newCall(request).execute();
            if (response.code() >= 200 && response.code() < 300) {
                if (response.body() != null) {
                    log.info("[createSecretKey] Create secret string key is:{}", response.body().string());
                    JSONObject jsonObject = JSONObject.parseObject(response.body().string());
                    return jsonObject.getString("secretKey");
                }
            }
        } catch (IOException e) {
            log.error("Tenant [{}] create secret key", tenant, e);
            throw new RuntimeException("Create secret key failed: {}", e);
        }

        return null;
    }

    public static void setSecretKey(String tenant, String secretKey, String token) {
        final String adminURL = "http://9.208.3.175:8080/admin/v2";
        final String algorithm = "HS256";
        OkHttpClient client = new OkHttpClient();
        String updateTenantKeyUrl = adminURL + "/tenants/" + tenant + "/tokenSecretKey/" + algorithm;
        log.info("[setSecretKey] The request URL for update tenant key is: {}", updateTenantKeyUrl);

        RequestBody body = RequestBody.create(MediaType.parse("application/json"), secretKey);
        Request request = new Request.Builder()
                .url(updateTenantKeyUrl)
                .addHeader("Content-Type", "application/json")
                .addHeader("Authorization", "Bearer " + token)
                .post(body)
                .get()
                .build();

        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                log.error("Tenant [{}] set secret key", tenant, e);
                throw new RuntimeException("Set secret key failed", e);
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (response.code() >= 200 && response.code() < 300) {
                    log.info("Successfully set secret key for tenant [{}]", tenant);
                }
            }
        });

    }
}
