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

package org.streamnative.rocketmq.example.springboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Service;

/**
 * SpringBoot push consume demo.
 */
@SpringBootApplication
public class SpringBootPushConsumeDemo {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootPushConsumeDemo.class, args);
    }

    @Slf4j
    @Service
    @RocketMQMessageListener(
            /* 使用主题全名 [namespace]%[topic], 例如：rocketmq-og7vojz5e33k|test-ns%test-topic */
            topic = "rocketmq-og7vojz5e33k|test-ns%test-topic",
            /* 使用主题全名 [namespace]%[group], 例如：rocketmq-og7vojz5e33k|test-ns%test-group */
            consumerGroup = "rocketmq-og7vojz5e33k|test-ns%test-group",
            /* 填写绑定的角色token */
            accessKey = "eyJrZXlJZCI6InJvY2tldG1xLW9nN3Zvano1ZTMzayIsImFsZyI6IkhTMjU2In0.eyJzdWIiOiJyb2NrZXRtcS1vZzd2b2p6NWUzM2tfdGVzdC1yb2xlIn0.YPYh-7dB8lzH178b6_tP_mzdNiy6dbnpRR9RFijIT3U",
            /* 固定填写ROP即可 */
            secretKey = "ROP"
    )
    public static class Consumer implements RocketMQListener<String> {

        public void onMessage(String message) {
            log.info("Received message: [{}]", message);
        }
    }
}
