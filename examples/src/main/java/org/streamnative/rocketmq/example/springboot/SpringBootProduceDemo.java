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

import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * SpringBoot produce demo.
 */
@Slf4j
@SpringBootApplication
public class SpringBootProduceDemo implements CommandLineRunner {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    public static void main(String[] args) {
        SpringApplication.run(SpringBootProduceDemo.class, args);
    }

    public void run(String... args) {
        log.info("Send message: [Hello world!]");

        // 使用主题全名 [namespace]%[topic], 例如：rocketmq-og7vojz5e33k|test-ns%test-topic
        rocketMQTemplate.convertAndSend("rocketmq-og7vojz5e33k|test-ns%test-topic", "Hello, World!");
    }
}
