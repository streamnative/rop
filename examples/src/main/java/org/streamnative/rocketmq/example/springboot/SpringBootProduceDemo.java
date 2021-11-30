package org.streamnative.rocketmq.example.springboot;

import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
