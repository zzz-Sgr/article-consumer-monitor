package com.article.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling; // 1. 导入定时任务包

@SpringBootApplication
@EnableScheduling // 2. 开启定时任务总开关，没有这个注解任务不会运行
public class ArticleConsumerMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ArticleConsumerMonitorApplication.class, args);
    }

}