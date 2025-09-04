package com.example.demo.config;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "redis.stream")
public class RedissonStreamConfig {
    @Getter
    private final List<RedissonStreamConfigProperty> groups = new ArrayList<>();
}
