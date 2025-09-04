package com.example.demo.config;

import lombok.Data;

@Data
public class RedissonStreamConfigProperty {
    private String stream;
    private String group;
    private String consumer;
}
