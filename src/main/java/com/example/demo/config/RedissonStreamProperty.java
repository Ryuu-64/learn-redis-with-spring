package com.example.demo.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class RedissonStreamProperty {
    private String stream;
    private String group;
    private String consumer;
}
