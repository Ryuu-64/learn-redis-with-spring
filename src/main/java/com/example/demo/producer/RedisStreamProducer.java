package com.example.demo.producer;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class RedisStreamProducer {
    private final StringRedisTemplate template;

    public RedisStreamProducer(StringRedisTemplate template) {
        this.template = template;
    }

    public String send(String streamKey, Map<String, String> message) {
        RecordId recordId = template.opsForStream().add(streamKey, message);
        return recordId != null ? recordId.getValue() : null;
    }
}

