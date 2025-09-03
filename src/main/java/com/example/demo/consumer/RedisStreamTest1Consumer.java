package com.example.demo.consumer;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class RedisStreamTest1Consumer extends RedisStreamConsumer {
    public RedisStreamTest1Consumer(StringRedisTemplate template) {
        super(template);
    }

    @Override
    protected String getStream() {
        return "learn_redis:stream:messages";
    }

    @Override
    protected String getGroup() {
        return "event:stream:message:test1";
    }
}
