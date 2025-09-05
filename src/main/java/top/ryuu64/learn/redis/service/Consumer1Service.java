package top.ryuu64.learn.redis.service;

import top.ryuu64.learn.redis.redisson.domain.RedisStreamArgs;
import top.ryuu64.learn.redis.redisson.EventStreamConsumer;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.ryuu.functional.Action2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class Consumer1Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer1Service.class);
    private final EventStreamConsumer eventStreamConsumer;

    public Consumer1Service(RedissonClient redissonClient) {
        Map<RedisStreamArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap = tryAddHandlers();
        eventStreamConsumer = new EventStreamConsumer(redissonClient, handlerMap);
    }

    private Map<RedisStreamArgs, Action2Args<StreamMessageId, Map<String, String>>> tryAddHandlers() {
        return Map.of(
                RedisStreamArgs.builder()
                        .streamName("learn_redis:redisson:stream:1")
                        .groupName("redisson-group-1")
                        .consumerName("consumer")
                        .build(),
                (key, value) -> LOGGER.debug("stream-1/group-1: key={}, value={}", key, value),
                RedisStreamArgs.builder()
                        .streamName("learn_redis:redisson:stream:1")
                        .groupName("redisson-group-5")
                        .consumerName("consumer")
                        .build(),
                (key, value) -> LOGGER.debug("stream-1/group-5: key={}, value={}", key, value),
                RedisStreamArgs.builder()
                        .streamName("learn_redis:redisson:stream:1")
                        .groupName("redisson-group-6")
                        .consumerName("consumer")
                        .build(),
                (key, value) -> LOGGER.debug("stream-1/group-6: key={}, value={}", key, value)
        );
    }
}
