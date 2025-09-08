package top.ryuu64.redis.redisson;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import top.ryuu64.redis.redisson.domain.ConsumerArgs;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.ryuu.functional.Action2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class ConsumerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTest.class);
    @Autowired
    private RedissonClient redisson;

    @Test
    void consumer() throws InterruptedException {
        StreamConsumer consumer = new StreamConsumer(redisson, getHandlers());
        consumer.startAsync()
                .whenComplete((res, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Failed to start EventStreamConsumer.", throwable);
                    } else {
                        LOGGER.info("EventStreamConsumer started successfully.");
                    }
                });
        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        try {
            consumer.stopAsync().get(5, TimeUnit.SECONDS);
            LOGGER.info("EventStreamConsumer gracefully stopped.");
        } catch (Exception e) {
            LOGGER.warn("Some tasks did not complete in time during shutdown.", e);
        }
    }

    private Map<ConsumerArgs, Action2Args<StreamMessageId, Map<String, String>>> getHandlers() {
        return Map.of(
                ConsumerArgs.builder()
                        .streamName("learn_redis:redisson:stream:1")
                        .groupName("redisson-group-1")
                        .consumerName("consumer")
                        .build(),
                (key, value) -> LOGGER.debug("stream-1/group-1: key={}, value={}", key, value),
                ConsumerArgs.builder()
                        .streamName("learn_redis:redisson:stream:1")
                        .groupName("redisson-group-5")
                        .consumerName("consumer")
                        .build(),
                (key, value) -> LOGGER.debug("stream-1/group-5: key={}, value={}", key, value),
                ConsumerArgs.builder()
                        .streamName("learn_redis:redisson:stream:1")
                        .groupName("redisson-group-6")
                        .consumerName("consumer")
                        .build(),
                (key, value) -> LOGGER.debug("stream-1/group-6: key={}, value={}", key, value)
        );
    }
}
