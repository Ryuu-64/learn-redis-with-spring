package top.ryuu64.redis.redisson;

import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamReadArgs;
import org.redisson.client.codec.StringCodec;
import org.ryuu.functional.Action2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import top.ryuu64.redis.redisson.domain.ConsumerArgs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;

@SpringBootTest
class DeadLetterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterTest.class);
    @Autowired
    private RedissonClient redisson;
    @Autowired
    private StringRedisTemplate template;
    @Autowired
    private StreamProducer producer;

    @Test
    void deadLetter() throws InterruptedException {
        ConsumerArgs args = ConsumerArgs.builder()
                .streamName("test-stream")
                .groupName("test-group")
                .consumerName("test-consumer")
                .build();

        // 1. 定义一个一定会失败的 handler
        Map<ConsumerArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap = Map.of(
                args,
                (msgId, msgBody) -> {
                    throw new RuntimeException("故意抛异常，模拟处理失败");
                }
        );

        StreamConsumer consumer = new StreamConsumer(redisson);
        consumer.startAsync(handlerMap)
                .whenComplete((res, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Failed to start ReliableRedisStreamConsumer", throwable);
                    } else {
                        LOGGER.info("ReliableRedisStreamConsumer started successfully");
                    }
                })
                .join();


        // 2. 添加一条消息
        Map<String, String> body = new HashMap<>();
        body.put("field_name", "field_value");
        template.opsForStream().add(args.getStreamName(), body);

        // 3. 检查死信队列
        String streamName = args.getStreamName() + ":dead_letter";
        RStream<String, String> deadStream = redisson.getStream(streamName, StringCodec.INSTANCE);
        Map<StreamMessageId, Map<String, String>> messages = null;
        long timeout = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < timeout) {
            messages = deadStream
                    .readAsync(
                            StreamReadArgs.
                                    greaterThan(StreamMessageId.ALL)
                                    .count(50)
                    )
                    .toCompletableFuture().join();
            if (messages != null && !messages.isEmpty()) {
                break;
            }
            Thread.sleep(100); // 小间隔重试
        }
        assertFalse(messages == null || messages.isEmpty(), "死信队列应该有消息");
        try {
            consumer.stopAsync().get(20, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void deadLetter2() throws InterruptedException {
        ConsumerArgs args = ConsumerArgs.builder()
                .streamName("test-stream")
                .groupName("test-group")
                .consumerName("test-consumer")
                .build();

        Map<ConsumerArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap = Map.of(
                args,
                (msgId, msgBody) -> {
                }
        );

        StreamConsumer consumer = new StreamConsumer(redisson);
        consumer.startAsync(handlerMap)
                .whenComplete((res, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Failed to start.", throwable);
                    } else {
                        LOGGER.info("Started successfully.");
                    }
                })
                .join();

        Map<String, String> messages = new HashMap<>();
        messages.put("field_name", "field_value");
        for (int i = 0; i < 10_000; i++) {
            producer.addAsync(args, messages);
        }

        try {
            consumer.stopAsync().get(20, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
