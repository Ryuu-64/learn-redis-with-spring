package top.ryuu64.learn.redis;

import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamReadArgs;
import org.redisson.client.codec.StringCodec;
import org.ryuu.functional.Action2Args;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import top.ryuu64.learn.redis.redisson.EventStreamConsumer;
import top.ryuu64.learn.redis.redisson.domain.RedisStreamArgs;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static top.ryuu64.learn.redis.redisson.EventStreamConsumer.MAX_RETRY_COUNT;
import static top.ryuu64.learn.redis.redisson.EventStreamConsumer.PEL_IDLE_THRESHOLD_MS;

@SpringBootTest
class DeadLetterTest {

    @Autowired
    private RedissonClient redisson;

    @Autowired
    private StringRedisTemplate template;

    @Test
    void testDeadLetter() throws InterruptedException {
        String streamKey = "test:stream";
        String group = "test-group";
        String consumer = "test-consumer";

        // 1. 添加一条消息
        Map<String, String> body = new HashMap<>();
        // field value
        body.put("foo", "bar");
        template.opsForStream().add(streamKey, body);

        // 2. 定义一个一定会失败的 handler
        Map<RedisStreamArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap = Map.of(
                RedisStreamArgs.builder()
                        .streamName(streamKey)
                        .groupName(group)
                        .consumerName(consumer)
                        .build(),
                (msgId, msgBody) -> {
                    throw new RuntimeException("故意抛异常，模拟处理失败");
                }
        );

        EventStreamConsumer consumerService = new EventStreamConsumer(redisson, handlerMap);

        // 3. 等待超过 MAX_RETRY_COUNT 次 claim
        Thread.sleep(PEL_IDLE_THRESHOLD_MS * (MAX_RETRY_COUNT + 1));

        // 4. 检查死信队列
        RStream<String, String> deadStream = redisson.getStream(streamKey + ":dead_letter", StringCodec.INSTANCE);
        Map<StreamMessageId, Map<String, String>> messages = null;
        long timeout = System.currentTimeMillis() + 10_000; // 最多等待 10 秒
        while (System.currentTimeMillis() < timeout) {
            // 读取死信队列最新 10 条消息
            messages = deadStream.readAsync(StreamReadArgs.greaterThan(StreamMessageId.ALL).count(10))
                    .toCompletableFuture().join();
            if (messages != null && !messages.isEmpty()) {
                break;
            }
            Thread.sleep(100); // 小间隔重试
        }
        assertFalse(messages.isEmpty(), "死信队列应该有消息");
    }
}
