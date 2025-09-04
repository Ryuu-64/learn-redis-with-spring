package com.example.demo.consumer;

import com.example.demo.config.RedissonStreamConfig;
import com.example.demo.config.RedissonStreamConfigProperty;
import org.redisson.api.RFuture;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.client.codec.StringCodec;
import org.ryuu.functional.Actions3Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

@Service
public class AsyncRedissonStreamConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRedissonStreamConsumerService.class);
    private static final int BATCH_SIZE = 10;
    private static final long READ_TIMEOUT_MS = 2000;
    private static final long ERROR_RETRY_DELAY_MS = 1000;

    private final RedissonClient redisson;
    private final Map<RedissonStreamConfigProperty, CompletableFuture<?>> consumerTasks = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    private final Actions3Args<RedissonStreamConfigProperty, StreamMessageId, Map<String, String>> handleMessage = Actions3Args.event();

    protected AsyncRedissonStreamConsumerService(
            RedissonClient redisson,
            RedissonStreamConfig config
    ) {
        this.redisson = redisson;
        List<RedissonStreamConfigProperty> groups = config.getGroups();
        groups.forEach(this::tryCreateGroup);
        groups.forEach(this::startConsuming);
        handleMessage.add((property, key, value) -> {
            System.out.println(property + " " + key + " " + value);
        });
    }

    /** 尝试创建消费组 */
    private void tryCreateGroup(RedissonStreamConfigProperty property) {
        RStream<String, String> stream = redisson.getStream(property.getStream(), StringCodec.INSTANCE);
        try {
            stream.createGroup(property.getGroup(), StreamMessageId.NEWEST);
            LOGGER.info("Created group [{}] for stream [{}]", property.getGroup(), property.getStream());
        } catch (Exception e) {
            LOGGER.info("Stream group [{}] already exists", property.getGroup());
        }
    }

    /** 启动消费 */
    private void startConsuming(RedissonStreamConfigProperty property) {
        CompletableFuture<?> task = CompletableFuture.runAsync(() -> consumeLoop(property));
        consumerTasks.put(property, task);
    }

    /** 消费循环 */
    private void consumeLoop(RedissonStreamConfigProperty property) {
        if (!running) {
            return;
        }

        RStream<String, String> stream = redisson.getStream(property.getStream(), StringCodec.INSTANCE);
        RFuture<Map<StreamMessageId, Map<String, String>>> future = stream.readGroupAsync(
                property.getGroup(),
                property.getConsumer(),
                BATCH_SIZE,
                READ_TIMEOUT_MS,
                TimeUnit.MILLISECONDS,
                StreamMessageId.NEVER_DELIVERED
        );

        future.whenComplete((messages, ex) -> {
            if (ex != null) {
                handleError(property, ex);
                return;
            }

            try {
                if (messages != null && !messages.isEmpty()) {
                    processMessages(property, stream, messages);
                }
            } catch (Exception e) {
                LOGGER.error("Error processing messages for group {}", property.getGroup(), e);
            } finally {
                // 使用 scheduledExecutor 调度下一次消费，避免递归
                if (running) {
                    scheduleNextConsume(property);
                }
            }
        });
    }

    /** 处理消息批次 */
    private void processMessages(
            RedissonStreamConfigProperty property, RStream<String, String> stream, Map<StreamMessageId, Map<String, String>> messages
    ) {
        for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
            try {
                a(property, entry);
                stream.ackAsync(property.getGroup(), entry.getKey())
                        .exceptionally(ex -> {
                            LOGGER.error("Failed to acknowledge message {} for group {}",
                                    entry.getKey(), property.getGroup(), ex);
                            return null;
                        });
            } catch (Exception e) {
                LOGGER.error("Failed to process message {} for group {}",
                        entry.getKey(), property.getGroup(), e);
            }
        }
    }

    private void a(RedissonStreamConfigProperty property, Map.Entry<StreamMessageId, Map<String, String>> entry) {
        handleMessage.invoke(property, entry.getKey(), entry.getValue());
    }

    /** 处理错误 */
    private void handleError(RedissonStreamConfigProperty property, Throwable ex) {
        LOGGER.error("Error in consumeLoop for group {}: {}", property.getGroup(), ex.getMessage(), ex);
        if (running) {
            CompletableFuture.delayedExecutor(ERROR_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                    .execute(() -> consumeLoop(property));
        }
    }

    private void scheduleNextConsume(RedissonStreamConfigProperty property) {
        CompletableFuture.runAsync(() -> consumeLoop(property));
    }

    @PreDestroy
    public void stop() {
        running = false;
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(
                consumerTasks.values().toArray(new CompletableFuture[0])
        );

        try {
            allTasks.get(5, TimeUnit.SECONDS);
            LOGGER.info("AsyncRedissonStreamConsumer gracefully stopped");
        } catch (Exception e) {
            LOGGER.warn("Some tasks did not complete in time during shutdown", e);
        }
    }
}
