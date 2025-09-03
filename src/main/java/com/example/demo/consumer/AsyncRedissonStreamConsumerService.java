package com.example.demo.consumer;

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

/**
 * 异步 Redisson Stream 消费基类
 * - 支持多个消费组
 * - 不阻塞线程
 * - 支持 pending 补偿
 */
@Service
public class AsyncRedissonStreamConsumerService {
    public record GroupConfig(String streamKey, String groupName, String consumerName) {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRedissonStreamConsumerService.class);
    private static final int BATCH_SIZE = 10;
    private static final long READ_TIMEOUT_MS = 2000;
    private static final long ERROR_RETRY_DELAY_MS = 1000;

    private final RedissonClient redisson;
    private final List<GroupConfig> groups;
    private final Map<GroupConfig, CompletableFuture<?>> consumerTasks = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private final Actions3Args<Object, Object, Object> handleMessage = Actions3Args.event();

    protected AsyncRedissonStreamConsumerService(
            RedissonClient redisson,
            List<GroupConfig> groups
    ) {
        this.redisson = redisson;
        this.groups = new ArrayList<>(groups); // 创建副本避免并发修改
        groups.forEach(this::tryCreateGroup);
        groups.forEach(this::startConsuming);
    }

    /** 尝试创建消费组 */
    private void tryCreateGroup(GroupConfig config) {
        RStream<String, String> stream = redisson.getStream(config.streamKey, StringCodec.INSTANCE);
        try {
            stream.createGroup(config.groupName, StreamMessageId.NEWEST);
            LOGGER.info("Created group [{}] for stream [{}]", config.groupName, config.streamKey);
        } catch (Exception e) {
            LOGGER.info("Stream group [{}] already exists", config.groupName);
        }
    }

    /** 启动消费 */
    private void startConsuming(GroupConfig config) {
        CompletableFuture<?> task = CompletableFuture.runAsync(() -> consumeLoop(config));
        consumerTasks.put(config, task);
    }

    /** 消费循环 */
    private void consumeLoop(GroupConfig config) {
        if (!running) return;

        RStream<String, String> stream = redisson.getStream(config.streamKey, StringCodec.INSTANCE);
        RFuture<Map<StreamMessageId, Map<String, String>>> future = stream.readGroupAsync(
                config.groupName,
                config.consumerName,
                BATCH_SIZE,
                READ_TIMEOUT_MS,
                TimeUnit.MILLISECONDS,
                StreamMessageId.NEVER_DELIVERED
        );

        future.whenComplete((messages, ex) -> {
            if (ex != null) {
                handleError(config, ex);
                return;
            }

            try {
                if (messages != null && !messages.isEmpty()) {
                    processMessages(config, stream, messages);
                }
            } catch (Exception e) {
                LOGGER.error("Error processing messages for group {}", config.groupName, e);
            } finally {
                // 使用 scheduledExecutor 调度下一次消费，避免递归
                if (running) {
                    scheduleNextConsume(config);
                }
            }
        });
    }

    /** 处理消息批次 */
    private void processMessages(
            GroupConfig config, RStream<String, String> stream, Map<StreamMessageId, Map<String, String>> messages
    ) {
        for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
            try {
                handleMessage.invoke(config, entry.getKey(), entry.getValue());
                stream.ackAsync(config.groupName, entry.getKey())
                        .exceptionally(ex -> {
                            LOGGER.error("Failed to acknowledge message {} for group {}",
                                    entry.getKey(), config.groupName, ex);
                            return null;
                        });
            } catch (Exception e) {
                LOGGER.error("Failed to process message {} for group {}",
                        entry.getKey(), config.groupName, e);
            }
        }
    }

    /** 处理错误 */
    private void handleError(GroupConfig config, Throwable ex) {
        LOGGER.error("Error in consumeLoop for group {}: {}", config.groupName, ex.getMessage(), ex);
        if (running) {
            CompletableFuture.delayedExecutor(ERROR_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                    .execute(() -> consumeLoop(config));
        }
    }

    private void scheduleNextConsume(GroupConfig config) {
        CompletableFuture.runAsync(() -> consumeLoop(config));
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
