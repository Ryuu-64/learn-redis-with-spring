package com.example.demo.consumer;

import com.example.demo.config.RedissonStreamConfig;
import com.example.demo.config.RedissonStreamProperty;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import org.ryuu.functional.Func2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

import static org.ryuu.functional.util.FunctionalInvokeIfNotNullUtils.*;

@Service
public class AsyncRedissonStreamConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRedissonStreamConsumerService.class);
    private static final int BATCH_SIZE = 10;
    private static final long READ_TIMEOUT_MS = 2000;
    private static final long ERROR_RETRY_DELAY_MS = 1000;

    private final RedissonClient redisson;
    private final Map<RedissonStreamProperty, CompletableFuture<?>> consumerTasks = new ConcurrentHashMap<>();
    private volatile boolean isRunning = true;
    private final Map<RedissonStreamProperty, Func2Args<StreamMessageId, Map<String, String>, Boolean>> handlerMap = new ConcurrentHashMap<>();

    protected AsyncRedissonStreamConsumerService(
            RedissonClient redisson,
            RedissonStreamConfig config
    ) {
        this.redisson = redisson;
        List<RedissonStreamProperty> groups = config.getGroups();
        groups.forEach(this::tryCreateGroup);
        groups.forEach(this::startConsuming);
    }

    public void addHandler(RedissonStreamProperty property, Func2Args<StreamMessageId, Map<String, String>, Boolean> handler) {
        handlerMap.put(property, handler);
    }

    private void tryCreateGroup(RedissonStreamProperty property) {
        RStream<String, String> stream = redisson.getStream(property.getStream(), StringCodec.INSTANCE);
        try {
            Collection<StreamGroup> groups = stream.listGroups();
            boolean groupExists = groups.stream()
                    .anyMatch(g -> g.getName().equals(property.getGroup()));

            if (!groupExists) {
                stream.createGroup(property.getGroup(), StreamMessageId.NEWEST);
                LOGGER.info("Created group [{}] for stream [{}]", property.getGroup(), property.getStream());
            } else {
                LOGGER.info("Stream group [{}] already exists", property.getGroup());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to check or create stream group [{}]", property.getGroup(), e);
        }
    }

    private void startConsuming(RedissonStreamProperty property) {
        CompletableFuture<?> task = CompletableFuture.runAsync(() -> consumeLoop(property));
        consumerTasks.put(property, task);
    }

    private void consumeLoop(RedissonStreamProperty property) {
        if (!isRunning) {
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
                if (isRunning) {
                    scheduleNextConsume(property);
                }
            }
        });
    }

    private void processMessages(
            RedissonStreamProperty property, RStream<String, String> stream, Map<StreamMessageId, Map<String, String>> messages
    ) {
        for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
            try {
                boolean shouldAck = handleMessage(property, entry);
                if (shouldAck) {
                    stream.ackAsync(property.getGroup(), entry.getKey())
                            .exceptionally(throwable -> {
                                LOGGER.error(
                                        "Failed to acknowledge message {} for group {}",
                                        entry.getKey(), property.getGroup(), throwable
                                );
                                return null;
                            });
                }
            } catch (Exception e) {
                LOGGER.error("Failed to process message {} for group {}",
                        entry.getKey(), property.getGroup(), e);
            }
        }
    }

    private boolean handleMessage(RedissonStreamProperty property, Map.Entry<StreamMessageId, Map<String, String>> entry) {
        Func2Args<StreamMessageId, Map<String, String>, Boolean> handler = handlerMap.getOrDefault(property, null);
        return invokeIfNotNull(handler, entry.getKey(), entry.getValue());
    }

    private void handleError(RedissonStreamProperty property, Throwable ex) {
        LOGGER.error("Error in consumeLoop for group {}: {}", property.getGroup(), ex.getMessage(), ex);
        if (isRunning) {
            CompletableFuture.delayedExecutor(ERROR_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                    .execute(() -> consumeLoop(property));
        }
    }

    private void scheduleNextConsume(RedissonStreamProperty property) {
        CompletableFuture.runAsync(() -> consumeLoop(property));
    }

    @PreDestroy
    public void stop() {
        isRunning = false;
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
