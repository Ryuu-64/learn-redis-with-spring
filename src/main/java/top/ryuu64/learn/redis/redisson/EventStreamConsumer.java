package top.ryuu64.learn.redis.redisson;

import top.ryuu64.learn.redis.redisson.domain.RedisStreamArgs;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.ryuu.functional.Action2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PreDestroy;

import java.util.*;
import java.util.concurrent.*;

public class EventStreamConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventStreamConsumer.class);
    private static final String DEAD_LETTER_STREAM_SUFFIX = ":dead_letter";
    public static final int READ_AND_PENDING_BATCH_SIZE = 10;
    public static final long READ_TIMEOUT_MS = 2000;
    public static final long ERROR_RETRY_DELAY_MS = 1000;
    public static final long PEL_IDLE_THRESHOLD_MS = 10_000;
    public static final int MAX_RETRY_COUNT = 3;

    private final RedissonClient redisson;
    private final Map<RedisStreamArgs, CompletableFuture<?>> consumerTasks = new ConcurrentHashMap<>();
    private volatile boolean isRunning = true;
    private final Map<RedisStreamArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap = new ConcurrentHashMap<>();

    public EventStreamConsumer(
            RedissonClient redisson, Map<RedisStreamArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap
    ) {
        this.redisson = redisson;

        this.handlerMap.putAll(handlerMap);
        Set<RedisStreamArgs> properties = handlerMap.keySet();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (RedisStreamArgs property : properties) {
            CompletableFuture<Void> future = ensureGroupExistsAsync(property, "");
            futures.add(future);
            future = ensureGroupExistsAsync(property, DEAD_LETTER_STREAM_SUFFIX);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        properties.forEach(this::startConsumerLoop);

    }

    private CompletableFuture<Void> ensureGroupExistsAsync(RedisStreamArgs property, String streamNameSuffix) {
        String streamName = property.getStreamName() + streamNameSuffix;
        long timeoutMillis = 30_000;
        long retryDelayMillis = 250;
        long startTime = System.currentTimeMillis();

        CompletableFuture<Void> future = new CompletableFuture<>();
        Runnable attempt = new Runnable() {
            @Override
            public void run() {
                try {
                    RStream<String, String> stream = redisson.getStream(streamName, StringCodec.INSTANCE);

                    if (!stream.isExists()) {
                        stream.createGroupAsync(property.getGroupName(), StreamMessageId.NEWEST);
                        LOGGER.info("Created by {}.", property);
                        scheduleRetry();
                        return;
                    }

                    boolean isGroupExists = stream.listGroups().stream()
                            .anyMatch(
                                    group -> group.getName().equals(property.getGroupName())
                            );
                    if (!isGroupExists) {
                        stream.createGroupAsync(property.getGroupName(), StreamMessageId.NEWEST);
                        LOGGER.info("Created by {}.", property);
                        scheduleRetry();
                        return;
                    }

                    LOGGER.info("Group exists for {}.", property);
                    future.complete(null);

                } catch (Exception e) {
                    LOGGER.warn("Retrying to create group {} for stream {} due to {}", property.getGroupName(), streamName, e.getMessage());
                    scheduleRetry();
                }
            }

            private void scheduleRetry() {
                if (System.currentTimeMillis() - startTime > timeoutMillis) {
                    future.completeExceptionally(new RuntimeException(
                            "Timeout: Failed to ensure group exists for " + property
                    ));
                } else {
                    CompletableFuture.delayedExecutor(retryDelayMillis, TimeUnit.MILLISECONDS)
                            .execute(this);
                }
            }
        };

        attempt.run();
        return future;
    }

    private void startConsumerLoop(RedisStreamArgs property) {
        CompletableFuture<?> task = CompletableFuture.runAsync(() -> consumeNextBatch(property));
        consumerTasks.put(property, task);
    }

    /** 消费下一批消息，包括新消息和 PEL 消息重试 */
    private void consumeNextBatch(RedisStreamArgs property) {
        if (!isRunning) {
            return;
        }

        RStream<String, String> stream = redisson.getStream(property.getStreamName(), StringCodec.INSTANCE);
        readMessages(stream, property);
        claimMessages(stream, property);
    }

    private void readMessages(RStream<String, String> stream, RedisStreamArgs property) {
        stream.readGroupAsync(
                        property.getGroupName(),
                        property.getConsumerName(),
                        READ_AND_PENDING_BATCH_SIZE,
                        READ_TIMEOUT_MS,
                        TimeUnit.MILLISECONDS,
                        StreamMessageId.NEVER_DELIVERED
                )
                .whenComplete(
                        (messages, throwable) -> handleBatch(property, stream, messages, throwable)
                );
    }

    /** 处理新消息或 claim 消息 */
    private void handleBatch(
            RedisStreamArgs property,
            RStream<String, String> stream,
            Map<StreamMessageId, Map<String, String>> messages,
            Throwable throwable
    ) {
        if (throwable != null) {
            scheduleRetryOnError(property, throwable);
            return;
        }

        try {
            if (messages != null && !messages.isEmpty()) {
                processAndAckMessages(property, stream, messages, false);
            }
        } catch (Exception e) {
            LOGGER.error("Error processing messages for group {}", property.getGroupName(), e);
        } finally {
            if (isRunning) scheduleNextBatch(property);
        }
    }

    /** 处理 PEL 消息（claim idle 消息并重试） */
    private void claimMessages(RStream<String, String> stream, RedisStreamArgs property) {
        // 异步获取 idle 超过阈值的 PEL 消息
        stream.pendingRangeAsync(
                property.getGroupName(),
                StreamMessageId.MIN,
                StreamMessageId.MAX,
                PEL_IDLE_THRESHOLD_MS,
                TimeUnit.MILLISECONDS,
                READ_AND_PENDING_BATCH_SIZE
        ).whenComplete((pendingMessages, ex) -> {
            if (ex != null) {
                LOGGER.error("Failed to fetch pending messages for group {}", property.getGroupName(), ex);
                return;
            }

            if (pendingMessages == null || pendingMessages.isEmpty()) {
                return; // 没有待处理消息
            }

            // claim 消息给当前 consumer
            StreamMessageId[] idsToClaim = pendingMessages.keySet().toArray(new StreamMessageId[0]);
            stream.claimAsync(
                            property.getGroupName(), property.getConsumerName(), PEL_IDLE_THRESHOLD_MS, TimeUnit.MILLISECONDS, idsToClaim
                    )
                    .whenComplete((claimedMessages, claimEx) -> {
                        if (claimEx != null) {
                            LOGGER.error("Failed to claim PEL messages for group {}", property.getGroupName(), claimEx);
                            return;
                        }

                        if (claimedMessages != null && !claimedMessages.isEmpty()) {
                            for (Map.Entry<StreamMessageId, Map<String, String>> entry : claimedMessages.entrySet()) {
                                Map<String, String> value = entry.getValue();
                                String retryCount = value.get("retryCount");
                                if (retryCount != null) {
                                    int i = Integer.parseInt(retryCount) + 1;
                                    value.put("retryCount", String.valueOf(i));
                                } else {
                                    value.put("retryCount", "1");
                                }

                                StreamAddArgs<String, String> args = StreamAddArgs.entries(value);
                                stream.addAsync(args).toCompletableFuture().join();
                            }

                            processAndAckMessages(property, stream, claimedMessages, true);
                        }
                    });
        });
    }

    /** 处理消息并 ack； isRetry=true 表示 PEL 消息 */
    private void processAndAckMessages(
            RedisStreamArgs property,
            RStream<String, String> stream,
            Map<StreamMessageId, Map<String, String>> messages,
            boolean isRetry
    ) {
        for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
            StreamMessageId msgId = entry.getKey();
            Map<String, String> body = entry.getValue();
            try {
                int retryCount = body.getOrDefault("retryCount", "0").isEmpty() ?
                        0 :
                        Integer.parseInt(body.getOrDefault("retryCount", "0"));

                if (isRetry && retryCount >= MAX_RETRY_COUNT) {
                    // 超过最大重试次数 → 移到死信队列
                    moveToDeadLetter(property, msgId, body);
                    stream.ackAsync(property.getGroupName(), msgId);
                    continue;
                }

                Action2Args<StreamMessageId, Map<String, String>> handler = handlerMap.get(property);
                if (handler != null) {
                    try {
                        handler.invoke(msgId, body);
                    } catch (Exception e) {
                        LOGGER.error("Failed to handle message {} for group {}", msgId, property.getGroupName(), e);
                    } finally {
                        stream.ackAsync(property.getGroupName(), msgId);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Failed to process message {} for group {}", msgId, property.getGroupName(), e);
            }
        }
    }

    /** 将消息移到死信队列 */
    private void moveToDeadLetter(RedisStreamArgs property, StreamMessageId msgId, Map<String, String> body) {
        String deadLetterStream = property.getStreamName() + DEAD_LETTER_STREAM_SUFFIX;
        RStream<String, String> deadStream = redisson.getStream(deadLetterStream, StringCodec.INSTANCE);

        // 在消息体里记录原消息信息
        Map<String, String> deadMessage = new HashMap<>(body);
        deadMessage.put("originalStream", property.getStreamName());
        deadMessage.put("originalId", msgId.toString());
        StreamAddArgs<String, String> args = StreamAddArgs.entries(deadMessage);
        deadStream.addAsync(args)
                .exceptionally(ex -> {
                    LOGGER.error("Failed to move message {} to dead letter stream {}", msgId, deadLetterStream, ex);
                    return null;
                });

        LOGGER.warn("Message {} moved to dead letter stream {}", msgId, deadLetterStream);
    }

    private void scheduleRetryOnError(RedisStreamArgs property, Throwable throwable) {
        LOGGER.error("Error in consumeLoop for group {}: {}", property.getGroupName(), throwable.getMessage(), throwable);
        if (isRunning) {
            CompletableFuture.delayedExecutor(ERROR_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)
                    .execute(() -> consumeNextBatch(property));
        }
    }

    private void scheduleNextBatch(RedisStreamArgs property) {
        CompletableFuture.runAsync(() -> consumeNextBatch(property));
    }

    @PreDestroy
    public void stop() {
        isRunning = false;
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(
                consumerTasks.values().toArray(new CompletableFuture[0])
        );

        try {
            allTasks.get(5, TimeUnit.SECONDS);
            LOGGER.info("EventStreamConsumer gracefully stopped");
        } catch (Exception e) {
            LOGGER.warn("Some tasks did not complete in time during shutdown", e);
        }
    }
}
