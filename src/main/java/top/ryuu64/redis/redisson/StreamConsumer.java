package top.ryuu64.redis.redisson;

import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import top.ryuu64.redis.redisson.domain.ConsumerArgs;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.ryuu.functional.Action2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class StreamConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamConsumer.class);
    private static final String DEAD_LETTER_STREAM_SUFFIX = ":dead_letter";
    public static final int READ_AND_PENDING_BATCH_SIZE = 10;
    public static final long READ_TIMEOUT_MS = 2000;
    public static final long PEL_IDLE_THRESHOLD_MS = 10_000;

    private final RedissonClient redisson;
    private final Map<ConsumerArgs, CompletableFuture<?>> consumerTasks = new ConcurrentHashMap<>();
    private volatile boolean isRunning = true;
    private final Map<ConsumerArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap = new ConcurrentHashMap<>();

    public StreamConsumer(
            RedissonClient redisson, Map<ConsumerArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap
    ) {
        this.redisson = redisson;
        this.handlerMap.putAll(handlerMap);
    }

    public CompletableFuture<Void> startAsync() {
        Set<ConsumerArgs> argsList = handlerMap.keySet();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ConsumerArgs args : argsList) {
            futures.add(tryCreateGroupAsync(args, ""));
            futures.add(tryCreateGroupAsync(args, DEAD_LETTER_STREAM_SUFFIX));
        }
        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .thenRunAsync(() -> {
                    LOGGER.info("All groups initialized, starting consumer loops.");
                    argsList.forEach(this::startConsumerLoopAsync);
                });
    }

    public CompletableFuture<Void> stopAsync() {
        isRunning = false;
        return CompletableFuture.allOf(consumerTasks.values().toArray(new CompletableFuture[0]));
    }

    private CompletableFuture<Void> tryCreateGroupAsync(ConsumerArgs args, String streamNameSuffix) {
        ConsumerArgs targetArgs = args.toBuilder().build();
        targetArgs.setStreamName(targetArgs.getStreamName() + streamNameSuffix);
        RStream<String, String> stream = redisson.getStream(targetArgs.getStreamName(), StringCodec.INSTANCE);
        StreamCreateGroupArgs createGroupArgs = StreamCreateGroupArgs
                .name(targetArgs.getGroupName())
                .id(StreamMessageId.NEWEST)
                .makeStream()
                .entriesRead(0);
        try {
            return stream
                    .createGroupAsync(createGroupArgs)
                    .toCompletableFuture()
                    .exceptionallyAsync(
                            throwable -> handleCreateThrowable(throwable, targetArgs)
                    ).thenCompose(result -> stream
                            .createConsumerAsync(
                                    targetArgs.getGroupName(), targetArgs.getConsumerName()
                            )
                            .toCompletableFuture()
                            .exceptionallyAsync(
                                    throwable -> handleCreateThrowable(throwable, targetArgs)
                            )
                    );
        } catch (Exception e) {
            LOGGER.error("Unexpected error during creation for {}", targetArgs, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private static Void handleCreateThrowable(Throwable throwable, ConsumerArgs targetArgs) {
        if (
                throwable instanceof org.redisson.client.RedisException &&
                        throwable.getMessage() != null &&
                        throwable.getMessage().contains("BUSYGROUP Consumer Group name already exists")
        ) {
            LOGGER.info(
                    "Group already exists for args '{}'. This is expected.", targetArgs
            );
            return null;
        }

        LOGGER.error(
                "Unexpected error during group/consumer creation for stream: {}, group: {}",
                targetArgs.getStreamName(), targetArgs.getGroupName(),
                throwable
        );
        throw new CompletionException(throwable);
    }

    private void startConsumerLoopAsync(ConsumerArgs args) {
        CompletableFuture<?> task = CompletableFuture.runAsync(() -> consumeNextBatch(args));
        consumerTasks.put(args, task);
    }

    /** 消费下一批消息，包括新消息和 PEL 消息重试 */
    private void consumeNextBatch(ConsumerArgs args) {
        if (!isRunning) {
            return;
        }

        RStream<String, String> stream = redisson.getStream(args.getStreamName(), StringCodec.INSTANCE);
        readMessages(stream, args);
        claimMessages(stream, args);
    }

    /** 处理 PEL 消息（claim idle 消息并重试） */
    private void claimMessages(RStream<String, String> stream, ConsumerArgs args) {
        stream.pendingRangeAsync(
                args.getGroupName(),
                StreamMessageId.MIN,
                StreamMessageId.MAX,
                PEL_IDLE_THRESHOLD_MS,
                TimeUnit.MILLISECONDS,
                READ_AND_PENDING_BATCH_SIZE
        ).whenComplete((pendingMessages, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Failed to fetch pending messages for args {}", args, throwable);
                return;
            }

            if (pendingMessages == null || pendingMessages.isEmpty()) {
                return;
            }

            // 直接移动到死信队列并 ACK
            for (Map.Entry<StreamMessageId, Map<String, String>> entry : pendingMessages.entrySet()) {
                StreamMessageId msgId = entry.getKey();
                Map<String, String> body = entry.getValue();
                moveToDeadLetter(args, msgId, body);
                stream.ackAsync(args.getGroupName(), msgId);
            }
        });
    }

    private void readMessages(RStream<String, String> stream, ConsumerArgs args) {
        Map<StreamMessageId, Map<String, String>> messages = stream.readGroup(
                args.getGroupName(),
                args.getConsumerName(),
                StreamReadGroupArgs
                        .greaterThan(StreamMessageId.NEVER_DELIVERED)
                        .count(READ_AND_PENDING_BATCH_SIZE)
                        .timeout(Duration.ofMillis(READ_TIMEOUT_MS))
        );
        handleBatch(args, stream, messages);
    }

    private void handleBatch(
            ConsumerArgs args, RStream<String, String> stream,
            Map<StreamMessageId, Map<String, String>> messages
    ) {
        try {
            if (messages == null || messages.isEmpty()) {
                LOGGER.debug("No messages found for args {}", args);
                return;
            }

            processAndAckMessages(args, stream, messages);
        } catch (Exception e) {
            LOGGER.error("Error processing messages for group {}", args.getGroupName(), e);
        } finally {
            if (isRunning) {
                scheduleNextBatch(args);
            }
        }
    }

    private void processAndAckMessages(
            ConsumerArgs args,
            RStream<String, String> stream,
            Map<StreamMessageId, Map<String, String>> messages
    ) {
        for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
            StreamMessageId msgId = entry.getKey();
            Map<String, String> body = entry.getValue();
            Action2Args<StreamMessageId, Map<String, String>> handler = handlerMap.get(args);
            try {
                if (handler != null) {
                    handler.invoke(msgId, body);
                } else {
                    LOGGER.debug("No handler for args {}", args);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to handle message {}, moving to dead letter", msgId, e);
                moveToDeadLetter(args, msgId, body);
            } finally {
                stream.ackAsync(args.getGroupName(), msgId);
            }
        }
    }

    /** 将消息移到死信队列 */
    private void moveToDeadLetter(ConsumerArgs args, StreamMessageId msgId, Map<String, String> body) {
        String deadLetterStream = args.getStreamName() + DEAD_LETTER_STREAM_SUFFIX;
        RStream<String, String> deadStream = redisson.getStream(deadLetterStream, StringCodec.INSTANCE);

        // 在消息体里记录原消息信息
        Map<String, String> deadMessage = new HashMap<>(body);
        deadMessage.put("originalStream", args.getStreamName());
        deadMessage.put("originalId", msgId.toString());
        StreamAddArgs<String, String> addArgs = StreamAddArgs.entries(deadMessage);
        deadStream.addAsync(addArgs)
                .exceptionally(ex -> {
                    LOGGER.error("Failed to move message {} to dead letter stream {}", msgId, deadLetterStream, ex);
                    return null;
                });

        LOGGER.warn("Message {} moved to dead letter stream {}", msgId, deadLetterStream);
    }

    private void scheduleNextBatch(ConsumerArgs args) {
        CompletableFuture.runAsync(() -> consumeNextBatch(args));
    }
}
