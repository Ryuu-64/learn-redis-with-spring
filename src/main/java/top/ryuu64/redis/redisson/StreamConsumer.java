package top.ryuu64.redis.redisson;

import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.ryuu.functional.Action2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.ryuu64.redis.redisson.domain.ConsumerArgs;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class StreamConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamConsumer.class);
    private final RedissonClient redisson;
    private final String deadLetterSuffix;
    private final int batchSize;
    private final long timeout;
    private final Map<ConsumerArgs, CompletableFuture<?>> futureMap = new ConcurrentHashMap<>();
    private final Map<ConsumerArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap = new ConcurrentHashMap<>();
    private volatile boolean isRunning = true;

    public StreamConsumer(RedissonClient redisson) {
        this.redisson = redisson;
        this.deadLetterSuffix = ":dead_letter";
        this.batchSize = 50;
        this.timeout = 2_000;
    }

    public StreamConsumer(
            RedissonClient redisson, String deadLetterSuffix, int batchSize, long timeout
    ) {
        this.redisson = redisson;
        this.deadLetterSuffix = deadLetterSuffix;
        this.batchSize = batchSize;
        this.timeout = timeout;
    }

    public CompletableFuture<Void> startAsync(
            Map<ConsumerArgs, Action2Args<StreamMessageId, Map<String, String>>> handlerMap
    ) {
        this.handlerMap.putAll(handlerMap);
        Set<ConsumerArgs> argsList = handlerMap.keySet();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ConsumerArgs args : argsList) {
            futures.add(tryCreateGroupAsync(args, ""));
            futures.add(tryCreateGroupAsync(args, deadLetterSuffix));
        }
        return CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0]))
                .thenRunAsync(() -> {
                    LOGGER.info("All groups initialized, starting consumer loops.");
                    argsList.forEach(this::startNextConsumerAsync);
                });
    }

    public CompletableFuture<Void> stopAsync() {
        isRunning = false;
        return CompletableFuture.allOf(futureMap.values().toArray(new CompletableFuture[0]));
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

    /** 消费下一批消息，包括新消息和 PEL 消息重试 */
    private void consumeNextBatch(ConsumerArgs args) {
        if (!isRunning) {
            return;
        }

        RStream<String, String> stream = redisson.getStream(args.getStreamName(), StringCodec.INSTANCE);
        readMessages(stream, args);
        claimMessages(stream, args);
    }

    private void readMessages(RStream<String, String> stream, ConsumerArgs args) {
        Map<StreamMessageId, Map<String, String>> groupMessages = stream.readGroup(
                args.getGroupName(),
                args.getConsumerName(),
                StreamReadGroupArgs
                        .greaterThan(StreamMessageId.NEVER_DELIVERED)
                        .count(batchSize)
                        .timeout(Duration.ofMillis(timeout))
        );
        handleGroupMessages(args, stream, groupMessages);
    }

    /** 处理 PEL 消息（claim idle 消息并重试） */
    private void claimMessages(RStream<String, String> stream, ConsumerArgs args) {
        stream.pendingRangeAsync(
                args.getGroupName(),
                StreamMessageId.MIN,
                StreamMessageId.MAX,
                timeout,
                TimeUnit.MILLISECONDS,
                batchSize
        ).whenComplete((pendingMessages, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Failed to fetch pending messages for args {}", args, throwable);
                return;
            }

            if (pendingMessages == null || pendingMessages.isEmpty()) {
                return;
            }

            for (Map.Entry<StreamMessageId, Map<String, String>> entry : pendingMessages.entrySet()) {
                StreamMessageId msgId = entry.getKey();
                Map<String, String> body = entry.getValue();
                moveToDeadLetter(args, msgId, body);
                stream.ackAsync(args.getGroupName(), msgId);
            }
        });
    }

    private void handleGroupMessages(
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
                startNextConsumerAsync(args);
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

    private void startNextConsumerAsync(ConsumerArgs args) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> consumeNextBatch(args));
        futureMap.put(args, future);
    }

    /** 将消息移到死信队列 */
    private void moveToDeadLetter(ConsumerArgs args, StreamMessageId msgId, Map<String, String> body) {
        Map<String, String> deadMessage = new HashMap<>(body);
        deadMessage.put("originalStream", args.getStreamName());
        deadMessage.put("originalId", msgId.toString());
        String deadLetterStream = args.getStreamName() + deadLetterSuffix;
        RStream<String, String> deadStream = redisson.getStream(deadLetterStream, StringCodec.INSTANCE);
        StreamAddArgs<String, String> addArgs = StreamAddArgs.entries(deadMessage);
        addArgs.trimNonStrict().maxLen(10_000);
        deadStream.addAsync(addArgs)
                .exceptionally(ex -> {
                    LOGGER.error("Failed to move message {} to dead letter stream {}", msgId, deadLetterStream, ex);
                    return null;
                });
        LOGGER.warn("Message {} moved to dead letter stream {}", msgId, deadLetterStream);
    }
}
