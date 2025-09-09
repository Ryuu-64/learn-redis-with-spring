package top.ryuu64.redis.redisson.mq;

import lombok.Getter;
import lombok.Setter;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.api.*;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.ryuu.functional.Action2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class StreamConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamConsumer.class);
    private final RedissonClient redisson;
    @Getter
    @Setter
    private String deadLetterSuffix = ":dead_letter";
    @Getter
    @Setter
    private int batchSize = 50;
    @Getter
    @Setter
    private long timeout = 2_000;
    private final Map<MQArgs, ConsumerRunner> consumerMap = new ConcurrentHashMap<>();

    public StreamConsumer(RedissonClient redisson) {
        this.redisson = redisson;
    }

    public CompletableFuture<Void> startBatchAsync(
            Map<MQArgs, Action2Args<StreamMessageId, Map<String, String>>> newHandlers
    ) {
        List<CompletableFuture<Void>> initFutures = new ArrayList<>();
        List<ConsumerRunner> consumersToStart = new ArrayList<>();
        for (MQArgs args : newHandlers.keySet()) {
            consumerMap.compute(args, (existingArgs, existingRunner) -> {
                if (existingRunner != null) {
                    LOGGER.warn("Duplicate key exists for key {}", args);
                    return existingRunner;
                } else {
                    ConsumerRunner runner = ConsumerRunner.builder()
                            .args(args)
                            .handler(newHandlers.get(args))
                            .isRunning(new AtomicBoolean(true))
                            .build();
                    consumersToStart.add(runner);
                    return runner;
                }
            });
            initFutures.add(tryCreateGroupAsync(args, ""));
            initFutures.add(tryCreateGroupAsync(args, deadLetterSuffix));
        }
        return CompletableFuture.allOf(initFutures.toArray(new CompletableFuture[0]))
                .thenRunAsync(() -> {
                    LOGGER.info("All groups initialized, starting consumer loops.");
                    for (ConsumerRunner runner : consumersToStart) {
                        runner.startAsync(this::startNextAsync);
                    }
                });
    }

    public List<CompletableFuture<Void>> stopBatchAsync(List<MQArgs> argsList) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (MQArgs args : argsList) {
            ConsumerRunner ctx = consumerMap.getOrDefault(args, null);
            CompletableFuture<Void> future = ctx.stopAsync();
            futures.add(future);
        }
        return futures;
    }

    public CompletableFuture<Void> stopAllAsync() {
        List<CompletableFuture<Void>> stopFutures = new ArrayList<>();
        consumerMap.values().forEach(runner -> {
            CompletableFuture<Void> stopFuture = runner.stopAsync();
            stopFutures.add(stopFuture);
        });

        return CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture[0]));
    }

    private CompletableFuture<Void> tryCreateGroupAsync(MQArgs args, String streamNameSuffix) {
        MQArgs targetArgs = args.toBuilder().build();
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

    private static Void handleCreateThrowable(Throwable throwable, MQArgs targetArgs) {
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

    private void internalStartNextAsync(MQArgs args) {
        ConsumerRunner runner = consumerMap.get(args);
        if (runner == null) {
            return;
        }

        RStream<String, String> stream = redisson.getStream(args.getStreamName(), StringCodec.INSTANCE);
        readMessages(stream, args);
        claimMessages(stream, args);
        startNextAsync(args);
    }

    private void startNextAsync(MQArgs args) {
        ConsumerRunner runner = consumerMap.get(args);
        if (runner == null) {
            LOGGER.warn("No consumer for args {}", args);
            return;
        }

        runner.startAsync(this::internalStartNextAsync);
    }

    private void readMessages(RStream<String, String> stream, MQArgs args) {
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
    private void claimMessages(RStream<String, String> stream, MQArgs args) {
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
            MQArgs args, RStream<String, String> stream,
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
        }
    }

    private void processAndAckMessages(
            MQArgs args,
            RStream<String, String> stream,
            Map<StreamMessageId, Map<String, String>> messages
    ) {
        for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
            StreamMessageId msgId = entry.getKey();
            Map<String, String> body = entry.getValue();
            ConsumerRunner runner = consumerMap.getOrDefault(args, null);
            try {
                runner.invoke(msgId, body);
            } catch (Exception e) {
                LOGGER.error("Failed to handle message {}, moving to dead letter", msgId, e);
                moveToDeadLetter(args, msgId, body);
            } finally {
                stream.ackAsync(args.getGroupName(), msgId);
            }
        }
    }

    /** 将消息移到死信队列 */
    private void moveToDeadLetter(MQArgs args, StreamMessageId msgId, Map<String, String> body) {
        Map<String, String> deadMessage = new HashMap<>(body);
        deadMessage.put("originalStream", args.getStreamName());
        deadMessage.put("originalId", msgId.toString());
        String deadLetterStream = args.getStreamName() + deadLetterSuffix;
        RStream<String, String> deadStream = redisson.getStream(deadLetterStream, StringCodec.INSTANCE);
        StreamAddArgs<String, String> addArgs = StreamAddArgs.entries(deadMessage);
        addArgs.trimNonStrict().maxLen(10_000);
        deadStream.addAsync(addArgs)
                .exceptionally(throwable -> {
                    LOGGER.error("Failed to move message {} to dead letter stream {}", msgId, deadLetterStream, throwable);
                    return null;
                });
        LOGGER.warn("Message {} moved to dead letter stream {}", msgId, deadLetterStream);
    }
}
