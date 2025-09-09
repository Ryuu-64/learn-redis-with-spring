package top.ryuu64.redis.redisson.mq;

import lombok.Builder;
import lombok.ToString;
import org.redisson.api.StreamMessageId;
import org.ryuu.functional.Action1Arg;
import org.ryuu.functional.Action2Args;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Builder
@ToString
public class ConsumerRunner {
    private final MQArgs args;
    private final Action2Args<StreamMessageId, Map<String, String>> handler;
    private final AtomicBoolean isRunning;
    private CompletableFuture<Void> runningFuture;

    public void startAsync(Action1Arg<MQArgs> startNextConsumerAsync) {
        if (!isRunning.get()) {
            return;
        }

        runningFuture = CompletableFuture.runAsync(() -> startNextConsumerAsync.invoke(args));
    }

    public void invoke(StreamMessageId msgId, Map<String, String> body) {
        handler.invoke(msgId, body);
    }

    public CompletableFuture<Void> stopAsync() {
        if (isRunning == null) {
            return CompletableFuture.failedFuture(
                    new Exception("Consumer is not properly initialized for args: " + args)
            );
        }

        if (runningFuture == null) {
            return CompletableFuture.failedFuture(
                    new Exception("Consumer is not properly initialized for args: " + args)
            );
        }

        isRunning.set(false);
        return runningFuture;
    }
}
