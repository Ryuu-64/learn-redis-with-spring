package top.ryuu64.redis.redisson.mq;

import lombok.Getter;
import lombok.Setter;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class StreamProducer {
    private final RedissonClient redisson;
    @Getter
    @Setter
    private int threshold = 10_000;

    public StreamProducer(RedissonClient redisson) {
        this.redisson = redisson;
    }

    public CompletableFuture<StreamMessageId> addAsync(String streamName, Map<String, String> messages) {
        RStream<String, String> stream = redisson.getStream(streamName, StringCodec.INSTANCE);
        StreamAddArgs<String, String> addArgs = StreamAddArgs.entries(messages);
        addArgs.noMakeStream()
                .trimNonStrict()
                .maxLen(threshold);
        return stream.addAsync(addArgs)
                .toCompletableFuture();
    }
}
