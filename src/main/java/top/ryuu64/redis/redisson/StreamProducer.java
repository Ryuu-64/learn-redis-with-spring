package top.ryuu64.redis.redisson;

import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamAddArgs;
import org.springframework.stereotype.Service;
import top.ryuu64.redis.redisson.domain.ConsumerArgs;

import java.util.Map;

@Service
public class StreamProducer {
    private final RedissonClient redisson;

    public StreamProducer(RedissonClient redisson) {
        this.redisson = redisson;
    }

    public void addAsync(ConsumerArgs args, Map<String, String> map) {
        RStream<String, String> stream = redisson.getStream(args.getStreamName());
        StreamAddArgs<String, String> addArgs = StreamAddArgs.entries(map);
        addArgs.noMakeStream()
                .trimNonStrict()
                .maxLen(10_000);
        stream.addAsync(addArgs);
    }
}
