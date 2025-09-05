package top.ryuu64.learn.redis.redisson.domain;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@Builder(toBuilder = true)
public class RedisStreamArgs {
    private String streamName;
    private String groupName;
    private String consumerName;
}
