package top.ryuu64.learn.redis.redisson.domain;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@Builder
public class RedisStreamArgs {
    private String streamKey;
    private String groupName;
    private String consumer;
}
