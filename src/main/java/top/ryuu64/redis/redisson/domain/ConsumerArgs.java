package top.ryuu64.redis.redisson.domain;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@Builder(toBuilder = true)
public class ConsumerArgs {
    private String streamName;
    private String groupName;
    private String consumerName;
}
