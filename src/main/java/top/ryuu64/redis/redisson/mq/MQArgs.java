package top.ryuu64.redis.redisson.mq;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
@Builder(toBuilder = true)
public class MQArgs {
    private String streamName;
    private String groupName;
    private String consumerName;
}
