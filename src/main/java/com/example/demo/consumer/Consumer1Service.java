package com.example.demo.consumer;

import com.example.demo.config.RedissonStreamProperty;
import org.redisson.api.StreamMessageId;
import org.ryuu.functional.Action2Args;
import org.ryuu.functional.Func2Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class Consumer1Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer1Service.class);

    public Consumer1Service(AsyncRedissonStreamConsumerService service) {
        tryAddHandlerForStream1Group6(service);
    }

    private void tryAddHandlerForStream1Group6(AsyncRedissonStreamConsumerService service) {
        RedissonStreamProperty property = new RedissonStreamProperty();
        property.setStream("learn_redis:redisson:stream:1");
        property.setGroup("redisson-group-6");
        property.setConsumer("consumer");

        Func2Args<StreamMessageId, Map<String, String>, Boolean> handler = (key, value) -> {
            LOGGER.debug("property={}, key={}, value={}", property, key, value);
            return true;
        };

        service.addHandler(property, handler);
    }
}
