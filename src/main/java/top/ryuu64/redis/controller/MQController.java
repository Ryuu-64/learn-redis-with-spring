package top.ryuu64.redis.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import top.ryuu64.redis.redisson.mq.StreamProducer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/streams")
public class MQController {
    private static final Logger LOGGER = LoggerFactory.getLogger(MQController.class);
    private final StreamProducer producer;

    public MQController(StreamProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/{stream}")
    public String send(@PathVariable String stream, @RequestBody Map<String, String> payload) {
        try {
            return producer.addAsync(stream, payload).get(5, TimeUnit.SECONDS).toString();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.warn(e.getMessage(), e);
        }

        return "";
    }
}
