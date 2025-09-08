package top.ryuu64.redis.controller;

import org.springframework.web.bind.annotation.*;
import top.ryuu64.redis.producer.RedisStreamProducer;

import java.util.Map;

@RestController
@RequestMapping("/streams")
public class MessageController {

    private final RedisStreamProducer producer;

    public MessageController(RedisStreamProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/{stream}")
    public String send(@PathVariable String stream, @RequestBody Map<String, String> payload) {
        return producer.send(stream, payload);
    }
}
