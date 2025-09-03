package com.example.demo.consumer;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
public abstract class RedisStreamConsumer implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisStreamConsumer.class);
    private static final Duration PENDING_CHECK_INTERVAL = Duration.ofSeconds(30);
    private static final Duration PENDING_MESSAGE_IDLE_TIME = Duration.ofSeconds(60);

    private final StringRedisTemplate template;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean running = true;

    public RedisStreamConsumer(
            StringRedisTemplate template
    ) {
        this.template = template;
    }


    @Override
    public void run(ApplicationArguments args) {
        tryCreateGroup();
        start();
    }

    protected abstract String getStream();

    protected abstract String getGroup();

    private String getConsumer() {
        return "consumer";
    }

    private void tryCreateGroup() {
        String stream = getStream();
        String group = getGroup();
        try {
            template.opsForStream().createGroup(stream, group);
            LOGGER.info("Created group {}", group);
        } catch (Exception ignored) {
            LOGGER.info("Group {} already exists", group);
        }
    }

    private void start() {
        executor.submit(this::consumeLoop);
        scheduler.scheduleWithFixedDelay(
                this::pendingLoop,
                PENDING_CHECK_INTERVAL.toSeconds(),
                PENDING_CHECK_INTERVAL.toSeconds(),
                TimeUnit.SECONDS
        );
    }

    private void consumeLoop() {
        String group = getGroup();
        String consumer = getConsumer();
        String stream = getStream();
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                List<MapRecord<String, Object, Object>> messages = template.opsForStream()
                        .read(
                                Consumer.from(group, consumer),
                                StreamReadOptions.empty().block(Duration.ofSeconds(2)).count(10),
                                StreamOffset.create(stream, ReadOffset.lastConsumed())
                        );

                if (messages != null) {
                    for (MapRecord<String, Object, Object> msg : messages) {
                        handleMessage(msg);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("消费异常", e);
            }
        }
    }

    private void pendingLoop() {
        String group = getGroup();
        String consumer = getConsumer();
        String stream = getStream();
        if (!running || Thread.currentThread().isInterrupted()) return;

        try {
            PendingMessages pending = template.opsForStream()
                    .pending(stream, group, Range.unbounded(), 100);

            if (pending.isEmpty()) return;

            // 批量 claim
            RecordId[] ids = pending.stream()
                    .map(PendingMessage::getId)
                    .toArray(RecordId[]::new);

            List<MapRecord<String, Object, Object>> claimed = template.opsForStream()
                    .claim(stream, group, consumer, PENDING_MESSAGE_IDLE_TIME, ids);

            if (!claimed.isEmpty()) {
                for (MapRecord<String, Object, Object> msg : claimed) {
                    LOGGER.info("Claim 到未确认消息: {}", msg.getValue());
                    handleMessage(msg);
                }
            }

        } catch (Exception e) {
            LOGGER.error("Pending 补偿异常", e);
        }
    }

    private void handleMessage(MapRecord<String, Object, Object> msg) {
        String group = getGroup();
        String stream = getStream();
        try {
            LOGGER.debug("收到消息: {}", msg.getValue());
            // TODO: 业务逻辑处理
            template.opsForStream().acknowledge(stream, group, msg.getId());
        } catch (Exception ex) {
            LOGGER.error("消息处理失败: {}", msg, ex);
            // TODO: 可写死信队列
        }
    }

    @PreDestroy
    public void stop() {
        String consumer = getConsumer();
        running = false;
        executor.shutdownNow();
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warn("Scheduler did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("Stopped consumer {}", consumer);
    }
}
