package dev.lidar.tagstream.nats;

import dev.lidar.tagstream.TagStreamProperties;
import dev.lidar.tagstream.tag.TagBatch;
import dev.lidar.tagstream.tag.TagParser;
import dev.lidar.tagstream.ws.TagWebSocketHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * ISL Engine 이 태그 변경을 뿌리는 NATS 주제를 일반 구독한다.
 *
 * 일반 구독이라 붙기 전 메시지는 받지 못하고 재전달도 없다. 실시간 화면용이지 적재용이 아니다.
 * 연결은 백그라운드에서 맺고 끊기면 계속 다시 붙는다 — Aspire 가 늦게 떠도 앱 기동은 막지 않는다.
 * 대신 연결 상태는 /actuator/health 의 nats 항목으로 드러낸다.
 */
@Component("nats")
public class NatsTagSubscriber implements SmartLifecycle, HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(NatsTagSubscriber.class);

    private final TagStreamProperties.Nats props;
    private final TagParser parser;
    private final TagWebSocketHandler sink;
    private final Counter messages;
    private final Counter tags;
    private final Counter parseFailures;

    private volatile boolean running;
    private volatile Connection connection;
    private volatile Instant lastMessageAt;

    public NatsTagSubscriber(TagStreamProperties props, TagParser parser, TagWebSocketHandler sink,
                             MeterRegistry registry) {
        this.props = props.nats();
        this.parser = parser;
        this.sink = sink;
        this.messages = registry.counter("tagstream.nats.messages");
        this.tags = registry.counter("tagstream.nats.tags");
        this.parseFailures = registry.counter("tagstream.nats.parse.failures");
    }

    @Override
    public void start() {
        Options options = new Options.Builder()
                .server(props.url())
                .connectionName("nats-tag-stream")
                .maxReconnects(-1)
                .reconnectWait(Duration.ofSeconds(2))
                .connectionListener(this::onConnectionEvent)
                .build();
        try {
            // 첫 연결도 재시도하며 비동기로 맺는다. 연결되면 CONNECTED 이벤트에서 구독을 건다.
            Nats.connectAsynchronously(options, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        running = true;
        log.info("NATS 연결 시작 {} subject={}", redact(props.url()), props.subject());
    }

    private void onConnectionEvent(Connection conn, ConnectionListener.Events event) {
        log.info("NATS {}", event);
        if (event == ConnectionListener.Events.CONNECTED) {
            connection = conn;
            // 재연결(RECONNECTED) 때는 jnats 가 구독을 되살린다. 처음 한 번만 건다.
            Dispatcher dispatcher = conn.createDispatcher(this::onMessage);
            dispatcher.subscribe(props.subject());
        }
    }

    private void onMessage(Message msg) {
        Instant receivedAt = Instant.now();
        lastMessageAt = receivedAt;
        messages.increment();
        TagBatch batch;
        try {
            batch = parser.parse(msg.getSubject(), msg.getData(), receivedAt);
        } catch (Exception e) {
            parseFailures.increment();
            log.warn("NATS 메시지 파싱 실패 subject={} bytes={}: {}",
                    msg.getSubject(), msg.getData().length, e.toString());
            return;
        }
        tags.increment(batch.tags().size());
        sink.broadcast(batch);
    }

    @Override
    public void stop() {
        Connection conn = connection;
        if (conn != null) {
            try {
                conn.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        connection = null;
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public Health health() {
        Connection conn = connection;
        Connection.Status status = conn == null ? Connection.Status.DISCONNECTED : conn.getStatus();
        Health.Builder builder = status == Connection.Status.CONNECTED ? Health.up() : Health.down();
        return builder
                .withDetail("status", status.name())
                .withDetail("server", redact(props.url()))
                .withDetail("subject", props.subject())
                .withDetail("lastMessageAt", lastMessageAt == null ? "-" : lastMessageAt.toString())
                .build();
    }

    /** nats://user:pass@host:port → nats://user:***@host:port */
    private static String redact(String url) {
        return url.replaceAll("://([^:/@]+):[^@]*@", "://$1:***@");
    }
}
