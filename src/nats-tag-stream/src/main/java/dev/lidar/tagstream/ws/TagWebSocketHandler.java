package dev.lidar.tagstream.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.lidar.tagstream.TagStreamProperties;
import dev.lidar.tagstream.tag.Tag;
import dev.lidar.tagstream.tag.TagBatch;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 태그 묶음을 접속한 클라이언트에게 밀어 준다. 클라이언트 → 서버 메시지는 쓰지 않는다.
 *
 * 프레임 하나 = NATS 메시지 하나(약 1초치)를 그 세션의 필터로 거른 것:
 * <pre>
 * {"type":"tags","subject":"…","edgeGroupId":"…","receivedAt":"…","count":3,"tags":[{…},…]}
 * </pre>
 * 거른 결과가 비면 보내지 않는다.
 *
 * 세션마다 가상 스레드로 보낸다. NATS 디스패처 스레드가 느린 클라이언트 하나에 묶이지 않게 하려는 것이다.
 * 동시 전송은 ConcurrentWebSocketSessionDecorator 가 줄 세우고, 시간 · 버퍼 한도를 넘으면 그 세션만 끊는다.
 */
@Component
public class TagWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(TagWebSocketHandler.class);

    private record Client(WebSocketSession session, TagFilter filter) {
    }

    private final Map<String, Client> clients = new ConcurrentHashMap<>();
    private final ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor();
    private final ObjectMapper mapper;
    private final TagStreamProperties.Ws props;
    private final Counter framesSent;
    private final Counter sendFailures;

    public TagWebSocketHandler(ObjectMapper mapper, TagStreamProperties props, MeterRegistry registry) {
        this.mapper = mapper;
        this.props = props.ws();
        this.framesSent = registry.counter("tagstream.ws.frames.sent");
        this.sendFailures = registry.counter("tagstream.ws.send.failures");
        registry.gaugeMapSize("tagstream.ws.sessions", List.of(), clients);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        TagFilter filter = TagFilter.from(session.getUri());
        var safe = new ConcurrentWebSocketSessionDecorator(
                session, props.sendTimeLimitMs(), props.bufferSizeLimitBytes());
        clients.put(session.getId(), new Client(safe, filter));
        log.info("WS 접속 {} {} filter={}", session.getId(), session.getRemoteAddress(), filter);

        Map<String, Object> hello = new LinkedHashMap<>();
        hello.put("type", "hello");
        hello.put("channels", filter.channels());
        hello.put("devicePrefixes", filter.devicePrefixes());
        safe.sendMessage(new TextMessage(mapper.writeValueAsString(hello)));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        clients.remove(session.getId());
        log.info("WS 종료 {} {}", session.getId(), status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.debug("WS 전송 오류 {}: {}", session.getId(), exception.toString());
    }

    public void broadcast(TagBatch batch) {
        if (clients.isEmpty()) {
            return;
        }
        // 같은 필터끼리는 한 번만 거르고 직렬화한다. 필터 없는 구독이 여럿이어도 300KB 직렬화는 한 번이다.
        Map<TagFilter, TextMessage> frames = new HashMap<>();
        for (Client client : clients.values()) {
            TextMessage frame = frames.computeIfAbsent(client.filter(), f -> frame(batch, f));
            if (frame != null) {
                senders.execute(() -> send(client, frame));
            }
        }
    }

    private TextMessage frame(TagBatch batch, TagFilter filter) {
        List<Tag> tags = filter.isAll() ? batch.tags() : batch.tags().stream().filter(filter::test).toList();
        if (tags.isEmpty()) {
            return null;
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", "tags");
        body.put("subject", batch.subject());
        body.put("edgeGroupId", batch.edgeGroupId());
        body.put("receivedAt", batch.receivedAt().toString());
        body.put("count", tags.size());
        body.put("tags", tags);
        try {
            return new TextMessage(mapper.writeValueAsString(body));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("태그 묶음 직렬화 실패", e);
        }
    }

    private void send(Client client, TextMessage frame) {
        WebSocketSession session = client.session();
        if (!session.isOpen()) {
            return;
        }
        try {
            session.sendMessage(frame);
            framesSent.increment();
        } catch (Exception e) {
            // 한도 초과(SessionLimitExceededException)면 데코레이터가 이미 세션을 닫았다
            sendFailures.increment();
            log.warn("WS 전송 실패 — 세션을 닫는다 {}: {}", session.getId(), e.toString());
            closeQuietly(session);
        }
    }

    private static void closeQuietly(WebSocketSession session) {
        try {
            session.close(CloseStatus.SESSION_NOT_RELIABLE);
        } catch (IOException ignored) {
            // 이미 닫힌 세션
        }
    }
}
