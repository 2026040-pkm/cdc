package dev.hotdb.cdc.adapter.debezium;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.timescale.HypertableResolver;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Debezium JSON(스키마 없는 envelope) → {@link CdcEvent}.
 *
 * <p>heartbeat · 스키마 변경 레코드처럼 op 가 없는 값은 empty 로 돌려준다.
 * Debezium 레코드 모양을 아는 곳은 여기뿐이다 — 포트 너머(라우팅 · 반영)는 {@link CdcEvent} 만 본다.
 */
public class DebeziumEventDecoder {

    private static final TypeReference<Map<String, Object>> ROW = new TypeReference<>() {};

    private final ObjectMapper json;
    private final Function<TableId, Optional<TableId>> resolver;

    public DebeziumEventDecoder(ObjectMapper json, HypertableResolver resolver) {
        this(json, resolver::resolve);
    }

    DebeziumEventDecoder(ObjectMapper json, Function<TableId, Optional<TableId>> resolver) {
        this.json = json;
        this.resolver = resolver;
    }

    public Decoded decode(String value) {
        if (value == null) {
            return Decoded.skip("tombstone");
        }
        try {
            JsonNode root = json.readTree(value);
            JsonNode op = root.get("op");
            JsonNode source = root.get("source");
            if (op == null || source == null || op.isNull()) {
                return Decoded.skip("no_op");
            }
            TableId physical = new TableId(source.path("schema").asText(), source.path("table").asText());
            Optional<TableId> logical = resolver.apply(physical);
            if (logical.isEmpty()) {
                return Decoded.skip("unknown_chunk");
            }
            return Decoded.of(new CdcEvent(logical.get(), physical, Op.of(op.asText()),
                    toMap(root.get("before")), toMap(root.get("after")),
                    source.hasNonNull("lsn") ? source.get("lsn").asLong() : null,
                    source.hasNonNull("ts_ms") ? source.get("ts_ms").asLong() : null,
                    value));
        } catch (Exception e) {
            throw new IllegalStateException("Debezium 레코드를 읽지 못했습니다: " + e.getMessage(), e);
        }
    }

    private Map<String, Object> toMap(JsonNode node) {
        return node == null || node.isNull() ? null : json.convertValue(node, ROW);
    }

    /** 해석 결과 — 이벤트 또는 버린 이유. */
    public record Decoded(CdcEvent event, String skipReason) {
        static Decoded of(CdcEvent e) {
            return new Decoded(e, null);
        }

        static Decoded skip(String reason) {
            return new Decoded(null, reason);
        }
    }
}
