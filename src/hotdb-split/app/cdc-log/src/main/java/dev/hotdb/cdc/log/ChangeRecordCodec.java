package dev.hotdb.cdc.log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import java.util.Map;

/**
 * 변경 로그의 선 형식 — 한 줄 = {@code <seq>\t<JSON>\n}.
 *
 * <pre>
 * 42\t{"pos":"lsn:2483194880","table":"tsdb.status_history","physical":"_timescaledb_internal._hyper_1_9_chunk",
 *      "op":"c","before":null,"after":{"tag_key":7,"status":"ONLINE",...},"lsn":2483194880,"ts":1791339636123}
 * </pre>
 *
 * 순번을 JSON 밖에 둔 이유: 캡처부는 줄을 해석하지 않고 순번만 읽어 색인 · 전송하고, 전달부는 줄을 받은 그대로
 * dead letter 원문으로 남긴다. JSON 안 필드 이름은 계약이다 — 더하기만 하고 바꾸지 않는다(모르는 필드는 무시한다).
 */
public final class ChangeRecordCodec {

    private static final TypeReference<Map<String, Object>> ROW = new TypeReference<>() {};

    private final ObjectMapper json;

    public ChangeRecordCodec(ObjectMapper json) {
        this.json = json;
    }

    /** 순번 없는 JSON 부분. 캡처부가 쓰고, 순번은 로그가 붙인다 */
    public String encode(String position, CdcEvent e) {
        ObjectNode n = json.createObjectNode();
        n.put("pos", position);
        n.put("table", e.table().toString());
        n.put("physical", e.physical() == null ? null : e.physical().toString());
        n.put("op", e.op().code());
        n.set("before", json.valueToTree(e.before()));
        n.set("after", json.valueToTree(e.after()));
        if (e.lsn() != null) {
            n.put("lsn", e.lsn());
        }
        if (e.commitTsMs() != null) {
            n.put("ts", e.commitTsMs());
        }
        try {
            return json.writeValueAsString(n);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("변경 로그 줄을 만들지 못했다: " + e.table(), ex);
        }
    }

    /** 한 줄({@code seq\tjson}) → 레코드 */
    public ChangeRecord decodeLine(String line) {
        int tab = line.indexOf('\t');
        if (tab <= 0) {
            throw new IllegalArgumentException("변경 로그 줄 형식이 아니다 (seq\tjson): " + abbreviate(line));
        }
        long seq = Long.parseLong(line, 0, tab, 10);
        String body = line.substring(tab + 1);
        JsonNode n = read(body);
        return new ChangeRecord(seq, text(n, "pos"), toEvent(n, body));
    }

    /** JSON 부분만 → 이벤트. dead letter 재처리가 쓴다 (원문 = 이 JSON) */
    public CdcEvent decodeJson(String body) {
        return toEvent(read(body), body);
    }

    private CdcEvent toEvent(JsonNode n, String raw) {
        TableId table = TableId.parse(n.get("table").asText());
        String physical = text(n, "physical");
        return new CdcEvent(table, physical == null ? table : TableId.parse(physical), Op.of(n.get("op").asText()),
                row(n.get("before")), row(n.get("after")),
                n.hasNonNull("lsn") ? n.get("lsn").asLong() : null,
                n.hasNonNull("ts") ? n.get("ts").asLong() : null,
                raw);
    }

    private Map<String, Object> row(JsonNode n) {
        return n == null || n.isNull() ? null : json.convertValue(n, ROW);
    }

    private JsonNode read(String body) {
        try {
            return json.readTree(body);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("변경 로그 JSON 을 읽지 못했다: " + abbreviate(body), ex);
        }
    }

    private static String text(JsonNode n, String field) {
        return n.hasNonNull(field) ? n.get(field).asText() : null;
    }

    private static String abbreviate(String s) {
        return s.length() > 120 ? s.substring(0, 120) + "…" : s;
    }
}
