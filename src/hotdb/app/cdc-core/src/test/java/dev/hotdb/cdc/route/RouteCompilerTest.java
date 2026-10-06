package dev.hotdb.cdc.route;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.route.RoutingProperties.RouteSpec;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** 설정 → SQL · 값, 그리고 스키마가 어긋났을 때 기동을 거부하는지. DB 없이 가짜 카탈로그로 돈다. */
class RouteCompilerTest {

    static final Map<String, Map<String, String>> TABLES = Map.of(
            "tsdb.status_history", cols("event_time", "timestamp with time zone", "tag_key", "bigint",
                    "status", "text", "temperature_c", "real", "received_at", "timestamp with time zone"),
            "svc_asm.device_status_current", cols("device_id", "text", "status", "text", "temperature_c", "real",
                    "src_event_time", "timestamp with time zone", "applied_at", "timestamp with time zone"),
            "svc_asm.device_status_change", cols("device_id", "text", "changed_at", "timestamp with time zone",
                    "status", "text", "applied_at", "timestamp with time zone"));

    static final Catalog CATALOG = new Catalog() {
        @Override
        public Optional<Map<String, String>> columnsOf(TableId t) {
            return Optional.ofNullable(TABLES.get(t.toString()));
        }

        @Override
        public Set<String> columnsOfQuery(String sql) {
            return Set.of();
        }
    };

    static final Map<String, Set<String>> LOOKUPS = Map.of("tag", Set.of("tag_key", "device_id", "zone"));

    @Test
    void upsert_SQL_과_값을_만든다() {
        CompiledRoute r = compile(currentRoute(cols("device_id", "tag.device_id", "status", "row.status",
                "temperature_c", "row.temperature_c", "src_event_time", "row.event_time", "applied_at", "$now")));

        assertThat(r.sql()).isEqualTo("INSERT INTO \"svc_asm\".\"device_status_current\" AS t "
                + "(\"device_id\", \"status\", \"temperature_c\", \"src_event_time\", \"applied_at\") "
                + "VALUES (CAST(? AS text), CAST(? AS text), CAST(? AS real), CAST(? AS timestamp with time zone), clock_timestamp()) "
                + "ON CONFLICT (\"device_id\") DO UPDATE SET \"status\" = EXCLUDED.\"status\", "
                + "\"temperature_c\" = EXCLUDED.\"temperature_c\", \"src_event_time\" = EXCLUDED.\"src_event_time\", "
                + "\"applied_at\" = EXCLUDED.\"applied_at\" "
                + "WHERE t.\"src_event_time\" IS NULL OR t.\"src_event_time\" <= EXCLUDED.\"src_event_time\"");

        EvalContext ctx = new EvalContext(event(Map.of("tag_key", 7, "status", "ONLINE", "temperature_c", 40.9,
                "event_time", "2026-10-06T01:00:00Z")), Map.of("tag", Map.of("device_id", "LDR-1", "zone", "ASSEMBLY")));
        assertThat(r.matches(ctx)).isTrue();
        assertThat(r.params(ctx)).containsExactly("LDR-1", "ONLINE", "40.9", "2026-10-06T01:00:00Z");

        assertThat(r.conflictKey(r.params(ctx))).containsExactly("LDR-1");
    }

    @Test
    void 권역이_다르면_거른다() {
        CompiledRoute r = compile(currentRoute(cols("device_id", "tag.device_id", "status", "row.status",
                "src_event_time", "row.event_time")));
        EvalContext other = new EvalContext(event(Map.of("tag_key", 7, "status", "ONLINE")),
                Map.of("tag", Map.of("device_id", "LDR-1", "zone", "OUTFITTING")));
        EvalContext unknownTag = new EvalContext(event(Map.of("tag_key", 7, "status", "ONLINE")), Map.of());
        assertThat(r.matches(other)).isFalse();
        assertThat(r.matches(unknownTag)).isFalse();
    }

    @Test
    void keep_coalesce_정책과_조건식() {
        RouteSpec spec = spec("svc_asm.device_status_current", "upsert", List.of("device_id"), null,
                cols("device_id", "tag.device_id", "status", "row.status | keep",
                        "src_event_time", "row.event_time if row.status=ERROR | coalesce"));
        CompiledRoute r = compile(spec);
        assertThat(r.sql()).contains("DO UPDATE SET \"src_event_time\" = COALESCE(EXCLUDED.\"src_event_time\", t.\"src_event_time\")")
                .doesNotContain("\"status\" = EXCLUDED");

        EvalContext online = new EvalContext(event(Map.of("status", "ONLINE", "event_time", "T1")),
                Map.of("tag", Map.of("device_id", "D")));
        assertThat(r.params(online)).containsExactly("D", "ONLINE", null);
    }

    @Test
    void insert_on_change_는_직전_행과_비교한다() {
        RouteSpec spec = new RouteSpec("change", "tsdb.status_history", Map.of("tag", "row.tag_key"), List.of(),
                List.of("c"), "svc_asm.device_status_change", "insert-on-change", List.of(), null,
                List.of("status"), List.of("device_id"), "changed_at", null, true,
                cols("device_id", "tag.device_id", "changed_at", "row.event_time", "status", "row.status", "applied_at", "$now"));
        assertThat(compile(spec).sql()).isEqualTo("INSERT INTO \"svc_asm\".\"device_status_change\" "
                + "(\"device_id\", \"changed_at\", \"status\", \"applied_at\") "
                + "SELECT v.\"device_id\", v.\"changed_at\", v.\"status\", v.\"applied_at\" FROM (VALUES "
                + "(CAST(? AS text), CAST(? AS timestamp with time zone), CAST(? AS text), clock_timestamp())) "
                + "AS v (\"device_id\", \"changed_at\", \"status\", \"applied_at\") "
                + "WHERE NOT EXISTS (SELECT 1 FROM (SELECT x.\"status\" FROM \"svc_asm\".\"device_status_change\" x "
                + "WHERE x.\"device_id\" = v.\"device_id\" ORDER BY x.\"changed_at\" DESC LIMIT 1) l "
                + "WHERE l.\"status\" IS NOT DISTINCT FROM v.\"status\") ON CONFLICT DO NOTHING");
    }

    @Test
    void 스키마가_어긋나면_전부_모아서_기동을_거부한다() {
        RouteSpec bad = currentRoute(cols("device_id", "tag.device_id", "status", "row.state",
                "humidity", "row.humidity", "src_event_time", "tag.nope"));
        assertThatThrownBy(() -> compile(bad))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("대상 svc_asm.device_status_current 에 컬럼 humidity 가 없습니다")
                .hasMessageContaining("원천 tsdb.status_history 에 컬럼 state 가 없습니다")
                .hasMessageContaining("lookup 'tag' 결과에 컬럼 nope 가 없습니다");
    }

    @Test
    void 원천_어긋남은_strict_를_끄면_경고만() {
        RouteSpec spec = currentRoute(cols("device_id", "tag.device_id", "status", "row.state",
                "src_event_time", "row.event_time"));
        CompiledRoute r = new RouteCompiler(CATALOG, LOOKUPS, false).compile(List.of(spec)).get(0);
        assertThat(r.params(new EvalContext(event(Map.of()), Map.of()))).containsExactly(null, null, null);
    }

    @Test
    void 대상_표가_없으면_거부() {
        RouteSpec spec = spec("svc_asm.nope", "insert", List.of(), null, cols("a", "row.status"));
        assertThatThrownBy(() -> compile(spec)).hasMessageContaining("대상 표 svc_asm.nope 가 없습니다");
    }

    @Test
    void 모르는_식은_거부() {
        assertThatThrownBy(() -> compile(currentRoute(cols("device_id", "device.id"))))
                .hasMessageContaining("'device' 는 row/before/meta 도");
    }

    // ── 도우미 ─────────────────────────────────────────────────────────────

    static RouteSpec currentRoute(LinkedHashMap<String, String> columns) {
        return new RouteSpec("current", "tsdb.status_history", Map.of("tag", "row.tag_key"),
                List.of("tag.zone=ASSEMBLY"), List.of("c", "r"), "svc_asm.device_status_current", "upsert",
                List.of("device_id"), "src_event_time", null, null, null, "row.received_at", true, columns);
    }

    static RouteSpec spec(String target, String mode, List<String> keys, String newer, LinkedHashMap<String, String> columns) {
        return new RouteSpec("r", "tsdb.status_history", Map.of("tag", "row.tag_key"), List.of(), List.of("c"),
                target, mode, keys, newer, null, null, null, null, true, columns);
    }

    static CompiledRoute compile(RouteSpec spec) {
        return new RouteCompiler(CATALOG, LOOKUPS, true).compile(List.of(spec)).get(0);
    }

    static CdcEvent event(Map<String, Object> after) {
        TableId t = TableId.parse("tsdb.status_history");
        return new CdcEvent(t, t, Op.CREATE, null, after, 1L, 0L, "{}");
    }

    static LinkedHashMap<String, String> cols(String... kv) {
        LinkedHashMap<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }
}
