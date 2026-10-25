package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 마이그레이션 결과 검증 — 통합안 · 정규화 30표(V17) · publication · 보존 정책 · 계정 경계.
 * 기동 중인 HotDB 가 필요하다:  gradlew :zone-service:test -Dhotdb.it=true
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class HotdbSchemaIT {

    @Test
    void 통합안_정규화_30표가_있고_옛_모듈_RDB_svc_는_없다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            assertThat(db.count("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'hot' AND table_type = 'BASE TABLE'"))
                    .as("hot: m 12 · l 9 · s 3 · p 4").isEqualTo(28);
            assertThat(db.count("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'ts' AND table_type = 'BASE TABLE'"))
                    .as("ts: device_signal · sensor_event").isEqualTo(2);
            assertThat(db.count("SELECT count(*) FROM pg_namespace WHERE nspname = 'svc'")).isZero();
            // 정규화 점검에서 새로 둔 표 넷
            assertThat(db.rows("""
                    SELECT table_name FROM information_schema.tables WHERE table_schema = 'hot'
                       AND table_name IN ('m_device_state', 'm_signal_code', 'm_signal_override', 's_scan_device') ORDER BY 1"""))
                    .extracting(r -> r.get("table_name")).containsExactly("m_device_state", "m_signal_code", "m_signal_override", "s_scan_device");
        }
    }

    @Test
    void 키_정리가_설계대로다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            // P1: p_progress PK = (anchor_id, as_of)
            assertThat(pk(db, "hot", "p_progress")).containsExactly("anchor_id", "as_of");
            // I6: l_event PK 에 event_ts
            assertThat(pk(db, "hot", "l_event")).containsExactly("unit_id", "event_group", "event_code", "source", "event_ts");
            // D3: m_signal_def PK 에 device_id 없음
            assertThat(pk(db, "hot", "m_signal_def")).containsExactly("class_code", "signal_code");
            // P2: anchor_id 는 l_work_unit.unit_id 를 가리킨다
            assertThat(db.count("""
                    SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_class r ON r.oid = c.confrelid
                     WHERE c.contype = 'f' AND r.relname = 'l_work_unit'
                       AND t.relname IN ('p_progress', 'p_item_state', 'p_action', 's_scan_detection')""")).isEqualTo(4);
        }
    }

    @Test
    void 하이퍼테이블_다섯_개_tsdb_청크만_publication_에_실려_있다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            List<Map<String, Object>> rows = db.rows("""
                    SELECT c.hypertable_schema || '.' || c.hypertable_name AS ht, count(*) AS chunks,
                           count(*) FILTER (WHERE pt.tablename IS NOT NULL) AS published
                      FROM timescaledb_information.chunks c
                      LEFT JOIN pg_publication_tables pt
                        ON pt.pubname = 'tsdb_cdc_pub' AND pt.schemaname = c.chunk_schema AND pt.tablename = c.chunk_name
                     WHERE c.hypertable_schema IN ('tsdb', 'ts')
                     GROUP BY 1 ORDER BY 1
                    """);
            rows.forEach(r -> assertThat(r.get("published")).as("%s", r.get("ht"))
                    .isEqualTo(r.get("ht").toString().startsWith("tsdb.") ? r.get("chunks") : 0L));
            assertThat(db.count("SELECT count(*) FROM pg_publication_tables WHERE pubname = 'hot_cdc_pub' AND schemaname = 'hot' AND tablename = 'p_progress'"))
                    .as("SAP 로 가는 것은 p_progress 뿐").isEqualTo(1);
            assertThat(db.count("SELECT count(*) FROM pg_publication WHERE pubname = 'svc_cdc_pub'")).isZero();
        }
    }

    @Test
    void tsdb_는_7일_뒤_압축_90일_뒤_삭제한다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            List<Map<String, Object>> jobs = db.rows("""
                    SELECT hypertable_name, proc_name,
                           coalesce(config->>'compress_after', config->>'drop_after') AS after
                      FROM timescaledb_information.jobs
                     WHERE hypertable_schema = 'tsdb' AND proc_name IN ('policy_retention', 'policy_compression')
                    """);
            assertThat(jobs).hasSize(6);
            jobs.forEach(j -> assertThat(j.get("after")).as("%s %s", j.get("hypertable_name"), j.get("proc_name"))
                    .isEqualTo(j.get("proc_name").equals("policy_compression") ? "7 days" : "90 days"));
            assertThat(db.count("SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = 'ts' AND proc_name = 'policy_retention'"))
                    .isEqualTo(2);
        }
    }

    @Test
    void 사전이_실려_있다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            assertThat(db.count("SELECT count(*) FROM hot.m_code WHERE code_group = 'CHECKPOINT' AND parent_code = 'C' AND in_scope"))
                    .as("가공 절점 10 (사상은 보류)").isEqualTo(10);
            assertThat(db.count("SELECT count(*) FROM hot.m_code WHERE code_group = 'TRADE_RULE'")).isEqualTo(4);
            assertThat(db.count("SELECT count(*) FROM hot.m_code WHERE code_group = 'EVENT' AND code IN ('STEEL_SECURED', 'CARY_DUE', 'STEEL_REISSUE')"))
                    .isEqualTo(3);
            assertThat(db.count("SELECT count(*) FROM ops.module WHERE trade IS NOT NULL")).isGreaterThanOrEqualTo(4);
        }
    }

    @Test
    void 판별_모듈과_RFC_Service_와_Provider_의_쓰기_경계() throws SQLException {
        try (Db asm = Db.as("svc_asm")) {
            assertThatThrownBy(() -> asm.update("DELETE FROM hot.l_work_unit WHERE false")).hasMessageContaining("permission denied");
            assertThatThrownBy(() -> asm.update("DELETE FROM tsdb.device WHERE false")).hasMessageContaining("permission denied");
            assertThat(asm.count("SELECT count(*) FROM hot.l_work_unit")).isGreaterThanOrEqualTo(0);   // 읽기는 된다
        }
        try (Db rfc = Db.as("rfc_service")) {
            assertThatThrownBy(() -> rfc.update("DELETE FROM hot.p_progress WHERE false")).as("실적은 CDC 로 읽기만")
                    .hasMessageContaining("permission denied");
            assertThat(rfc.count("SELECT count(*) FROM hot.p_progress")).isGreaterThanOrEqualTo(0);
        }
        try (Db p = Db.as("hotdb_provider")) {
            assertThatThrownBy(() -> p.update("DELETE FROM hot.s_scan_session WHERE false")).hasMessageContaining("permission denied");
            assertThatThrownBy(() -> p.update("DELETE FROM tsdb.status_history WHERE false")).hasMessageContaining("permission denied");
        }
    }

    @Test
    void 모니터_계정은_TimescaleDB_함수를_부를_수_있다() throws SQLException {
        try (Db m = Db.as("hotdb_monitor")) {
            assertThat(m.count("SELECT hypertable_size('tsdb.status_history')")).isGreaterThanOrEqualTo(0);
            assertThat(m.count("SELECT count(*) FROM ops.module_apply_lag()")).isGreaterThanOrEqualTo(4);
        }
    }

    @Test
    void 레거시_스키마는_Hot_DB_에_없다() throws SQLException {
        // SAP · Oracle 사본은 별도 레거시 DB(compose.legacy.yml) — V10 에서 Hot DB 에서 뺐다. hot.l_* 는 사본이 아니라 정규화한 참조 표
        try (Db db = Db.as("postgres")) {
            assertThat(db.count("SELECT count(*) FROM pg_namespace WHERE nspname IN ('erp','mes','lgs','geo')")).isZero();
            assertThat(db.count("SELECT count(*) FROM pg_roles WHERE rolname IN ('rfc_agent','db_agent')")).isZero();
        }
    }

    static List<Object> pk(Db db, String schema, String table) throws SQLException {
        return db.rows("""
                SELECT a.attname FROM pg_index i
                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                 WHERE i.indrelid = to_regclass(?) AND i.indisprimary
                 ORDER BY array_position(i.indkey::int[], a.attnum::int)""", schema + "." + table)
                .stream().map(r -> r.get("attname")).toList();
    }
}
