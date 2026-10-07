package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 마이그레이션 결과 검증 — 표 · publication · 보존 정책 · 계정 경계.
 * 기동 중인 HotDB 가 필요하다:  gradlew :zone-service:test -Dhotdb.it=true
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class HotdbSchemaIT {

    @Test
    void 하이퍼테이블_세_개와_청크가_publication_에_실려_있다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            List<Map<String, Object>> rows = db.rows("""
                    SELECT c.hypertable_name, count(*) AS chunks,
                           count(*) FILTER (WHERE pt.tablename IS NOT NULL) AS published
                      FROM timescaledb_information.chunks c
                      LEFT JOIN pg_publication_tables pt
                        ON pt.pubname = 'tsdb_cdc_pub' AND pt.schemaname = c.chunk_schema AND pt.tablename = c.chunk_name
                     WHERE c.hypertable_schema = 'tsdb'
                     GROUP BY 1 ORDER BY 1
                    """);
            assertThat(rows).extracting(r -> r.get("hypertable_name"))
                    .containsExactly("actual_history", "artifact_history", "status_history");
            rows.forEach(r -> assertThat(r.get("published")).as("%s 의 모든 청크가 publication 에", r.get("hypertable_name"))
                    .isEqualTo(r.get("chunks")));
            assertThat(db.count("""
                    SELECT count(*) FROM pg_publication_tables
                     WHERE pubname = 'tsdb_cdc_pub' AND schemaname = 'tsdb' AND tablename IN ('device','tag_catalog')
                    """)).isEqualTo(2);
        }
    }

    @Test
    void tsdb_는_7일_뒤_압축_90일_뒤_삭제한다() throws SQLException {
        // V13: 압축은 7일 뒤 — CDC 소비자가 읽기 전에 압축되지 않게. 보존 90일
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
        }
    }

    @Test
    void 판별_모듈은_다른_모듈의_행을_쓰지도_보지도_못한다() throws SQLException {
        try (Db asm = Db.as("svc_asm")) {
            // 행 보안(RLS): svc_asm 은 module = 'asm' 행만 — 'oft' 로 넣으면 거부
            assertThatThrownBy(() -> asm.update("""
                    INSERT INTO svc.device_status_current (module, site, device_id, status, src_event_time, src_received_at, applied_at)
                    SELECT 'oft', site, device_id, 'ONLINE', now(), now(), now() FROM tsdb.device LIMIT 1"""))
                    .hasMessageContaining("row-level security");
            assertThat(asm.count("SELECT count(*) FROM svc.device_status_current WHERE module <> 'asm'"))
                    .as("다른 모듈 행은 보이지 않는다").isZero();
            assertThatThrownBy(() -> asm.update("DELETE FROM tsdb.device WHERE false"))
                    .hasMessageContaining("permission denied");
            // 읽기는 된다
            assertThat(asm.count("SELECT count(*) FROM tsdb.tag_catalog")).isGreaterThanOrEqualTo(0);
        }
        try (Db rfc = Db.as("rfc_service")) {
            assertThat(rfc.count("SELECT count(DISTINCT module) FROM svc.device_status_current"))
                    .as("RFC Service 는 모든 모듈을 읽는다").isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void Provider_는_tsdb_에만_쓴다() throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            assertThatThrownBy(() -> p.update("DELETE FROM svc.scan WHERE false"))
                    .hasMessageContaining("permission denied");
            // 이력은 지우지 못한다 — 정리는 보존 정책만 한다
            assertThatThrownBy(() -> p.update("DELETE FROM tsdb.status_history WHERE false"))
                    .hasMessageContaining("permission denied");
        }
    }

    @Test
    void 모니터_계정은_TimescaleDB_함수를_부를_수_있다() throws SQLException {
        try (Db m = Db.as("hotdb_monitor")) {
            assertThat(m.count("SELECT hypertable_size('tsdb.status_history')")).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void 레거시_스키마는_Hot_DB_에_없다() throws SQLException {
        // SAP · Oracle 사본은 별도 레거시 DB(compose.legacy.yml) — V10 에서 Hot DB 에서 뺐다
        try (Db db = Db.as("postgres")) {
            assertThat(db.count("""
                    SELECT count(*) FROM pg_namespace WHERE nspname IN ('erp','mes','lgs','geo')
                    """)).isZero();
            assertThat(db.count("SELECT count(*) FROM pg_roles WHERE rolname IN ('rfc_agent','db_agent')")).isZero();
        }
    }
}
