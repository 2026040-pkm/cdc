package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * tsdb INSERT → (Debezium) → 판별 모듈 → svc (module 컬럼) 까지 실제로 흐르는지.
 *
 * <p>기동 중인 스택(HotDB + zone-asm + zone-oft)이 필요하다. 발행기와 섞이지 않게 IT- 로 시작하는
 * 장비를 새로 등록해서 쓴다. 실행마다 장비 id 가 달라 몇 번을 돌려도 된다.
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ZoneCdcE2eIT {

    static final Duration WAIT = Duration.ofSeconds(30);
    static final String RUN = UUID.randomUUID().toString().substring(0, 8);
    static final String ASM = "IT-ASM-" + RUN;
    static final String OFT = "IT-OFT-" + RUN;
    static final OffsetDateTime T0 = OffsetDateTime.now().withNano(0);
    static long asmStatus;
    static long asmActual;
    static long asmMatrix;
    static long asmSegment;
    static long asmRegistered;
    static long oftStatus;

    @BeforeAll
    static void 장비와_태그를_등록한다() throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            asmStatus = register(p, ASM, "ASSEMBLY", "status", null);
            asmActual = register(p, ASM, "ASSEMBLY", "actual", null);
            asmMatrix = register(p, ASM, "ASSEMBLY", "artifact", "TRANSFORMATION_MATRIX");
            asmSegment = register(p, ASM, "ASSEMBLY", "artifact", "SEGMENTED_PCD");
            asmRegistered = register(p, ASM, "ASSEMBLY", "artifact", "REGISTERED_PCD");
            oftStatus = register(p, OFT, "OUTFITTING", "status", null);
        }
    }

    @Test
    @Order(1)
    void status_가_자기_권역_최신값_표에만_들어간다() throws SQLException {
        status(asmStatus, T0, "ONLINE", null);
        status(oftStatus, T0, "ONLINE", null);

        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "ONLINE".equals(
                    db.one("SELECT status FROM svc.device_status_current WHERE module = 'asm' AND device_id = ?", ASM)));
            await().atMost(WAIT).until(() -> db.count(
                    "SELECT count(*) FROM svc.device_status_current WHERE module = 'oft' AND device_id = ?", OFT) == 1);

            assertThat(db.count("SELECT count(*) FROM svc.device_status_current WHERE module = 'oft' AND device_id = ?", ASM)).isZero();
            assertThat(db.count("SELECT count(*) FROM svc.device_status_current WHERE module = 'asm' AND device_id = ?", OFT)).isZero();

            double lag = ((Number) db.one("""
                    SELECT extract(epoch FROM applied_at - src_received_at)
                      FROM svc.device_status_current WHERE module = 'asm' AND device_id = ?""", ASM)).doubleValue();
            assertThat(lag).as("수신 → 반영 지연(초)").isLessThan(5);
        }
    }

    @Test
    @Order(2)
    void 상태가_바뀐_순간만_전이_이력에_남는다() throws SQLException {
        status(asmStatus, T0.plusSeconds(1), "ERROR", "E101");
        status(asmStatus, T0.plusSeconds(2), "ERROR", "E101");   // 같은 상태 — 이력 안 늘어남
        status(asmStatus, T0.plusSeconds(3), "ONLINE", null);

        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> db.count(
                    "SELECT count(*) FROM svc.device_status_change WHERE module = 'asm' AND device_id = ?", ASM) == 3);
            assertThat(db.rows("SELECT status FROM svc.device_status_change WHERE module = 'asm' AND device_id = ? ORDER BY changed_at", ASM))
                    .extracting(r -> r.get("status")).containsExactly("ONLINE", "ERROR", "ONLINE");
        }
    }

    @Test
    @Order(3)
    void 늦게_온_옛_이벤트는_최신값을_덮지_않는다() throws SQLException {
        status(asmStatus, T0.minusSeconds(30), "OFFLINE", null);
        status(asmStatus, T0.plusSeconds(4), "CALIBRATING", null);   // 이게 반영되면 앞의 것도 처리된 것

        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "CALIBRATING".equals(
                    db.one("SELECT status FROM svc.device_status_current WHERE module = 'asm' AND device_id = ?", ASM)));
        }
    }

    @Test
    @Order(4)
    void 스캔_START_부터_COMPLETE_까지_스캔과_실적이_만들어진다() throws SQLException {
        UUID scan = UUID.randomUUID();
        actual(scan, T0, "START", 0);
        actual(scan, T0.plusSeconds(60), "PROGRESS", 55);
        actual(scan, T0.plusSeconds(120), "COMPLETE", 100);

        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> db.count(
                    "SELECT count(*) FROM svc.actual_result WHERE module = 'asm' AND scan_id = ?", scan) == 1);
            var s = db.rows("SELECT * FROM svc.scan WHERE module = 'asm' AND scan_id = ?", scan).get(0);
            assertThat(s.get("last_event_type")).isEqualTo("COMPLETE");
            assertThat(s.get("completed_at")).isNotNull();
            assertThat(((java.sql.Timestamp) s.get("first_event_at")).toInstant()).isEqualTo(T0.toInstant());
            assertThat(db.count("SELECT count(*) FROM svc.scan WHERE module = 'oft' AND scan_id = ?", scan)).isZero();
        }
    }

    @Test
    @Order(5)
    void 산출물_변환행렬_배열이_그대로_옮겨진다() throws SQLException {
        UUID scan = UUID.randomUUID();
        try (Db p = Db.as("hotdb_provider")) {
            p.update("""
                    INSERT INTO tsdb.artifact_history (event_time, tag_key, scan_id, artifact_type, segment_key, hull_no, block_id,
                      transformation_matrix, source_time_text, received_at)
                    VALUES (?, ?, ?, 'TRANSFORMATION_MATRIX', '', 'H1', 'B1',
                      '{1,0,0,10.5, 0,1,0,0, 0,0,1,0, 0,0,0,1}', 'it', clock_timestamp())
                    """, T0, asmMatrix, scan);
            p.update("""
                    INSERT INTO tsdb.artifact_history (event_time, tag_key, scan_id, artifact_type, segment_key, hull_no, block_id,
                      storage_uri, file_size_bytes, source_time_text, received_at)
                    VALUES (?, ?, ?, 'SEGMENTED_PCD', 'SEG-01', 'H1', 'B1', 'file://it/seg01.pcd', 123, 'it', clock_timestamp())
                    """, T0, asmSegment, scan);
        }
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> db.count("SELECT count(*) FROM svc.artifact WHERE module = 'asm' AND scan_id = ?", scan) == 2);
            assertThat(db.one("""
                    SELECT transformation_matrix[4] FROM svc.artifact
                     WHERE module = 'asm' AND scan_id = ? AND artifact_type = 'TRANSFORMATION_MATRIX'""", scan)).isEqualTo(10.5f);
            assertThat(db.one("SELECT device_id FROM svc.artifact WHERE module = 'asm' AND scan_id = ? AND segment_key = 'SEG-01'", scan))
                    .isEqualTo(ASM);
        }
    }

    @Test
    @Order(6)
    void 새로_생긴_청크도_따로_손대지_않고_따라온다() throws SQLException {
        // 6시간 청크 간격이라 40일 뒤 시각은 반드시 새 청크를 만든다 → 서비스는 처음 보는 청크 이름을 받는다
        OffsetDateTime future = T0.plusDays(40);
        status(asmStatus, future, "OFFLINE", null);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "OFFLINE".equals(
                    db.one("SELECT status FROM svc.device_status_current WHERE module = 'asm' AND device_id = ?", ASM)));
            assertThat(db.count("SELECT count(*) FROM ops.cdc_dead_letter WHERE payload::text LIKE ?", "%" + RUN + "%"))
                    .isZero();
        }
    }

    @Test
    @Order(7)
    void 산출물이_스캔보다_먼저_와도_부모_자리를_만들고_START_가_제자리를_채운다() throws SQLException {
        UUID scan = UUID.randomUUID();
        try (Db p = Db.as("hotdb_provider")) {
            p.update("""
                    INSERT INTO tsdb.artifact_history (event_time, tag_key, scan_id, artifact_type, segment_key, hull_no, block_id,
                      storage_uri, source_time_text, received_at)
                    VALUES (?, ?, ?, 'REGISTERED_PCD', '', 'H2', 'B2', 'file://it/reg.pcd', 'it', clock_timestamp())
                    """, T0.plusSeconds(10), asmRegistered, scan);
        }
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> db.count("SELECT count(*) FROM svc.artifact WHERE module = 'asm' AND scan_id = ?", scan) == 1);
            assertThat(db.one("SELECT last_event_type FROM svc.scan WHERE module = 'asm' AND scan_id = ?", scan)).isEqualTo("PENDING");
        }
        actual(scan, T0, "START", 0);   // 산출물보다 이른 시각의 START 가 뒤늦게 온다
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "START".equals(
                    db.one("SELECT last_event_type FROM svc.scan WHERE module = 'asm' AND scan_id = ?", scan)));
            assertThat(((java.sql.Timestamp) db.one("SELECT first_event_at FROM svc.scan WHERE module = 'asm' AND scan_id = ?", scan))
                    .toInstant()).as("first_event_at = min(산출물, START)").isEqualTo(T0.toInstant());
        }
    }

    @Test
    @Order(8)
    void 모듈_RDB_는_관계로_묶여_있어_부모_없는_자식은_들어가지_않는다() throws SQLException {
        try (Db svc = Db.as("svc_asm")) {
            assertThatThrownBy(() -> svc.update("""
                    INSERT INTO svc.actual_result (module, hull_no, block_id, scan_id, device_id, completed_at,
                      src_received_at, applied_at)
                    VALUES ('asm', 'H9', 'B9', ?, ?, now(), now(), now())""", UUID.randomUUID(), ASM))
                    .hasMessageContaining("foreign key");
            assertThatThrownBy(() -> svc.update("""
                    INSERT INTO svc.device_status_current (module, site, device_id, status, src_event_time, src_received_at, applied_at)
                    VALUES ('asm', 'it', 'NO-SUCH-DEVICE', 'ONLINE', now(), now(), now())"""))
                    .as("장비 마스터(tsdb.device)에 없는 장비").hasMessageContaining("foreign key");
        }
    }

    // ── 도우미 ─────────────────────────────────────────────────────────────

    static long register(Db p, String device, String zone, String channel, String artifactType) throws SQLException {
        p.update("INSERT INTO tsdb.device (site, device_id, device_role, zone) VALUES ('it', ?, 'LIDAR', ?) ON CONFLICT DO NOTHING",
                device, zone);
        String tagId = device + "." + channel + (artifactType == null ? "" : "." + artifactType);
        p.update("""
                INSERT INTO tsdb.tag_catalog (edge_group_id, tag_id, mqtt_topic, source_id, site, device_id, channel, artifact_type)
                VALUES ('it', ?, 'it', ?, 'it', ?, ?, ?)
                """, tagId, device, device, channel, artifactType);
        return ((Number) p.one("SELECT tag_key FROM tsdb.tag_catalog WHERE edge_group_id = 'it' AND tag_id = ?", tagId)).longValue();
    }

    static void status(long tag, OffsetDateTime at, String status, String error) throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            p.update("""
                    INSERT INTO tsdb.status_history (event_time, tag_key, status, error_code, source_time_text, received_at)
                    VALUES (?, ?, ?, ?, 'it', clock_timestamp())
                    """, at, tag, status, error);
        }
    }

    static void actual(UUID scan, OffsetDateTime at, String type, double progress) throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            p.update("""
                    INSERT INTO tsdb.actual_history (event_time, tag_key, scan_id, event_type, hull_no, block_id,
                      block_progress_rate, source_time_text, received_at)
                    VALUES (?, ?, ?, ?, 'H1', 'B1', ?, 'it', clock_timestamp())
                    """, at, asmActual, scan, type, (float) progress);
        }
    }
}
