package dev.hotdb.rfc;

import static org.assertj.core.api.Assertions.assertThat;
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
 * CDC 두 번: tsdb INSERT → 권역 서비스 → svc.actual_result → RFC Provider → ops.rfc_sent.
 *
 * <p>기동 중인 스택(HotDB + zone-asm + rfc-provider)이 필요하다. IT- 장비 · 선체를 실행마다 새로 쓴다.
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RfcProviderE2eIT {

    static final Duration WAIT = Duration.ofSeconds(30);
    static final String RUN = UUID.randomUUID().toString().substring(0, 8);
    static final String DEVICE = "IT-RFC-" + RUN;
    static final String HULL = "IT" + RUN;
    static final UUID SCAN = UUID.randomUUID();
    static long actualTag;

    @BeforeAll
    static void 장비와_태그를_등록한다() throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            p.update("INSERT INTO tsdb.device (site, device_id, device_role, zone) VALUES ('it', ?, 'LIDAR', 'ASSEMBLY')",
                    DEVICE);
            p.update("""
                    INSERT INTO tsdb.tag_catalog (edge_group_id, tag_id, mqtt_topic, source_id, site, device_id, channel)
                    VALUES ('it', ?, 'it', ?, 'it', ?, 'actual')
                    """, DEVICE + ".actual", DEVICE, DEVICE);
            actualTag = p.count("SELECT tag_key FROM tsdb.tag_catalog WHERE edge_group_id = 'it' AND tag_id = ?",
                    DEVICE + ".actual");
        }
    }

    @Test
    @Order(1)
    void tsdb_의_COMPLETE_가_두_번의_CDC_를_거쳐_SAP_송신_기록까지_간다() throws SQLException {
        OffsetDateTime t0 = OffsetDateTime.now().withNano(0);
        try (Db p = Db.as("hotdb_provider")) {
            p.update("""
                    INSERT INTO tsdb.actual_history (event_time, tag_key, scan_id, event_type, hull_no, block_id,
                      block_progress_rate, source_time_text, received_at)
                    VALUES (?, ?, ?, 'COMPLETE', ?, 'B1', 100, 'it', clock_timestamp())
                    """, t0, actualTag, SCAN, HULL);
        }
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> sent(db) == 1);
            var row = db.rows("""
                    SELECT zone, judged_status, extract(epoch FROM sent_at - src_received_at) AS e2e
                      FROM ops.rfc_sent WHERE hull_no = ?""", HULL).get(0);
            assertThat(row.get("zone")).isEqualTo("asm");
            assertThat(row.get("judged_status")).isEqualTo("PENDING");
            assertThat(((Number) row.get("e2e")).doubleValue()).as("Provider 수신 → SAP 송신(초)").isLessThan(10);
        }
    }

    @Test
    @Order(2)
    void 같은_실적의_재반영은_다시_보내지_않고_판정이_바뀌면_한_번_더_보낸다() throws SQLException {
        try (Db svc = Db.as("svc_asm"); Db db = Db.as("postgres")) {
            // 권역 서비스가 재전송을 받으면 applied_at 만 바뀐 UPDATE 가 나간다
            svc.update("UPDATE svc.actual_result SET applied_at = clock_timestamp() WHERE hull_no = ?", HULL);
            svc.update("UPDATE svc.actual_result SET judged_status = 'CONFIRMED', applied_at = clock_timestamp() "
                    + "WHERE hull_no = ?", HULL);

            await().atMost(WAIT).until(() -> sent(db) == 2);
            assertThat(db.rows("SELECT judged_status FROM ops.rfc_sent WHERE hull_no = ? ORDER BY sent_at", HULL))
                    .extracting(r -> r.get("judged_status")).containsExactly("PENDING", "CONFIRMED");
            // 판정 뒤로도 늘지 않는다 (applied_at 만 바뀐 UPDATE 는 걸렀다)
            await().during(Duration.ofSeconds(3)).atMost(WAIT).until(() -> sent(db) == 2);
        }
    }

    @Test
    @Order(3)
    void dead_letter_가_없다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            assertThat(db.count("SELECT count(*) FROM ops.cdc_dead_letter WHERE pipeline = 'rfc-provider' "
                    + "AND payload::text LIKE ?", "%" + HULL + "%")).isZero();
        }
    }

    static long sent(Db db) throws SQLException {
        return db.count("SELECT count(*) FROM ops.rfc_sent WHERE hull_no = ?", HULL);
    }
}
