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
 * CDC 두 번: tsdb INSERT → 판별 모듈(s_scan_detection → 센서 판별 → hot.p_progress) → RFC Service → ops.rfc_progress_sent.
 *
 * <p>기동 중인 스택(HotDB + zone-asm + rfc-service)이 필요하다. IT- 장비 · 호선을 실행마다 새로 쓰고,
 * 조립 앵커(송선 = l_work_unit ACTIVITY 행)는 직접 넣는다 (RFC Service 적재 대신).
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RfcServiceE2eIT {

    static final Duration WAIT = Duration.ofSeconds(30);
    static final String RUN = UUID.randomUUID().toString().substring(0, 8);
    static final String DEVICE = "IT-RFC-" + RUN;
    static final String HULL = "R" + RUN.substring(0, 4);
    static final String ANCHOR = HULL + "SB01A200‖S6G9";
    static long actualTag;

    @BeforeAll
    static void 장비와_태그와_앵커를_등록한다() throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            p.update("INSERT INTO tsdb.device (site, device_id, device_role, zone) VALUES ('it', ?, 'LIDAR', 'ASSEMBLY')", DEVICE);
            p.update("""
                    INSERT INTO tsdb.tag_catalog (edge_group_id, tag_id, mqtt_topic, source_id, site, device_id, channel)
                    VALUES ('it', ?, 'it', ?, 'it', ?, 'actual')
                    """, DEVICE + ".actual", DEVICE, DEVICE);
            actualTag = p.count("SELECT tag_key FROM tsdb.tag_catalog WHERE edge_group_id = 'it' AND tag_id = ?", DEVICE + ".actual");
        }
        try (Db rfc = Db.as("rfc_service")) {
            rfc.update("INSERT INTO hot.m_hull (hull_no) VALUES (?) ON CONFLICT DO NOTHING", HULL);
            rfc.update("INSERT INTO hot.m_block (hull_no, block_no) VALUES (?, 'B01') ON CONFLICT DO NOTHING", HULL);
            rfc.update("""
                    INSERT INTO hot.l_work_unit (unit_id, level, hull_no, block_no, trade, wstg_code, plan_mh, actl_sd, site,
                                                 source_table, anchor_type, target_qty, target_basis)
                    VALUES (?, 'ACTIVITY', ?, 'B01', 'S', 'S6G9', 600, DATE '2026-10-01', 'IN', 'YPWG411M', 'WSTG', 11, 'ASSY_QTY')
                    ON CONFLICT (unit_id) DO NOTHING""", ANCHOR, HULL);
        }
    }

    @Test
    @Order(1)
    void tsdb_의_COMPLETE_가_판별을_거쳐_SAP_송신_기록까지_간다() throws SQLException {
        complete(55, 0.95);   // inProgress
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> sent(db) == 1);
            var row = db.rows("""
                    SELECT state, extract(epoch FROM sent_at - src_judged_at) AS hop2
                      FROM ops.rfc_progress_sent WHERE anchor_id = ?""", ANCHOR).get(0);
            assertThat(row.get("state")).isEqualTo("inProgress");
            assertThat(((Number) row.get("hop2")).doubleValue()).as("판별 → SAP 송신(초)").isLessThan(10);
        }
    }

    @Test
    @Order(2)
    void 진척이_바뀌면_다시_보내고_같은_내용은_다시_보내지_않는다() throws SQLException {
        complete(70, 0.95);   // 같은 상태(inProgress)에서 auto_pct 55 → 70 — 내용 해시가 바뀌어 다시 보낸다 (V20 F3)
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> sent(db) == 2);
        }
        complete(70, 0.95);   // 같은 내용 — p_progress 는 갱신되지만(증거 · 판정 시각) 송신 없음
        try (Db db = Db.as("postgres")) {
            await().during(Duration.ofSeconds(5)).atMost(WAIT).until(() -> sent(db) == 2);
        }
        complete(100, 0.95);  // done
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> sent(db) == 3);
            assertThat(db.rows("SELECT state FROM ops.rfc_progress_sent WHERE anchor_id = ? ORDER BY sent_at", ANCHOR))
                    .extracting(r -> r.get("state")).containsExactly("inProgress", "inProgress", "done");
        }
    }

    @Test
    @Order(3)
    void dead_letter_가_없다() throws SQLException {
        try (Db db = Db.as("postgres")) {
            assertThat(db.count("SELECT count(*) FROM ops.cdc_dead_letter WHERE pipeline = 'rfc-service' "
                    + "AND payload::text LIKE ?", "%" + HULL + "%")).isZero();
        }
    }

    static long sent(Db db) throws SQLException {
        return db.count("SELECT count(*) FROM ops.rfc_progress_sent WHERE anchor_id = ?", ANCHOR);
    }

    static void complete(double progress, double confidence) throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            p.update("""
                    INSERT INTO tsdb.actual_history (event_time, tag_key, scan_id, event_type, hull_no, block_id,
                      block_progress_rate, match_confidence, source_time_text, received_at)
                    VALUES (?, ?, ?, 'COMPLETE', ?, 'B01', ?, ?, 'it', clock_timestamp())
                    """, OffsetDateTime.now(), actualTag, UUID.randomUUID(), HULL, (float) progress, (float) confidence);
        }
    }
}
