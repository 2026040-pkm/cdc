package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * tsdb COMPLETE → (CDC) → s_scan_detection (judged=false) → (센서 판별) → p_progress_block · p_progress · p_action.
 *
 * <p>기동 중인 zone-asm(기본 임계값 규칙: 진척률 100 · 정합도 0.9)이 필요하다. 조립 앵커(송선 = l_work_unit ACTIVITY 행)는
 * RFC Service 가 레거시에서 채우는 표라, 여기서는 시험용 호선 · 블록의 앵커를 직접 넣는다.
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class JudgementIT {

    static final Duration WAIT = Duration.ofSeconds(30);
    static final String RUN = UUID.randomUUID().toString().substring(0, 8);
    static final String DEVICE = "IT-JDG-" + RUN;
    static final String HULL = "J" + RUN.substring(0, 4);
    static long actualTag;

    @BeforeAll
    static void 장비와_앵커를_등록한다() throws SQLException {
        try (Db p = Db.as("hotdb_provider")) {
            actualTag = ZoneCdcE2eIT.register(p, DEVICE, "ASSEMBLY", "actual", null);
        }
        try (Db rfc = Db.as("rfc_service")) {
            rfc.update("INSERT INTO hot.m_hull (hull_no) VALUES (?) ON CONFLICT DO NOTHING", HULL);
            for (String b : new String[] {"B01", "B02", "B03", "B04"}) {
                rfc.update("INSERT INTO hot.m_block (hull_no, block_no) VALUES (?, ?) ON CONFLICT DO NOTHING", HULL, b);
            }
            // B01: 송선 앵커 하나, 레거시 W/O 착수됨 → done 이 될 수 있다
            anchor(rfc, HULL, "B01", "S6G9", true);
            // B02: 송선 앵커 하나, 착수 전 → 판별 O / 레거시 X = held
            anchor(rfc, HULL, "B02", "S6G9", false);
            // B03: 송선 앵커 둘 → 블록 단위까지만
            anchor(rfc, HULL, "B03", "S6G9", true);
            anchor(rfc, HULL, "B03", "S7G9", true);
            // B01 의 하위 W/O 둘 — 오늘이 계획 기간에 드는 취부(15번째 글자 F) W/O 가 하루치로 골라진다
            wo(rfc, HULL + "SB01A200‖S6G9", HULL + "SB01A200SF01", -1, 1);
            wo(rfc, HULL + "SB01A200‖S6G9", HULL + "SB01A200SW01", -2, 2);
            // B04: 계획이 석 달 전 — 4주 풀에도 없다 → W/O 불일치
            anchor(rfc, HULL, "B04", "S6G9", true, -100, -90);
        }
    }

    static void anchor(Db rfc, String hull, String block, String wstg, boolean started) throws SQLException {
        anchor(rfc, hull, block, wstg, started, -3, 3);
    }

    /** 계획 = 오늘 + sd ~ 오늘 + fd (일) */
    static void anchor(Db rfc, String hull, String block, String wstg, boolean started, int sd, int fd) throws SQLException {
        rfc.update("""
                INSERT INTO hot.l_work_unit (unit_id, level, hull_no, block_no, trade, wstg_code, plan_mh, actl_sd, site,
                                             source_table, anchor_type, target_qty, target_basis, plan_sd, plan_fd)
                VALUES (?, 'ACTIVITY', ?, ?, 'S', ?, 600, ?, 'IN', 'YPWG411M', 'WSTG', 11, 'ASSY_QTY',
                        current_date + ?, current_date + ?)
                ON CONFLICT (unit_id) DO NOTHING
                """, hull + "S" + block + "A200‖" + wstg, hull, block, wstg, started ? java.sql.Date.valueOf("2026-10-01") : null,
                sd, fd);
    }

    static void wo(Db rfc, String parent, String woNo, int sd, int fd) throws SQLException {
        rfc.update("""
                INSERT INTO hot.l_work_unit (unit_id, level, parent_id, trade, plan_mh, site, source_table, plan_sd, plan_fd)
                VALUES (?, 'WO', ?, 'S', 300, 'IN', 'YPWG411M', current_date + ?, current_date + ?)
                ON CONFLICT (unit_id) DO NOTHING
                """, woNo, parent, sd, fd);
    }

    @Test
    void 임계를_넘고_앵커가_하나면_done_이_실적과_블록_롤업에_남는다() throws SQLException {
        UUID scan = complete("B01", 100, 0.95);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> Boolean.TRUE.equals(judged(db, scan)));
            var d = db.rows("SELECT anchor_id, anchor_type, judge_note FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString()).get(0);
            assertThat(d.get("anchor_id")).isEqualTo(HULL + "SB01A200‖S6G9");
            assertThat(d.get("anchor_type")).isEqualTo("WSTG");
            assertThat(d.get("judge_note")).isEqualTo("done");

            var p = db.rows("SELECT * FROM hot.p_progress WHERE anchor_id = ? AND as_of = current_date", HULL + "SB01A200‖S6G9").get(0);
            assertThat(p.get("state")).isEqualTo("done");
            assertThat(((Number) p.get("auto_pct")).doubleValue()).isEqualTo(100.0);
            assertThat(p.get("match_state")).isEqualTo("MATCHED");
            assertThat(p.get("match_pool")).isEqualTo("DAY");
            assertThat(p.get("match_wo_no")).isEqualTo(HULL + "SB01A200SF01");
            assertThat(p.get("match_lead")).isEqualTo("ON_TIME");
            assertThat(p.get("evidence").toString()).contains(scan.toString());

            var b = db.rows("SELECT * FROM hot.p_progress_block WHERE hull_no = ? AND block_no = 'B01' AND as_of = current_date", HULL).get(0);
            assertThat(b.get("trade")).isEqualTo("S");
            assertThat(b.get("stage")).isEqualTo("ASSY");
            assertThat(b.get("wip_state")).isEqualTo("DONE");
            assertThat(((Number) b.get("anchor_cnt")).intValue()).isEqualTo(1);
            assertThat(b).containsKey("open_action_cnt");
        }
    }

    @Test
    void 계획_범위에_맞는_W_O_가_없으면_불일치_알림() throws SQLException {
        UUID scan = complete("B04", 100, 0.95);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> Boolean.TRUE.equals(judged(db, scan)));
            assertThat(db.one("SELECT judge_note FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString())).isEqualTo("done/WO_MISMATCH");
            var p = db.rows("SELECT * FROM hot.p_progress WHERE anchor_id = ? AND as_of = current_date", HULL + "SB04A200‖S6G9").get(0);
            assertThat(p.get("match_state")).isEqualTo("MISMATCH");
            assertThat(p.get("match_pool")).isEqualTo("NONE");
            assertThat(p.get("match_lead")).isEqualTo("LATE");
            assertThat(db.count("SELECT count(*) FROM hot.p_action WHERE sub_type = 'WO_MISMATCH' AND hull_no = ? AND block_no = 'B04'", HULL))
                    .isEqualTo(1);
        }
    }

    @Test
    void 레거시_W_O_가_착수_전이면_완료_보류_held() throws SQLException {
        UUID scan = complete("B02", 100, 0.95);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> Boolean.TRUE.equals(judged(db, scan)));
            assertThat(db.one("SELECT state FROM hot.p_progress WHERE anchor_id = ? AND as_of = current_date", HULL + "SB02A200‖S6G9"))
                    .isEqualTo("held");
        }
    }

    @Test
    void 앵커가_여럿이면_블록_롤업까지만() throws SQLException {
        UUID scan = complete("B03", 60, 0.95);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> Boolean.TRUE.equals(judged(db, scan)));
            assertThat((String) db.one("SELECT judge_note FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString()))
                    .startsWith("BLOCK_ONLY");
            assertThat(db.count("SELECT count(*) FROM hot.p_progress WHERE hull_no = ? AND block_no = 'B03'", HULL)).isZero();
            var b = db.rows("SELECT * FROM hot.p_progress_block WHERE hull_no = ? AND block_no = 'B03' AND as_of = current_date", HULL).get(0);
            assertThat(b.get("wip_state")).isEqualTo("WORKING");
            assertThat(((Number) b.get("anchor_cnt")).intValue()).isEqualTo(2);
        }
    }

    @Test
    void 정합도가_모자라면_실적_대신_알림() throws SQLException {
        UUID scan = complete("B01", 100, 0.5);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> Boolean.TRUE.equals(judged(db, scan)));
            assertThat(db.one("SELECT judge_note FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString())).isEqualTo("LOW_CONFIDENCE");
            assertThat(db.count("""
                    SELECT count(*) FROM hot.p_action WHERE action_type = 'ALERT' AND sub_type = 'LOW_CONFIDENCE'
                     AND hull_no = ? AND block_no = 'B01' AND status = 'OPEN'""", HULL)).isEqualTo(1);
        }
    }

    @Test
    void 앵커가_없으면_기다렸다가_앵커가_오면_다시_판정한다() throws SQLException {
        UUID scan = complete("B98", 100, 0.95);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> Boolean.TRUE.equals(judged(db, scan)));
            var d = db.rows("SELECT judge_note, match_attempts, match_wait_until > received_ts AS waits FROM hot.s_scan_detection "
                    + "WHERE scan_id = ?", scan.toString()).get(0);
            assertThat(d.get("judge_note")).as("앵커보다 실적이 먼저 왔다 (V20 F1)").isEqualTo("WAIT_ANCHOR");
            assertThat(((Number) d.get("match_attempts")).intValue()).isEqualTo(1);
            assertThat(d.get("waits")).isEqualTo(true);
            assertThat(db.count("SELECT count(*) FROM hot.p_action WHERE sub_type = 'UNMATCHED' AND hull_no = ? AND block_no = 'B98'", HULL))
                    .as("기다리는 동안은 알림 없음").isZero();
        }
        try (Db rfc = Db.as("rfc_service")) {   // 레거시 적재가 늦게 앵커를 넣는다
            rfc.update("INSERT INTO hot.m_block (hull_no, block_no) VALUES (?, 'B98') ON CONFLICT DO NOTHING", HULL);
            anchor(rfc, HULL, "B98", "S6G9", true);
        }
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "done".equals(db.one(
                    "SELECT judge_note FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString())));
            var d = db.rows("SELECT anchor_id, match_attempts FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString()).get(0);
            assertThat(d.get("anchor_id")).isEqualTo(HULL + "SB98A200‖S6G9");
            assertThat(((Number) d.get("match_attempts")).intValue()).isEqualTo(2);
            assertThat(db.one("SELECT state FROM hot.p_progress WHERE anchor_id = ?", HULL + "SB98A200‖S6G9")).isEqualTo("done");
        }
    }

    @Test
    void 기다림이_끝나도_앵커가_없으면_UNMATCHED_확정_알림() throws SQLException {
        UUID scan = complete("B99", 100, 0.95);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> "WAIT_ANCHOR".equals(db.one(
                    "SELECT judge_note FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString())));
            // 기다림 끝 시각을 지난 것으로 당긴다
            db.update("UPDATE hot.s_scan_detection SET match_wait_until = now() - interval '1 minute' WHERE scan_id = ?", scan.toString());
            await().atMost(WAIT).until(() -> "UNMATCHED".equals(db.one(
                    "SELECT judge_note FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString())));
            assertThat(db.count("SELECT count(*) FROM hot.p_action WHERE sub_type = 'UNMATCHED' AND hull_no = ? AND block_no = 'B99'", HULL))
                    .isEqualTo(1);
        }
    }

    @Test
    void 같은_실적이_다시_와도_판정은_되돌아가지_않는다() throws SQLException {
        UUID scan = complete("B01", 100, 0.95);
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).until(() -> Boolean.TRUE.equals(judged(db, scan)));
            Object judgedTs = db.one("SELECT judged_ts FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString());

            complete(scan, "B01", 100, 0.95);   // 재전송 — 라우트가 judged · anchor_id 를 keep
            await().atMost(WAIT).until(() -> db.count("""
                    SELECT count(*) FROM hot.s_scan_detection WHERE scan_id = ? AND received_ts > judged_ts""", scan.toString()) == 1);
            assertThat(db.one("SELECT judged FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString())).isEqualTo(true);
            assertThat(db.one("SELECT judged_ts FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString())).isEqualTo(judgedTs);
        }
    }

    static Boolean judged(Db db, UUID scan) throws SQLException {
        return (Boolean) db.one("SELECT judged FROM hot.s_scan_detection WHERE scan_id = ?", scan.toString());
    }

    static UUID complete(String block, double progress, double confidence) throws SQLException {
        UUID scan = UUID.randomUUID();
        complete(scan, block, progress, confidence);
        return scan;
    }

    static void complete(UUID scan, String block, double progress, double confidence) throws SQLException {
        ZoneCdcE2eIT.actual(actualTag, scan, OffsetDateTime.now(), "COMPLETE", HULL, block, progress, confidence);
    }
}
