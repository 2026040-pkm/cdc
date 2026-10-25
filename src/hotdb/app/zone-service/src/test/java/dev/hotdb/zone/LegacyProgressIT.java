package dev.hotdb.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 레거시 실적 판별 — 기동 중인 zone-pnt · zone-oft 의 LegacyProgressWorker 가 레거시 적재 표(l_*)로 p_progress 를 쓴다.
 *
 * <pre>
 *   도장: Σ l_wo_daily(하위 W/O, MP B) 832 ÷ 앵커 target_qty(Σ212) 1000 = 83.2 · 요소 A 공정율 10 ÷ 40 = 25
 *   의장: 품목 설치 4 ÷ 소요 10 = 40 (auto) · 같은 액티비티 W/O 진척 70 (legacy) → 차이 30 &gt; 10 → GAP 알림 · 품목 상태 2건
 * </pre>
 *
 * <p>판별 주기(JUDGE_LEGACY_INTERVAL_MS, 기본 60초) 를 기다리므로 넉넉히 기다린다.
 */
@EnabledIfSystemProperty(named = "hotdb.it", matches = "true")
class LegacyProgressIT {

    static final Duration WAIT = Duration.ofSeconds(150);
    static final String RUN = UUID.randomUUID().toString().substring(0, 4).toUpperCase();
    static final String HULL = "L" + RUN;
    static final String PBLOCK = HULL + "‖P01‖P";
    static final String GDO = "IT" + RUN + "‖0010";
    static final String ACT = HULL + "W01ACT01";

    @BeforeAll
    static void 레거시_적재_표를_채운다() throws SQLException {
        try (Db rfc = Db.as("rfc_service")) {
            // 도장 블록 앵커 + 하위 W/O 둘 (MP B) + 일별 실적 + 도장 요소
            rfc.update("""
                    INSERT INTO hot.l_work_unit (unit_id, level, hull_no, block_no, trade, plan_mh, site, source_table, anchor_type,
                                                 target_qty, target_basis, plan_sd, plan_fd)
                    VALUES (?, 'PBLOCK', ?, 'P01', 'P', 900, 'IN', 'YPWG411M', 'PAINT_ORDER', 1000, 'MH_212', current_date - 3, current_date + 3)
                    """, PBLOCK, HULL);
            for (String[] w : new String[][] {{"1", "300"}, {"2", "532"}}) {
                String wo = HULL + "P01PNT0" + w[0];
                rfc.update("""
                        INSERT INTO hot.l_work_unit (unit_id, level, parent_id, hull_no, block_no, trade, mp, plan_mh, site, source_table,
                                                     plan_sd, plan_fd)
                        VALUES (?, 'WO', ?, ?, 'P01', 'P', 'B', 450, 'IN', 'YPWG411M', current_date - 3, current_date + 3)
                        """, wo, PBLOCK, HULL);
                rfc.update("INSERT INTO hot.l_wo_daily (wo_no, actl_date, org_code, daily_mh) VALUES (?, current_date - 1, 'IT', ?)",
                        wo, Double.parseDouble(w[1]));
            }
            rfc.update("""
                    INSERT INTO hot.l_paint_element (anchor_id, kind, zone_code, elem_code, seq, std_mh, done_date) VALUES
                      (?, 'A', 'Z1', 'E1', '1', 10, current_date - 1), (?, 'A', 'Z1', 'E2', '2', 30, NULL)
                    """, PBLOCK, PBLOCK);

            // 의장 GDO 앵커 + 같은 액티비티 W/O + 품목 둘
            rfc.update("""
                    INSERT INTO hot.l_work_unit (unit_id, level, act_no, hull_no, block_no, trade, stage, site, source_table, anchor_type,
                                                 target_qty, target_basis, plan_sd, plan_fd)
                    VALUES (?, 'ORDER', ?, ?, 'W01', 'W', 'S1', 'IN', 'ZPPT200', 'GDO', 10, 'ITEMS', current_date - 3, current_date + 3)
                    """, GDO, ACT, HULL);
            rfc.update("""
                    INSERT INTO hot.l_work_unit (unit_id, level, act_no, hull_no, block_no, trade, plan_mh, acc_rate, site, source_table)
                    VALUES (?, 'WO', ?, ?, 'W01', 'W', 100, 70, 'IN', 'YPWG411M')
                    """, ACT + "0001", ACT, HULL);
            rfc.update("""
                    INSERT INTO hot.l_outfit_item (rsnum, rspos, hull_no, block_no, matnr, req_qty, inst_qty, gdo_no, issue_ts) VALUES
                      (?, '0001', ?, 'W01', 'M1', 4, 4, ?, now()), (?, '0002', ?, 'W01', 'M2', 6, 0, ?, now())
                    """, "IT" + RUN, HULL, GDO, "IT" + RUN, HULL, GDO);
        }
    }

    @Test
    void 도장_블록_실적률은_413_합_나누기_212_합() throws SQLException {
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).pollInterval(Duration.ofSeconds(3)).until(() ->
                    db.count("SELECT count(*) FROM hot.p_progress WHERE anchor_id = ? AND as_of = current_date", PBLOCK) == 1);
            var p = db.rows("SELECT * FROM hot.p_progress WHERE anchor_id = ? AND as_of = current_date", PBLOCK).get(0);
            assertThat(((Number) p.get("legacy_pct")).doubleValue()).isEqualTo(83.2);
            assertThat(p.get("state")).isEqualTo("inProgress");
            assertThat(p.get("anchor_type")).isEqualTo("PAINT_ORDER");
            assertThat(p.get("cp_stats").toString()).contains("\"basis\": \"MH_212\"").contains("\"pct\": 25.0");
            var b = db.rows("SELECT * FROM hot.p_progress_block WHERE hull_no = ? AND block_no = 'P01' AND trade = 'P' AND as_of = current_date",
                    HULL).get(0);
            assertThat(b.get("stage")).isEqualTo("PAINT");
            assertThat(((Number) b.get("legacy_pct")).doubleValue()).isEqualTo(83.2);
            assertThat(b.get("weight_basis")).isEqualTo("MH_212");
        }
    }

    @Test
    void 의장_GDO_설치율과_레거시_차이_알림_품목_상태() throws SQLException {
        try (Db db = Db.as("postgres")) {
            await().atMost(WAIT).pollInterval(Duration.ofSeconds(3)).until(() ->
                    db.count("SELECT count(*) FROM hot.p_progress WHERE anchor_id = ? AND as_of = current_date", GDO) == 1);
            var p = db.rows("SELECT * FROM hot.p_progress WHERE anchor_id = ? AND as_of = current_date", GDO).get(0);
            assertThat(((Number) p.get("auto_pct")).doubleValue()).isEqualTo(40.0);
            assertThat(((Number) p.get("legacy_pct")).doubleValue()).isEqualTo(70.0);
            assertThat(((Number) p.get("gap_pct")).doubleValue()).isEqualTo(-30.0);
            assertThat(p.get("gap_alert")).isEqualTo(true);
            assertThat(db.count("SELECT count(*) FROM hot.p_action WHERE anchor_id = ? AND sub_type = 'GAP' AND status = 'OPEN'", GDO))
                    .isEqualTo(1);
            assertThat(db.rows("""
                    SELECT item_id, state, work_stage FROM hot.p_item_state WHERE anchor_id = ? ORDER BY item_id""", GDO))
                    .extracting(r -> r.get("state") + "/" + r.get("work_stage"))
                    .containsExactly("INSTALLED/S1", "ISSUED/S1");
            var b = db.rows("SELECT * FROM hot.p_progress_block WHERE hull_no = ? AND block_no = 'W01' AND trade = 'W' AND stage = 'S1' "
                    + "AND as_of = current_date", HULL).get(0);
            assertThat(((Number) b.get("auto_pct")).doubleValue()).isEqualTo(40.0);
            assertThat(b.get("weight_basis")).isEqualTo("WO_MH");
            assertThat(((Number) b.get("open_action_cnt")).intValue()).isGreaterThanOrEqualTo(1);
        }
    }
}
