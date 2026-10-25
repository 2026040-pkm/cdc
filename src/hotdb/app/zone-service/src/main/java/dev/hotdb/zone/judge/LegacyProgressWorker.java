package dev.hotdb.zone.judge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.resource.WorkThreads;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 레거시 실적 판별 (도장 P · 의장 W) — 센서가 블록 단위까지만 보는 것을 레거시 실적으로 채운다.
 *
 * <pre>
 *   도장 블록 앵커(PAINT_ORDER)
 *     legacy_pct = Σ l_wo_daily.daily_mh (하위 W/O, MP B · P) ÷ target_qty (Σ212 ACTY_MH_1 공정당 1회)   ← wiki 도장 실적률
 *     cp_stats.kinds = 도장 요소(l_paint_element) 종류별 공정율 = 끝난 요소 std_mh ÷ 전체
 *   의장 GDO 앵커
 *     auto_pct   = Σ설치 ÷ Σ소요 (l_outfit_item, gdo_no = 앵커)   ← wiki GDO 설치율
 *     legacy_pct = 같은 액티비티 W/O 의 진척률 (시수 가중)
 *     p_item_state = 품목마다 INSTALLED · FIXED · TEMP · ISSUED · WAITING
 *   공통
 *     센서 판별이 이미 쓴 오늘 행(evidence 있음)은 auto_pct · state 를 건드리지 않고 legacy_pct · gap 만 채운다
 *     |auto − legacy| &gt; TRADE_RULE gap_alert_pct → gap_alert + 알림 GAP
 *     p_progress_block 블록 × 단계 롤업 — 가중치 W/O 시수, 하나라도 없으면 품목 수 (의장) / 212 시수 (도장)
 * </pre>
 *
 * <p>어느 앵커를 다시 보나: 계획이 목록 모수(±list_window_days) 에 걸친 앵커 중 오늘 레거시 판정이 없거나 refreshMinutes 가 지난 것.
 */
public class LegacyProgressWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(LegacyProgressWorker.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String RULE = "SELECT attrs FROM hot.m_code WHERE code_group = 'TRADE_RULE' AND code = ?";
    private static final String STALE = """
            SELECT u.unit_id, u.act_no, u.hull_no, u.block_no, u.site, u.plan_mh, u.target_qty, u.actl_fd
              FROM hot.l_work_unit u
              LEFT JOIN hot.p_progress p ON p.anchor_id = u.unit_id AND p.as_of = current_date
             WHERE u.anchor_type = ? AND u.trade = ? AND NOT u.excluded AND u.src_deleted_at IS NULL
               AND u.plan_sd <= current_date + ? AND coalesce(u.plan_fd, u.plan_sd) >= current_date - ?
               AND (p.anchor_id IS NULL OR p.cp_stats ->> 'legacy_at' IS NULL
                    OR (p.cp_stats ->> 'legacy_at')::timestamptz < clock_timestamp() - make_interval(mins => ?)
                    OR u.mnt_ts > (p.cp_stats ->> 'legacy_at')::timestamptz)
             ORDER BY (p.cp_stats ->> 'legacy_at') NULLS FIRST, u.plan_sd
             LIMIT ?
            """;
    // 도장 — 하위 W/O 의 일별 실적 시수 합 · 레거시 완료
    private static final String PAINT_DAILY = """
            SELECT w.parent_id, coalesce(sum(d.mh), 0) AS mh, count(*) AS wo_cnt, bool_and(w.actl_fd IS NOT NULL) AS all_done
              FROM hot.l_work_unit w
              LEFT JOIN (SELECT wo_no, sum(daily_mh) AS mh FROM hot.l_wo_daily WHERE src_deleted_at IS NULL GROUP BY wo_no) d
                     ON d.wo_no = w.unit_id
             WHERE w.level = 'WO' AND w.parent_id = ANY (?) AND w.mp IN ('B', 'P') AND w.src_deleted_at IS NULL
             GROUP BY w.parent_id
            """;
    private static final String PAINT_ELEM = """
            SELECT anchor_id, kind, count(*) AS cnt, count(done_date) AS done_cnt,
                   coalesce(sum(std_mh), 0) AS total_mh, coalesce(sum(std_mh) FILTER (WHERE done_date IS NOT NULL), 0) AS done_mh
              FROM hot.l_paint_element WHERE anchor_id = ANY (?) AND src_deleted_at IS NULL GROUP BY anchor_id, kind ORDER BY anchor_id, kind
            """;
    // 의장 — 같은 액티비티 W/O
    private static final String OFT_WO = """
            SELECT act_no, sum(plan_mh) AS mh, sum(acc_rate * plan_mh) / NULLIF(sum(plan_mh), 0) AS rate,
                   bool_and(actl_fd IS NOT NULL) AS all_done
              FROM hot.l_work_unit WHERE level = 'WO' AND trade = 'W' AND act_no = ANY (?) AND src_deleted_at IS NULL
             GROUP BY act_no
            """;
    private static final String OFT_ITEMS = """
            SELECT gdo_no, rsnum, rspos, hull_no, block_no, req_qty, inst_qty, fixed_qty, temp_qty, issue_ts, inst_date
              FROM hot.l_outfit_item WHERE gdo_no = ANY (?) AND src_deleted_at IS NULL
            """;
    private static final String PROGRESS = """
            INSERT INTO hot.p_progress AS p (anchor_id, as_of, hull_no, block_no, anchor_type, state, auto_pct, legacy_pct, gap_pct,
                                             planned_qty, done_qty, cp_stats, weight_mh, site, excluded, gap_alert, match_state, judged_at)
            VALUES (?, current_date, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?, false, ?, 'MATCHED', clock_timestamp())
            ON CONFLICT (anchor_id, as_of) DO UPDATE SET
                 state = CASE WHEN p.state = 'confirmed' OR p.evidence IS NOT NULL THEN p.state ELSE EXCLUDED.state END,
                 auto_pct = CASE WHEN p.evidence IS NOT NULL THEN p.auto_pct ELSE EXCLUDED.auto_pct END,
                 legacy_pct = EXCLUDED.legacy_pct,
                 gap_pct = CASE WHEN p.evidence IS NOT NULL THEN p.auto_pct - EXCLUDED.legacy_pct ELSE EXCLUDED.gap_pct END,
                 gap_alert = CASE WHEN p.evidence IS NOT NULL THEN abs(coalesce(p.auto_pct - EXCLUDED.legacy_pct, 0)) > ?
                                  ELSE EXCLUDED.gap_alert END,
                 planned_qty = EXCLUDED.planned_qty, done_qty = EXCLUDED.done_qty,
                 cp_stats = coalesce(p.cp_stats, '{}'::jsonb) || EXCLUDED.cp_stats,
                 weight_mh = EXCLUDED.weight_mh, site = EXCLUDED.site,
                 excluded = CASE WHEN p.exclude_reason = 'SOURCE_DELETED' THEN false ELSE p.excluded END,
                 exclude_reason = NULLIF(p.exclude_reason, 'SOURCE_DELETED'),
                 judged_at = clock_timestamp()
            RETURNING gap_alert, gap_pct
            """;
    private static final String ITEM = """
            INSERT INTO hot.p_item_state AS s (hull_no, block_no, item_type, item_id, anchor_id, state, legacy_ts, work_stage, state_history)
            VALUES (?, ?, 'OUTFIT_ITEM', ?, ?, ?, ?, ?, jsonb_build_array(jsonb_build_object('state', ?::text, 'at', clock_timestamp())))
            ON CONFLICT (hull_no, block_no, item_type, item_id) DO UPDATE SET
                 anchor_id = EXCLUDED.anchor_id, legacy_ts = EXCLUDED.legacy_ts, work_stage = EXCLUDED.work_stage,
                 state_history = CASE WHEN s.state IS DISTINCT FROM EXCLUDED.state
                                      THEN coalesce(s.state_history, '[]'::jsonb) || EXCLUDED.state_history ELSE s.state_history END,
                 state = EXCLUDED.state
            """;
    // 원천에서 지워진 앵커의 실적 → 제외하고, 그 블록 롤업을 다시 낸다 (V20 F4). 1분에 한 번
    private static final String EXCLUDE_DELETED = """
            UPDATE hot.p_progress p SET excluded = true, exclude_reason = 'SOURCE_DELETED', judged_at = clock_timestamp()
              FROM hot.l_work_unit u
             WHERE u.unit_id = p.anchor_id AND u.anchor_type = ? AND u.src_deleted_at IS NOT NULL AND NOT p.excluded
            RETURNING p.hull_no || '‖' || p.block_no
            """;
    // 원천에서 지워진 의장 품목의 상태
    private static final String ITEM_DELETED = """
            UPDATE hot.p_item_state s SET state = 'SOURCE_DELETED',
                   state_history = coalesce(s.state_history, '[]'::jsonb)
                                   || jsonb_build_array(jsonb_build_object('state', 'SOURCE_DELETED', 'at', clock_timestamp()))
              FROM hot.l_outfit_item i
             WHERE i.src_deleted_at IS NOT NULL AND s.item_type = 'OUTFIT_ITEM' AND s.item_id = i.rsnum || '‖' || i.rspos
               AND s.hull_no = i.hull_no AND s.block_no = i.block_no AND s.state IS DISTINCT FROM 'SOURCE_DELETED'
            """;
    private static final String ACTION = """
            INSERT INTO hot.p_action (action_type, hull_no, block_no, anchor_id, sub_type, detail, value_pct)
            SELECT 'ALERT', ?, ?, ?, 'GAP', CAST(? AS jsonb), ?
             WHERE NOT EXISTS (SELECT 1 FROM hot.p_action a WHERE a.anchor_id = ? AND a.sub_type = 'GAP' AND a.status = 'OPEN')
            """;
    // 블록 × 단계 롤업. 단계 = 앵커의 stage (의장 ZZSTAGE_CODE), 없으면 공종 단계 이름
    static final String ROLLUP = """
            WITH a AS (
                SELECT p.hull_no, p.block_no, coalesce(u.stage, ?) AS stage, p.state, p.auto_pct, p.legacy_pct, p.weight_mh,
                       greatest(coalesce((p.cp_stats ->> 'item_cnt')::numeric, 0), 1) AS items
                  FROM hot.p_progress p JOIN hot.l_work_unit u ON u.unit_id = p.anchor_id
                 WHERE p.as_of = current_date AND p.anchor_type = ? AND NOT p.excluded AND p.hull_no || '‖' || p.block_no = ANY (?)),
            b AS (
                SELECT a.*, bool_and(coalesce(weight_mh, 0) > 0) OVER (PARTITION BY hull_no, block_no, stage) AS by_mh FROM a),
            c AS (
                SELECT b.*, CASE WHEN by_mh THEN weight_mh ELSE items END AS w FROM b)
            INSERT INTO hot.p_progress_block (hull_no, block_no, trade, stage, as_of, auto_pct, legacy_pct, weight_basis, anchor_cnt,
                                              open_action_cnt, bts_lot, wip_state, updated_at)
            SELECT hull_no, block_no, ?, stage, current_date,
                   round(sum(auto_pct * w) FILTER (WHERE auto_pct IS NOT NULL) / NULLIF(sum(w) FILTER (WHERE auto_pct IS NOT NULL), 0), 1),
                   round(sum(legacy_pct * w) FILTER (WHERE legacy_pct IS NOT NULL) / NULLIF(sum(w) FILTER (WHERE legacy_pct IS NOT NULL), 0), 1),
                   CASE WHEN bool_and(by_mh) THEN ? ELSE 'ITEMS' END, count(*),
                   (SELECT count(*) FROM hot.p_action x WHERE x.status = 'OPEN' AND x.hull_no = c.hull_no AND x.block_no = c.block_no),
                   (SELECT m.bts_lot FROM hot.m_block m WHERE m.hull_no = c.hull_no AND m.block_no = c.block_no),
                   CASE WHEN bool_and(state IN ('done', 'confirmed')) THEN 'DONE'
                        WHEN bool_or(state <> 'notDue') THEN 'WORKING' ELSE 'WAITING' END,
                   clock_timestamp()
              FROM c GROUP BY hull_no, block_no, stage
            ON CONFLICT (hull_no, block_no, trade, stage, as_of) DO UPDATE SET
                 auto_pct = GREATEST(hot.p_progress_block.auto_pct, EXCLUDED.auto_pct), legacy_pct = EXCLUDED.legacy_pct,
                 weight_basis = EXCLUDED.weight_basis, anchor_cnt = EXCLUDED.anchor_cnt,
                 open_action_cnt = EXCLUDED.open_action_cnt, bts_lot = EXCLUDED.bts_lot,
                 wip_state = CASE WHEN hot.p_progress_block.wip_state = 'DONE' THEN 'DONE' ELSE EXCLUDED.wip_state END,
                 updated_at = clock_timestamp()
            """;

    private final String module;
    private final JudgementProperties.Legacy props;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;
    private final AtomicLong lastBatch = new AtomicLong();
    private ModuleTrade trade;
    private int windowDays = 30;
    private double gapLimit = 10;
    private double donePct = 100;
    private ScheduledExecutorService scheduler;
    private long lastExcludeMs;

    record Anchor(String unitId, String actNo, String hullNo, String blockNo, String site, Double planMh, Double targetQty,
                  boolean legacyDone) {}

    /** 한 앵커의 판정 결과 (p_progress 한 행). */
    record Row(Anchor a, String state, Double auto, Double legacy, Double planned, Double done, Map<String, Object> stats,
               Double weight) {}

    public LegacyProgressWorker(String module, JudgementProperties.Legacy props, JdbcTemplate jdbc, TransactionTemplate tx,
                                MeterRegistry meters) {
        this.module = module;
        this.props = props;
        this.jdbc = jdbc;
        this.tx = tx;
        this.meters = meters;
        for (String s : List.of("notDue", "inProgress", "done")) {
            meters.counter("hotdb.judge.legacy.judged", "status", s);
        }
        meters.counter("hotdb.judge.legacy.gap");
        Gauge.builder("hotdb.judge.legacy.last.batch", lastBatch, AtomicLong::get)
                .description("마지막 주기에 다시 판정한 앵커 수 (레거시 실적)").register(meters);
    }

    void loadDictionary() {
        trade = ModuleTrade.load(jdbc, module);
        if (!List.of("P", "W").contains(trade.trade())) {
            throw new IllegalStateException("레거시 실적 판별은 도장(P) · 의장(W) 만 — 모듈 " + module + " 공종 " + trade.trade());
        }
        JsonNode r = json(jdbc.query(RULE, rs -> rs.next() ? rs.getString(1) : null, trade.trade()));
        windowDays = r.path("list_window_days").asInt(30);
        gapLimit = r.path("gap_alert_pct").asDouble(10);
        donePct = r.path("done_pct").asDouble(100);
        log.info("레거시 실적 판별 사전 — 공종 {} · 앵커 {} · 목록 모수 ±{}일 · 차이 알림 {}%p", trade.trade(), trade.anchorType(),
                windowDays, gapLimit);
    }

    /** 한 주기. @return 다시 판정한 앵커 수 */
    public int judgeOnce() {
        if (trade == null) {
            loadDictionary();
        }
        excludeDeleted();
        List<Anchor> anchors = jdbc.query(STALE, (rs, i) -> new Anchor(rs.getString("unit_id"), rs.getString("act_no"),
                        rs.getString("hull_no"), rs.getString("block_no"), rs.getString("site"), dbl(rs.getObject("plan_mh")),
                        dbl(rs.getObject("target_qty")), rs.getDate("actl_fd") != null),
                trade.anchorType(), trade.trade(), windowDays, windowDays, props.refreshMinutes(), props.batchSize());
        lastBatch.set(anchors.size());
        if (anchors.isEmpty()) {
            return 0;
        }
        List<Row> rows = new ArrayList<>();
        List<Object[]> items = new ArrayList<>();
        if ("P".equals(trade.trade())) {
            paint(anchors, rows);
        } else {
            outfit(anchors, rows, items);
        }
        Map<String, Integer> counts = new LinkedHashMap<>();
        Set<String> blocks = new LinkedHashSet<>();
        String now = Instant.now().toString();
        tx.executeWithoutResult(s -> {
            for (Row r : rows) {
                Anchor a = r.a();
                r.stats().put("legacy_at", now);
                Double gap = r.auto() == null || r.legacy() == null ? null : Math.round(10 * (r.auto() - r.legacy())) / 10.0;
                boolean gapAlert = LegacyRules.gapAlert(r.auto(), r.legacy(), gapLimit);
                Map<String, Object> res = jdbc.queryForMap(PROGRESS, a.unitId(), a.hullNo(), a.blockNo(), trade.anchorType(),
                        r.state(), r.auto(), r.legacy(), gap, r.planned(), r.done(), toJson(r.stats()), r.weight(), a.site(),
                        gapAlert, gapLimit);
                if (Boolean.TRUE.equals(res.get("gap_alert"))) {
                    meters.counter("hotdb.judge.legacy.gap").increment();
                    jdbc.update(ACTION, a.hullNo(), a.blockNo(), a.unitId(),
                            toJson(Map.of("module", module, "auto", String.valueOf(r.auto()), "legacy", String.valueOf(r.legacy()),
                                    "limit", gapLimit)), res.get("gap_pct"), a.unitId());
                }
                counts.merge(r.state(), 1, Integer::sum);
                if (a.hullNo() != null && a.blockNo() != null) {
                    blocks.add(a.hullNo() + "‖" + a.blockNo());
                }
            }
            if (!items.isEmpty()) {
                jdbc.batchUpdate(ITEM, items);
            }
            if (!blocks.isEmpty()) {
                jdbc.update(ROLLUP, trade.stage(), trade.anchorType(), blocks.toArray(String[]::new), trade.trade(),
                        "P".equals(trade.trade()) ? "MH_212" : "WO_MH");
            }
        });
        counts.forEach((k, v) -> meters.counter("hotdb.judge.legacy.judged", "status", k).increment(v));
        log.info("레거시 실적 판별 — 앵커 {} ({}) · 품목 {} · 블록 롤업 {}", anchors.size(), counts, items.size(), blocks.size());
        return anchors.size();
    }

    /** 원천에서 지워진 앵커 실적 · 의장 품목 상태를 표시하고 그 블록 롤업을 다시 낸다. 1분에 한 번. */
    void excludeDeleted() {
        if (System.currentTimeMillis() - lastExcludeMs < 60_000) {
            return;
        }
        lastExcludeMs = System.currentTimeMillis();
        tx.executeWithoutResult(s -> {
            List<String> blocks = jdbc.queryForList(EXCLUDE_DELETED, String.class, trade.anchorType());
            int items = "W".equals(trade.trade()) ? jdbc.update(ITEM_DELETED) : 0;
            if (!blocks.isEmpty()) {
                jdbc.update(ROLLUP, trade.stage(), trade.anchorType(),
                        blocks.stream().filter(java.util.Objects::nonNull).distinct().toArray(String[]::new), trade.trade(),
                        "P".equals(trade.trade()) ? "MH_212" : "WO_MH");
            }
            if (!blocks.isEmpty() || items > 0) {
                log.info("원천에서 지워진 앵커 {} · 품목 {} — 실적 제외 · 품목 SOURCE_DELETED · 블록 롤업 다시", blocks.size(), items);
            }
        });
    }

    private void paint(List<Anchor> anchors, List<Row> rows) {
        String[] ids = anchors.stream().map(Anchor::unitId).toArray(String[]::new);
        Map<String, Object[]> daily = new HashMap<>();
        jdbc.query(PAINT_DAILY, rs -> {
            daily.put(rs.getString(1), new Object[] {rs.getDouble("mh"), rs.getInt("wo_cnt"), rs.getBoolean("all_done")});
        }, (Object) ids);
        Map<String, Map<String, Object>> kinds = new HashMap<>();
        jdbc.query(PAINT_ELEM, rs -> {
            Map<String, Object> k = new LinkedHashMap<>();
            double total = rs.getDouble("total_mh");
            double done = rs.getDouble("done_mh");
            k.put("pct", LegacyRules.pct(done, total));
            k.put("done_mh", done);
            k.put("total_mh", total);
            k.put("done", rs.getInt("done_cnt"));
            k.put("cnt", rs.getInt("cnt"));
            kinds.computeIfAbsent(rs.getString("anchor_id"), x -> new LinkedHashMap<>()).put(rs.getString("kind"), k);
        }, (Object) ids);
        for (Anchor a : anchors) {
            Object[] d = daily.get(a.unitId());
            double mh = d == null ? 0 : (double) d[0];
            boolean allDone = a.legacyDone() || (d != null && (boolean) d[2]);
            Double legacy = LegacyRules.paintPct(mh, a.targetQty());
            Map<String, Object> st = new LinkedHashMap<>();
            st.put("basis", "MH_212");
            st.put("daily_mh", mh);
            st.put("target_mh", a.targetQty());
            st.put("wo_cnt", d == null ? 0 : d[1]);
            st.put("kinds", kinds.getOrDefault(a.unitId(), Map.of()));
            rows.add(new Row(a, LegacyRules.state(legacy, allDone, donePct), null, legacy, a.targetQty(), mh, st, a.targetQty()));
        }
    }

    private void outfit(List<Anchor> anchors, List<Row> rows, List<Object[]> items) {
        String[] ids = anchors.stream().map(Anchor::unitId).toArray(String[]::new);
        String[] acts = anchors.stream().map(Anchor::actNo).filter(x -> x != null).distinct().toArray(String[]::new);
        Map<String, Object[]> wo = new HashMap<>();
        jdbc.query(OFT_WO, rs -> {
            wo.put(rs.getString("act_no"), new Object[] {dbl(rs.getObject("mh")), dbl(rs.getObject("rate")), rs.getBoolean("all_done")});
        }, (Object) acts);
        Map<String, double[]> sums = new HashMap<>();   // req · inst · item 수
        Map<String, Map<String, Integer>> byState = new HashMap<>();
        Map<String, String> stage = new HashMap<>();
        jdbc.query("SELECT unit_id, stage FROM hot.l_work_unit WHERE unit_id = ANY (?)",
                rs -> { stage.put(rs.getString(1), rs.getString(2)); }, (Object) ids);
        jdbc.query(OFT_ITEMS, rs -> {
            String g = rs.getString("gdo_no");
            Double req = dbl(rs.getObject("req_qty"));
            Double inst = dbl(rs.getObject("inst_qty"));
            Timestamp issue = rs.getTimestamp("issue_ts");
            Date instDate = rs.getDate("inst_date");
            String state = LegacyRules.itemState(req, inst, dbl(rs.getObject("fixed_qty")), dbl(rs.getObject("temp_qty")),
                    issue != null);
            double[] s = sums.computeIfAbsent(g, k -> new double[3]);
            if (!"NA".equals(state)) {
                s[0] += req;
                s[1] += Math.min(inst == null ? 0 : inst, req);
                s[2] += 1;
            }
            byState.computeIfAbsent(g, k -> new LinkedHashMap<>()).merge(state, 1, Integer::sum);
            String hull = rs.getString("hull_no");
            String block = rs.getString("block_no");
            if (hull != null && block != null) {
                Timestamp legacyTs = instDate != null ? new Timestamp(instDate.getTime()) : issue;
                items.add(new Object[] {hull, block, rs.getString("rsnum") + "‖" + rs.getString("rspos"), g, state, legacyTs,
                        stage.get(g), state});
            }
        }, (Object) ids);
        for (Anchor a : anchors) {
            double[] s = sums.get(a.unitId());
            Object[] w = a.actNo() == null ? null : wo.get(a.actNo());
            Double auto = s == null ? null : LegacyRules.pct(s[1], s[0]);
            Double legacy = w == null || w[1] == null ? null : Math.round(10 * (Double) w[1]) / 10.0;
            boolean legacyDone = a.legacyDone() || (w != null && (boolean) w[2]);
            // 품목이 있으면 품목 전부 설치가 완료 (state_rule ITEMS_ALL), 없으면 레거시 진척 · 완료일
            String state = auto != null ? LegacyRules.state(auto, false, donePct) : LegacyRules.state(legacy, legacyDone, donePct);
            Map<String, Object> st = new LinkedHashMap<>();
            st.put("basis", auto != null ? "ITEMS" : "WO");
            st.put("item_cnt", s == null ? 0 : (int) s[2]);
            st.put("items", byState.getOrDefault(a.unitId(), Map.of()));
            st.put("wo_mh", w == null ? null : w[0]);
            rows.add(new Row(a, state, auto, legacy, s == null ? null : s[0], s == null ? null : s[1], st,
                    w == null ? null : (Double) w[0]));
        }
    }

    private static JsonNode json(String s) {
        try {
            return s == null ? JSON.createObjectNode() : JSON.readTree(s);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("attrs 가 JSON 이 아니다: " + s, e);
        }
    }

    private static String toJson(Object o) {
        try {
            return JSON.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Double dbl(Object v) {
        return v == null ? null : ((Number) v).doubleValue();
    }

    @Override
    public void start() {
        log.info("레거시 실적 판별 시작 — 모듈 {} · {}ms 마다 앵커 최대 {}", module, props.intervalMs(), props.batchSize());
        scheduler = Executors.newSingleThreadScheduledExecutor(WorkThreads.factory("judge", "legacy-" + module, meters));
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                judgeOnce();
            } catch (RuntimeException e) {
                log.warn("레거시 실적 판별 주기 실패: {}", e.toString());
            }
        }, props.intervalMs(), props.intervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    @Override
    public boolean isRunning() {
        return scheduler != null;
    }
}
