package dev.hotdb.zone.judge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hotdb.cdc.resource.WorkThreads;
import dev.hotdb.zone.judge.CheckpointEvaluator.Checkpoint;
import dev.hotdb.zone.judge.CheckpointEvaluator.PartStat;
import dev.hotdb.zone.judge.CheckpointEvaluator.Reached;
import dev.hotdb.zone.judge.CheckpointEvaluator.Result;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
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
 * 절점 판별 단계 (가공) — 센서 대신 레거시 절점으로 실적을 판정한다.
 *
 * <pre>
 *   앵커(l_work_unit, DWG) ── l_event(절점 1~7 시각) + l_cut_part(절점 8~10 부재) ──▶ CheckpointEvaluator
 *      ──▶ p_progress (앵커 × 오늘: last_cp · state · auto_pct · legacy_pct · cp_history · cp_stats)
 *      ──▶ p_progress_block (블록 × 가공 × CUT × 오늘: 시수 가중 도면 완료율 = 공식 가공 실적률)
 *      ──▶ p_action ORDER_REVERSED (앞 절점 없이 뒤 절점이 찍힘) · CARRY_OVERDUE (강재 반입예정 경과 · 불출 전)
 *   cp_plan · late_cp_cnt — 마킹(절단계획일) · 절단 완료(절단완료 계획일)의 계획일과 늦은 절점 수
 *   블록 롤업에 열린 알림 수 · BTS 지번(m_block)
 * </pre>
 *
 * <p>어느 앵커를 다시 보나: 목록 모수(절단계획 ±N일, TRADE_RULE attrs.list_window_days) 안의 앵커 중 오늘 행이 없거나
 * 사건 · 부재 · 앵커가 마지막 판정 뒤에 바뀐 것. 한 주기에 batchSize 개까지.
 */
public class CheckpointWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(CheckpointWorker.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String CHECKPOINTS = """
            SELECT code, seq, attrs FROM hot.m_code
             WHERE code_group = 'CHECKPOINT' AND parent_code = ? AND in_scope ORDER BY seq
            """;
    private static final String RULE = "SELECT attrs FROM hot.m_code WHERE code_group = 'TRADE_RULE' AND code = ?";
    private static final String STALE = """
            SELECT u.unit_id, u.hull_no, u.block_no, u.site, u.plan_mh, u.target_qty, u.plan_sd, u.plan_fd
              FROM hot.l_work_unit u
              LEFT JOIN hot.p_progress p ON p.anchor_id = u.unit_id AND p.as_of = current_date
             WHERE u.anchor_type = ? AND u.trade = ? AND NOT u.excluded AND u.src_deleted_at IS NULL
               AND u.plan_sd BETWEEN current_date - ? AND current_date + ?
               AND (p.anchor_id IS NULL
                    OR u.mnt_ts > p.judged_at
                    OR EXISTS (SELECT 1 FROM hot.l_event e WHERE e.unit_id = u.unit_id AND e.mnt_ts > p.judged_at)
                    OR EXISTS (SELECT 1 FROM hot.l_cut_part c WHERE c.dwg_no = u.unit_id AND c.mnt_ts > p.judged_at))
             ORDER BY p.judged_at NULLS FIRST, u.plan_sd
             LIMIT ?
            """;
    private static final String EVENTS = """
            SELECT DISTINCT ON (unit_id, event_code) unit_id, event_code, event_ts, source, conf
              FROM hot.l_event WHERE event_group = 'CHECKPOINT' AND unit_id = ANY (?) AND src_deleted_at IS NULL
             ORDER BY unit_id, event_code, event_ts DESC
            """;
    private static final String CARY = """
            SELECT unit_id, max(event_ts) FROM hot.l_event WHERE event_code = 'CARY_DUE' AND unit_id = ANY (?) AND src_deleted_at IS NULL
             GROUP BY unit_id
            """;
    private static final String PARTS = """
            SELECT dwg_no,
                   count(*) FILTER (WHERE bend_flag) AS bend_target, count(*) FILTER (WHERE bend_flag AND bend_done_ts IS NOT NULL) AS bend_done,
                   count(*) AS part_cnt, count(group_ts) AS group_done, count(issue_ts) AS issue_done,
                   count(*) FILTER (WHERE grind_flag) AS grind_target, count(*) FILTER (WHERE grind_flag AND grind_done_ts IS NOT NULL) AS grind_done
              FROM hot.l_cut_part WHERE dwg_no = ANY (?) AND src_deleted_at IS NULL GROUP BY dwg_no
            """;
    private static final String LEGACY = """
            SELECT ref_no, max(acc_rate) FROM hot.l_work_unit
             WHERE level = 'WO' AND ref_no = ANY (?) AND step_code IN ('CT', 'CM') AND src_deleted_at IS NULL GROUP BY ref_no
            """;
    private static final String PROGRESS = """
            INSERT INTO hot.p_progress AS p (anchor_id, as_of, hull_no, block_no, anchor_type, state, auto_pct, legacy_pct, gap_pct,
                                        planned_qty, done_qty, last_cp, cp_history, cp_stats, weight_mh, site, excluded,
                                        match_state, cp_plan, late_cp_cnt, judged_at)
            VALUES (?, current_date, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?, ?, false, 'MATCHED',
                    CAST(? AS jsonb), ?, clock_timestamp())
            ON CONFLICT (anchor_id, as_of) DO UPDATE SET
                 state = CASE WHEN p.state = 'confirmed' THEN p.state ELSE EXCLUDED.state END,
                 auto_pct = EXCLUDED.auto_pct, legacy_pct = EXCLUDED.legacy_pct, gap_pct = EXCLUDED.gap_pct,
                 excluded = CASE WHEN p.exclude_reason = 'SOURCE_DELETED' THEN false ELSE p.excluded END,
                 exclude_reason = NULLIF(p.exclude_reason, 'SOURCE_DELETED'),
                 planned_qty = EXCLUDED.planned_qty, done_qty = EXCLUDED.done_qty, last_cp = EXCLUDED.last_cp,
                 cp_history = EXCLUDED.cp_history, cp_stats = EXCLUDED.cp_stats, weight_mh = EXCLUDED.weight_mh,
                 site = EXCLUDED.site, cp_plan = EXCLUDED.cp_plan, late_cp_cnt = EXCLUDED.late_cp_cnt,
                 judged_at = clock_timestamp()
            """;
    // 블록 롤업 — 시수 가중 도면 완료율 (공식 가공 실적률). 분모 = 블록의 제외 아닌 DWG 앵커 전부 (오늘 행이 있는 것)
    private static final String ROLLUP = """
            INSERT INTO hot.p_progress_block (hull_no, block_no, trade, stage, as_of, auto_pct, legacy_pct, weight_basis,
                                              anchor_cnt, wip_state, open_action_cnt, bts_lot, updated_at)
            SELECT hull_no, block_no, ?, ?, current_date,
                   round(100.0 * sum(CASE WHEN state IN ('done', 'confirmed') THEN coalesce(weight_mh, 0) ELSE 0 END)
                         / NULLIF(sum(coalesce(weight_mh, 0)), 0), 1),
                   round(sum(coalesce(legacy_pct, 0) * coalesce(weight_mh, 0)) / NULLIF(sum(coalesce(weight_mh, 0)), 0), 1),
                   'DWG_MH', count(*),
                   CASE WHEN bool_and(state IN ('done', 'confirmed')) THEN 'DONE'
                        WHEN bool_or(state <> 'notDue') THEN 'WORKING' ELSE 'WAITING' END,
                   (SELECT count(*) FROM hot.p_action x WHERE x.status = 'OPEN' AND x.hull_no = p.hull_no AND x.block_no = p.block_no),
                   (SELECT m.bts_lot FROM hot.m_block m WHERE m.hull_no = p.hull_no AND m.block_no = p.block_no),
                   clock_timestamp()
              FROM hot.p_progress p
             WHERE as_of = current_date AND anchor_type = ? AND NOT excluded AND hull_no || '‖' || block_no = ANY (?)
             GROUP BY hull_no, block_no
            ON CONFLICT (hull_no, block_no, trade, stage, as_of) DO UPDATE SET
                 auto_pct = EXCLUDED.auto_pct, legacy_pct = EXCLUDED.legacy_pct, weight_basis = EXCLUDED.weight_basis,
                 anchor_cnt = EXCLUDED.anchor_cnt, wip_state = EXCLUDED.wip_state, open_action_cnt = EXCLUDED.open_action_cnt,
                 bts_lot = EXCLUDED.bts_lot, updated_at = clock_timestamp()
            """;
    // 원천에서 지워진 앵커의 실적 → 제외하고, 그 블록 롤업을 다시 낸다 (V20 F4). 1분에 한 번
    private static final String EXCLUDE_DELETED = """
            UPDATE hot.p_progress p SET excluded = true, exclude_reason = 'SOURCE_DELETED', judged_at = clock_timestamp()
              FROM hot.l_work_unit u
             WHERE u.unit_id = p.anchor_id AND u.anchor_type = ? AND u.src_deleted_at IS NOT NULL AND NOT p.excluded
            RETURNING p.hull_no || '‖' || p.block_no
            """;
    private static final String ACTION = """
            INSERT INTO hot.p_action (action_type, hull_no, block_no, anchor_id, sub_type, detail, cp_code)
            SELECT 'ALERT', ?, ?, ?, ?, CAST(? AS jsonb), ?
             WHERE NOT EXISTS (SELECT 1 FROM hot.p_action a
                                WHERE a.anchor_id = ? AND a.sub_type = ? AND a.status = 'OPEN')
            """;

    private final String module;
    private final JudgementProperties.Checkpoint props;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;
    private final AtomicLong lastBatch = new AtomicLong();
    private ModuleTrade trade;
    private List<Checkpoint> checkpoints;
    private int windowDays = 30;
    private ScheduledExecutorService scheduler;
    private long lastExcludeMs;

    record Anchor(String unitId, String hullNo, String blockNo, String site, Double planMh, Double targetQty, LocalDate planSd,
                  LocalDate planFd) {}

    public CheckpointWorker(String module, JudgementProperties.Checkpoint props, JdbcTemplate jdbc, TransactionTemplate tx,
                           MeterRegistry meters) {
        this.module = module;
        this.props = props;
        this.jdbc = jdbc;
        this.tx = tx;
        this.meters = meters;
        for (String s : List.of("notDue", "inProgress", "done")) {
            meters.counter("hotdb.judge.checkpoint.judged", "status", s);
        }
        meters.counter("hotdb.judge.checkpoint.reversed");
        meters.counter("hotdb.judge.checkpoint.carry.overdue");
        Gauge.builder("hotdb.judge.checkpoint.last.batch", lastBatch, AtomicLong::get)
                .description("마지막 주기에 다시 판정한 앵커 수").register(meters);
    }

    /** 사전을 읽는다 — 절점 목록 · 산정 규칙. 기동 뒤 첫 주기에서 (DB 가 늦게 떠도 서비스는 뜬다). */
    void loadDictionary() {
        trade = ModuleTrade.load(jdbc, module);
        checkpoints = jdbc.query(CHECKPOINTS, (rs, i) -> {
            JsonNode a = json(rs.getString("attrs"));
            return new Checkpoint(rs.getString("code"), rs.getInt("seq"), text(a, "basis", "DWG"), text(a, "phase", "CUT"),
                    a.path("done").asBoolean(false), "EMERGENCY".equals(text(a, "skip_if", null)), text(a, "record_group", null));
        }, trade.trade());
        if (checkpoints.isEmpty()) {
            throw new IllegalStateException("m_code CHECKPOINT 에 공종 " + trade.trade() + " 의 절점이 없다 (V17 사전)");
        }
        String ruleAttrs = jdbc.query(RULE, rs -> rs.next() ? rs.getString(1) : null, trade.trade());
        windowDays = json(ruleAttrs).path("list_window_days").asInt(30);
        log.info("절점 판별 사전 — 공종 {} · 앵커 {} · 절점 {} · 목록 모수 ±{}일", trade.trade(), trade.anchorType(),
                checkpoints.stream().map(Checkpoint::code).toList(), windowDays);
    }

    /** 한 주기. 테스트와 스케줄러가 부른다. @return 다시 판정한 앵커 수 */
    public int judgeOnce() {
        if (checkpoints == null) {
            loadDictionary();
        }
        excludeDeleted();
        List<Anchor> anchors = jdbc.query(STALE, (rs, i) -> new Anchor(rs.getString("unit_id"), rs.getString("hull_no"),
                        rs.getString("block_no"), rs.getString("site"), doubleOf(rs.getObject("plan_mh")),
                        doubleOf(rs.getObject("target_qty")),
                        rs.getDate("plan_sd") == null ? null : rs.getDate("plan_sd").toLocalDate(),
                        rs.getDate("plan_fd") == null ? null : rs.getDate("plan_fd").toLocalDate()),
                trade.anchorType(), trade.trade(), windowDays, windowDays, props.batchSize());
        lastBatch.set(anchors.size());
        if (anchors.isEmpty()) {
            return 0;
        }
        String[] ids = anchors.stream().map(Anchor::unitId).toArray(String[]::new);
        Map<String, Map<String, Reached>> events = events(ids);
        Map<String, Map<String, PartStat>> parts = parts(ids);
        Map<String, Double> legacy = legacy(ids);
        Map<String, Instant> cary = new HashMap<>();
        jdbc.query(CARY, rs -> {
            Timestamp t = rs.getTimestamp(2);
            if (t != null) {
                cary.put(rs.getString(1), t.toInstant());
            }
        }, (Object) ids);
        Instant now = Instant.now();
        LocalDate today = now.atZone(JudgementWorker.SITE_ZONE).toLocalDate();

        Map<String, Integer> counts = new LinkedHashMap<>();
        Set<String> blocks = new LinkedHashSet<>();
        tx.executeWithoutResult(s -> {
            List<Object[]> rows = new ArrayList<>();
            for (Anchor a : anchors) {
                Result r = CheckpointEvaluator.evaluate(checkpoints, events.getOrDefault(a.unitId(), Map.of()),
                        parts.getOrDefault(a.unitId(), Map.of()), "OUT".equals(a.site()), false);
                Double legacyPct = legacy.get(a.unitId());
                Double gap = legacyPct == null ? null : r.autoPct() - legacyPct;
                boolean done = "done".equals(r.state());
                Map<String, Reached> ev = events.getOrDefault(a.unitId(), Map.of());
                CheckpointEvaluator.Plan plan = CheckpointEvaluator.plan(a.planSd(), a.planFd(), ev, today, JudgementWorker.SITE_ZONE);
                rows.add(new Object[] {a.unitId(), a.hullNo(), a.blockNo(), trade.anchorType(), r.state(), r.autoPct(),
                        legacyPct, gap, a.targetQty(), done ? a.planMh() : 0.0, r.lastCp(), toJson(r.history()),
                        toJson(r.stats()), a.planMh(), a.site(), toJson(plan.plan()), plan.late()});
                if (CheckpointEvaluator.carryOverdue(cary.get(a.unitId()), ev.containsKey("STEEL_ISSUE"), "OUT".equals(a.site()), now)) {
                    meters.counter("hotdb.judge.checkpoint.carry.overdue").increment();
                    jdbc.update(ACTION, a.hullNo(), a.blockNo(), a.unitId(), "CARRY_OVERDUE",
                            toJson(Map.of("cary_due", cary.get(a.unitId()).toString())), "STEEL_ISSUE", a.unitId(), "CARRY_OVERDUE");
                }
                counts.merge(r.state(), 1, Integer::sum);
                if (a.hullNo() != null && a.blockNo() != null) {
                    blocks.add(a.hullNo() + "‖" + a.blockNo());
                }
                if (!r.reversed().isEmpty()) {
                    meters.counter("hotdb.judge.checkpoint.reversed").increment();
                    jdbc.update(ACTION, a.hullNo(), a.blockNo(), a.unitId(), "ORDER_REVERSED", toJson(Map.of("reversed", r.reversed(),
                            "last_cp", r.lastCp() == null ? "" : r.lastCp())), r.reversed().get(0), a.unitId(), "ORDER_REVERSED");
                }
            }
            jdbc.batchUpdate(PROGRESS, rows);
            if (!blocks.isEmpty()) {
                jdbc.update(ROLLUP, trade.trade(), trade.stage(), trade.anchorType(), blocks.toArray(String[]::new));
            }
        });
        counts.forEach((k, v) -> meters.counter("hotdb.judge.checkpoint.judged", "status", k).increment(v));
        log.info("절점 판별 — 앵커 {} ({}) · 블록 롤업 {}", anchors.size(), counts, blocks.size());
        return anchors.size();
    }

    /** 원천에서 지워진 앵커의 실적을 제외하고 그 블록 롤업을 다시 낸다. 1분에 한 번. */
    void excludeDeleted() {
        if (System.currentTimeMillis() - lastExcludeMs < 60_000) {
            return;
        }
        lastExcludeMs = System.currentTimeMillis();
        tx.executeWithoutResult(s -> {
            List<String> blocks = jdbc.queryForList(EXCLUDE_DELETED, String.class, trade.anchorType());
            if (!blocks.isEmpty()) {
                jdbc.update(ROLLUP, trade.trade(), trade.stage(), trade.anchorType(),
                        blocks.stream().filter(java.util.Objects::nonNull).distinct().toArray(String[]::new));
                log.info("원천에서 지워진 앵커 {} 건 — 실적 제외 · 블록 롤업 다시", blocks.size());
            }
        });
    }

    private Map<String, Map<String, Reached>> events(String[] ids) {
        Map<String, Map<String, Reached>> out = new HashMap<>();
        jdbc.query(EVENTS, rs -> {
            Timestamp t = rs.getTimestamp("event_ts");
            out.computeIfAbsent(rs.getString("unit_id"), k -> new HashMap<>())
                    .put(rs.getString("event_code"), new Reached(t == null ? null : t.toInstant(), rs.getString("source"),
                            rs.getString("conf")));
        }, (Object) ids);
        return out;
    }

    private Map<String, Map<String, PartStat>> parts(String[] ids) {
        Map<String, Map<String, PartStat>> out = new HashMap<>();
        jdbc.query(PARTS, rs -> {
            Map<String, PartStat> m = new HashMap<>();
            m.put("BEND_DONE", new PartStat(rs.getInt("bend_done"), rs.getInt("bend_target")));
            m.put("GROUP", new PartStat(rs.getInt("group_done"), rs.getInt("part_cnt")));
            m.put("PART_ISSUE", new PartStat(rs.getInt("issue_done"), rs.getInt("part_cnt")));
            m.put("GRIND", new PartStat(rs.getInt("grind_done"), rs.getInt("grind_target")));
            out.put(rs.getString("dwg_no"), m);
        }, (Object) ids);
        return out;
    }

    private Map<String, Double> legacy(String[] ids) {
        Map<String, Double> out = new HashMap<>();
        jdbc.query(LEGACY, rs -> {
            out.put(rs.getString(1), doubleOf(rs.getObject(2)));
        }, (Object) ids);
        return out;
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

    private static String text(JsonNode n, String field, String dflt) {
        return n.hasNonNull(field) ? n.get(field).asText() : dflt;
    }

    private static Double doubleOf(Object v) {
        return v == null ? null : ((Number) v).doubleValue();
    }

    @Override
    public void start() {
        log.info("절점 판별 단계 시작 — 모듈 {} · {}ms 마다 앵커 최대 {}", module, props.intervalMs(), props.batchSize());
        scheduler = Executors.newSingleThreadScheduledExecutor(WorkThreads.factory("judge", "checkpoint-" + module, meters));
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                judgeOnce();
            } catch (RuntimeException e) {
                log.warn("절점 판별 주기 실패: {}", e.toString());
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

    /** 테스트용 — 사전을 밖에서 넣는다. */
    void dictionary(ModuleTrade t, List<Checkpoint> cps, int days) {
        this.trade = t;
        this.checkpoints = cps;
        this.windowDays = days;
    }
}
