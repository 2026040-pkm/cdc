package dev.hotdb.zone.judge;

import dev.hotdb.cdc.resource.WorkThreads;
import dev.hotdb.zone.judge.JudgementRule.Verdict;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 센서 판별 단계 — judged = false 인 인식 객체(hot.s_scan_detection)를 집어 규칙으로 판정하고 실적 표에 쓴다.
 *
 * <pre>
 *   인식 객체(블록) ──규칙──▶ DONE · IN_PROGRESS · LOW_CONFIDENCE
 *     ├ 앵커 매칭: l_work_unit 에서 같은 호선 · 블록 · 이 공종의 앵커 유형(제외 아닌 것)을 찾는다
 *     │    1개   → p_progress (앵커 × 오늘) + p_progress_block (블록 × 공종 × 단계 × 오늘), match_state MATCHED
 *     │    여럿  → p_progress_block 만 (인식이 블록 단위까지라 송선 · GDO 를 못 고른다) — judge_note BLOCK_ONLY
 *     │    0개   → p_progress_block + 앵커 기다림 WAIT_ANCHOR (V20 F1). 앵커가 적재되면 judged = false 로 되돌려 다시 판정,
 *     │            기다림(TRADE_RULE anchor_wait_days)이 끝나면 UNMATCHED 확정 + 알림 p_action UNMATCHED
 *   실적 기준일(as_of)은 판정한 날이 아니라 인식 시각(Asia/Seoul)의 날짜 (V20 F2)
 *   원천에서 지워진 앵커 · W/O(src_deleted_at)는 찾지 않는다 (V20 F4)
 *     ├ DONE 인데 앵커의 레거시 W/O 가 착수 전이면 state = held (판별 O / 레거시 X → 완료 처리 금지, ASM-F10)
 *     └ LOW_CONFIDENCE → 실적 안 쓰고 알림 p_action LOW_CONFIDENCE
 *   조립(S) 은 앵커 하나를 고른 뒤 하위 W/O 와 맞춘다 (ASM-F09) — 인식일이 계획 기간에 드는 W/O(하루치, 취부 F 우선)
 *     → 앵커 계획이 인식일 ±match_week_days 안(4주 풀) → 불일치(match_state MISMATCH + 알림 WO_MISMATCH).
 *     match_pool · match_wo_no · match_lead(EARLY · ON_TIME · LATE) 를 p_progress 에 남긴다
 *   location_id 는 앵커 작업장이 m_location 에 있을 때만 적는다 (구역 등록 전에는 비운다)
 *   블록 행에는 열린 알림 수 · BTS 지번(m_block) 을 같이 적는다
 *   끝나면 s_scan_detection 에 anchor_id · judged · judged_ts · judge_note 를 적는다 (라우트가 keep 이라 재전송이 되돌리지 않는다)
 * </pre>
 *
 * <p>CDC 반영(엔진 스레드)과 같은 프로세스의 다른 스레드다. 반영은 judged = false 로 넣기만 하고 여기서 판정하므로,
 * 판정이 느리거나 규칙이 터져도 슬롯은 밀리지 않는다. 여러 인스턴스가 같은 모듈을 돌려도 FOR UPDATE SKIP LOCKED 라
 * 한 행을 두 번 판정하지 않는다.
 */
public class JudgementWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JudgementWorker.class);
    /** 규칙이 터진 행을 이만큼 넘게 쌓으면 비우고 다시 시도한다 — 기억이 끝없이 크지 않게 */
    private static final int MAX_FAILED_KEYS = 10_000;
    /** 인식일을 셀 때 쓰는 현장 시간대 */
    static final ZoneId SITE_ZONE = ZoneId.of("Asia/Seoul");
    private static final Pattern WEEK_DAYS = Pattern.compile("\"match_week_days\"\\s*:\\s*(\\d+)");
    private static final Pattern WAIT_DAYS = Pattern.compile("\"anchor_wait_days\"\\s*:\\s*(\\d+)");

    private static final String CLAIM = """
            SELECT d.scan_id, d.object_key, d.hull_no, d.block_no, d.object_type, d.progress_est, d.match_conf,
                   d.received_ts, d.event_type
              FROM hot.s_scan_detection d
              JOIN hot.s_scan_session s ON s.scan_id = d.scan_id
             WHERE NOT d.judged AND s.zone = ?
             ORDER BY d.received_ts
             LIMIT ?
               FOR UPDATE OF d SKIP LOCKED
            """;
    private static final String ANCHORS = """
            SELECT unit_id, plan_mh, target_qty, site, acc_rate, actl_sd, actl_fd, location_id, plan_sd, plan_fd
              FROM hot.l_work_unit
             WHERE anchor_type = ? AND hull_no = ? AND block_no = ? AND NOT excluded AND src_deleted_at IS NULL
             ORDER BY unit_id
            """;
    private static final String CHILD_WO = """
            SELECT unit_id, plan_sd, plan_fd FROM hot.l_work_unit WHERE level = 'WO' AND parent_id = ? AND NOT excluded
               AND src_deleted_at IS NULL
            """;
    private static final String RULE = "SELECT attrs FROM hot.m_code WHERE code_group = 'TRADE_RULE' AND code = ?";
    private static final String MARK = """
            UPDATE hot.s_scan_detection
               SET judged = true, judged_ts = clock_timestamp(), anchor_id = ?, anchor_type = ?, judge_note = ?,
                   match_attempts = match_attempts + 1
             WHERE scan_id = ? AND object_key = ? AND NOT judged
            """;
    // 앵커 기다림 — 끝 시각은 처음 기다릴 때 정하고(인식 + N일) 다시 기다려도 늘리지 않는다
    private static final String WAIT = """
            UPDATE hot.s_scan_detection
               SET judged = true, judged_ts = clock_timestamp(), anchor_id = NULL, anchor_type = NULL, judge_note = 'WAIT_ANCHOR',
                   match_attempts = match_attempts + 1,
                   match_wait_until = coalesce(match_wait_until, received_ts + make_interval(days => ?))
             WHERE scan_id = ? AND object_key = ? AND NOT judged
            RETURNING match_wait_until < clock_timestamp()
            """;
    // 앵커가 생긴 블록의 기다림을 다시 판정 대상으로
    private static final String REQUEUE = """
            UPDATE hot.s_scan_detection d SET judged = false
              FROM hot.s_scan_session s
             WHERE s.scan_id = d.scan_id AND s.zone = ? AND d.judged AND d.judge_note = 'WAIT_ANCHOR'
               AND EXISTS (SELECT 1 FROM hot.l_work_unit u
                            WHERE u.anchor_type = ? AND u.hull_no = d.hull_no AND u.block_no = d.block_no
                              AND NOT u.excluded AND u.src_deleted_at IS NULL)
            """;
    // 기다림이 끝난 것 → UNMATCHED 확정
    private static final String EXPIRE = """
            UPDATE hot.s_scan_detection d SET judge_note = 'UNMATCHED', judged_ts = clock_timestamp()
              FROM hot.s_scan_session s
             WHERE s.scan_id = d.scan_id AND s.zone = ? AND d.judged AND d.judge_note = 'WAIT_ANCHOR'
               AND d.match_wait_until < clock_timestamp()
            RETURNING d.scan_id, d.object_key, d.hull_no, d.block_no, d.progress_est
            """;
    // 원천에서 지워진 앵커의 실적 → 제외 (지우지 않는다)
    private static final String EXCLUDE_DELETED = """
            UPDATE hot.p_progress p SET excluded = true, exclude_reason = 'SOURCE_DELETED', judged_at = clock_timestamp()
              FROM hot.l_work_unit u
             WHERE u.unit_id = p.anchor_id AND u.anchor_type = ? AND u.src_deleted_at IS NOT NULL AND NOT p.excluded
            """;
    private static final String PROGRESS = """
            INSERT INTO hot.p_progress (anchor_id, as_of, hull_no, block_no, anchor_type, state, auto_pct, legacy_pct, gap_pct,
                                        planned_qty, evidence, first_detect_ts, last_detect_ts, weight_mh, site, excluded,
                                        match_state, match_pool, match_wo_no, match_lead, location_id, judged_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?, ?, ?, ?, false, ?, ?, ?, ?,
                    (SELECT l.location_id FROM hot.m_location l WHERE l.location_id = ?), clock_timestamp())
            ON CONFLICT (anchor_id, as_of) DO UPDATE SET
                 state = CASE WHEN hot.p_progress.state = 'confirmed' THEN hot.p_progress.state ELSE EXCLUDED.state END,
                 auto_pct = GREATEST(coalesce(hot.p_progress.auto_pct, 0), EXCLUDED.auto_pct),
                 legacy_pct = EXCLUDED.legacy_pct, gap_pct = EXCLUDED.gap_pct, planned_qty = EXCLUDED.planned_qty,
                 evidence = coalesce(hot.p_progress.evidence, '[]'::jsonb) || EXCLUDED.evidence,
                 first_detect_ts = LEAST(hot.p_progress.first_detect_ts, EXCLUDED.first_detect_ts),
                 last_detect_ts = GREATEST(hot.p_progress.last_detect_ts, EXCLUDED.last_detect_ts),
                 weight_mh = EXCLUDED.weight_mh, site = EXCLUDED.site, match_state = EXCLUDED.match_state,
                 match_pool = EXCLUDED.match_pool, match_wo_no = EXCLUDED.match_wo_no, match_lead = EXCLUDED.match_lead,
                 location_id = coalesce(EXCLUDED.location_id, hot.p_progress.location_id),
                 excluded = CASE WHEN hot.p_progress.exclude_reason = 'SOURCE_DELETED' THEN false ELSE hot.p_progress.excluded END,
                 exclude_reason = NULLIF(hot.p_progress.exclude_reason, 'SOURCE_DELETED'),
                 judged_at = clock_timestamp()
            """;
    private static final String BLOCK = """
            INSERT INTO hot.p_progress_block (hull_no, block_no, trade, stage, as_of, auto_pct, weight_basis, anchor_cnt,
                                              wip_state, first_detect_ts, last_detect_ts, open_action_cnt, bts_lot, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'COUNT', ?, ?, ?, ?,
                    (SELECT count(*) FROM hot.p_action x WHERE x.status = 'OPEN' AND x.hull_no = ? AND x.block_no = ?),
                    (SELECT m.bts_lot FROM hot.m_block m WHERE m.hull_no = ? AND m.block_no = ?), clock_timestamp())
            ON CONFLICT (hull_no, block_no, trade, stage, as_of) DO UPDATE SET
                 auto_pct = GREATEST(coalesce(hot.p_progress_block.auto_pct, 0), EXCLUDED.auto_pct),
                 anchor_cnt = EXCLUDED.anchor_cnt,
                 wip_state = CASE WHEN hot.p_progress_block.wip_state = 'DONE' THEN 'DONE' ELSE EXCLUDED.wip_state END,
                 first_detect_ts = LEAST(hot.p_progress_block.first_detect_ts, EXCLUDED.first_detect_ts),
                 last_detect_ts = GREATEST(hot.p_progress_block.last_detect_ts, EXCLUDED.last_detect_ts),
                 open_action_cnt = EXCLUDED.open_action_cnt, bts_lot = EXCLUDED.bts_lot,
                 updated_at = clock_timestamp()
            """;
    private static final String ACTION = """
            INSERT INTO hot.p_action (action_type, hull_no, block_no, anchor_id, sub_type, detail, value_pct)
            SELECT 'ALERT', ?, ?, ?, ?, CAST(? AS jsonb), ?
             WHERE NOT EXISTS (SELECT 1 FROM hot.p_action a
                                WHERE a.action_type = 'ALERT' AND a.sub_type = ? AND a.status = 'OPEN'
                                  AND a.hull_no IS NOT DISTINCT FROM ? AND a.block_no IS NOT DISTINCT FROM ?)
            """;
    private static final String BACKLOG = """
            SELECT count(*), coalesce(extract(epoch FROM clock_timestamp() - min(d.received_ts)), 0)
              FROM hot.s_scan_detection d JOIN hot.s_scan_session s ON s.scan_id = d.scan_id
             WHERE NOT d.judged AND s.zone = ?
            """;

    private final String module;
    private final JudgementRule rule;
    private final JudgementProperties props;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;
    private final Set<Key> failed = new HashSet<>();
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong oldestPendingSeconds = new AtomicLong();
    private ModuleTrade trade;
    private String zoneCode;
    private int matchWeekDays = 28;
    private int anchorWaitDays = 7;
    private long lastExcludeMs;
    private ScheduledExecutorService scheduler;

    record Key(String scanId, String objectKey) {}

    /** 앵커 후보 한 행 (l_work_unit). */
    record Anchor(String unitId, Double planMh, Double targetQty, String site, Double accRate, boolean legacyStarted,
                  String locationId, LocalDate planSd, LocalDate planFd) {}

    public JudgementWorker(String module, JudgementRule rule, JudgementProperties props, JdbcTemplate jdbc,
                           TransactionTemplate tx, MeterRegistry meters) {
        this.module = module;
        this.rule = rule;
        this.props = props;
        this.jdbc = jdbc;
        this.tx = tx;
        this.meters = meters;
        // 평소 0 인 지표도 시계열이 있어야 대시보드가 "없음"과 "0"을 구분한다
        for (String status : List.of("done", "inProgress", "held", "low_confidence", "unmatched", "block_only", "wo_mismatch",
                "wait_anchor")) {
            meters.counter("hotdb.judge.judged", "status", status);
        }
        meters.counter("hotdb.judge.failed");
        Gauge.builder("hotdb.judge.pending", pending, AtomicLong::get)
                .description("판정을 기다리는 인식 객체 수").register(meters);
        Gauge.builder("hotdb.judge.pending.oldest.seconds", oldestPendingSeconds, AtomicLong::get)
                .description("가장 오래 기다린 인식 객체의 대기(수신 → 지금, 초)").register(meters);
    }

    /** 한 번 훑는다. 테스트와 스케줄러가 부른다. @return 판정한 건수 */
    public int judgeOnce() {
        if (trade == null) {
            trade = ModuleTrade.load(jdbc, module);
            zoneCode = jdbc.queryForObject("SELECT zone_code FROM ops.module WHERE module = ?", String.class, module);
            String attrs = jdbc.query(RULE, rs -> rs.next() ? rs.getString(1) : null, trade.trade());
            Matcher m = attrs == null ? null : WEEK_DAYS.matcher(attrs);
            if (m != null && m.find()) {
                matchWeekDays = Integer.parseInt(m.group(1));
            }
            Matcher w = attrs == null ? null : WAIT_DAYS.matcher(attrs);
            if (w != null && w.find()) {
                anchorWaitDays = Integer.parseInt(w.group(1));
            }
            meters.counter("hotdb.judge.anchor.requeued");
        }
        housekeeping();
        Map<String, Integer> byStatus = tx.execute(s -> {
            List<Detection> rows = jdbc.query(CLAIM, JudgementWorker::map, zoneCode, props.batchSize() + failed.size());
            Map<String, Integer> counts = new LinkedHashMap<>();
            int done = 0;
            for (Detection d : rows) {
                if (done >= props.batchSize()) {
                    break;
                }
                Key key = new Key(d.scanId(), d.objectKey());
                if (failed.contains(key)) {
                    continue;
                }
                try {
                    String status = judgeOne(d);
                    counts.merge(status, 1, Integer::sum);
                    done++;
                } catch (RuntimeException e) {
                    remember(key);
                    meters.counter("hotdb.judge.failed").increment();
                    log.warn("판정 실패 {}/{} scan={} 규칙={} — judged=false 로 둔다: {}", d.hullNo(), d.blockNo(),
                            d.scanId(), rule.name(), e.toString());
                }
            }
            return counts;
        });
        int total = 0;
        for (Map.Entry<String, Integer> e : byStatus.entrySet()) {
            meters.counter("hotdb.judge.judged", "status", e.getKey()).increment(e.getValue());
            total += e.getValue();
        }
        if (total > 0) {
            log.debug("판정 {} — {}", total, byStatus);
        }
        refreshBacklog();
        return total;
    }

    /**
     * 주기마다 앞에서 — ① 앵커가 생긴 블록의 기다림을 다시 판정 대상으로 ② 기다림이 끝난 것은 UNMATCHED 확정 + 알림
     * ③ 원천에서 지워진 앵커의 실적은 제외.
     */
    void housekeeping() {
        tx.executeWithoutResult(s -> {
            int requeued = jdbc.update(REQUEUE, zoneCode, trade.anchorType());
            List<Detection> expired = jdbc.query(EXPIRE, (rs, i) -> new Detection(rs.getString(1), rs.getString(2),
                    rs.getString(3), rs.getString(4), null, floatOf(rs.getObject(5)), null, null, "COMPLETE"), zoneCode);
            for (Detection d : expired) {
                alert(d.hullNo(), d.blockNo(), null, "UNMATCHED", d, d.progressEst());
            }
            int excluded = 0;
            if (System.currentTimeMillis() - lastExcludeMs > 60_000) {   // 원천 삭제는 폴링 주기(분)로만 바뀐다
                excluded = jdbc.update(EXCLUDE_DELETED, trade.anchorType());
                lastExcludeMs = System.currentTimeMillis();
            }
            if (requeued + expired.size() + excluded > 0) {
                meters.counter("hotdb.judge.anchor.requeued").increment(requeued);
                meters.counter("hotdb.judge.judged", "status", "unmatched").increment(expired.size());
                log.info("앵커 기다림 — 다시 판정 {} · UNMATCHED 확정 {} · 원천 삭제로 실적 제외 {}", requeued, expired.size(), excluded);
            }
        });
    }

    /** 인식 객체 하나를 판정해 실적 표에 쓰고 judged 를 찍는다. @return 지표 · 로그용 상태 */
    private String judgeOne(Detection d) {
        Verdict v = rule.judge(d);
        if (v == null) {
            throw new IllegalStateException("판정이 비었다");
        }
        if (v == Verdict.LOW_CONFIDENCE) {
            alert(d.hullNo(), d.blockNo(), null, "LOW_CONFIDENCE", d, d.matchConf());
            mark(d, null, "LOW_CONFIDENCE");
            return "low_confidence";
        }
        List<Anchor> anchors = d.hullNo() == null || d.blockNo() == null ? List.of()
                : jdbc.query(ANCHORS, JudgementWorker::anchor, trade.anchorType(), d.hullNo(), d.blockNo());
        double pct = d.progressEst();
        String blockWip = v == Verdict.DONE ? "DONE" : "WORKING";
        java.sql.Date asOf = java.sql.Date.valueOf(detectDay(d));
        jdbc.update(BLOCK, d.hullNo(), d.blockNo(), trade.trade(), trade.stage(), asOf, pct, anchors.size(), blockWip,
                ts(d.receivedTs()), ts(d.receivedTs()), d.hullNo(), d.blockNo(), d.hullNo(), d.blockNo());

        if (anchors.isEmpty()) {
            if (d.hullNo() == null || d.blockNo() == null) {
                // 호선 · 블록이 없는 실적은 기다려도 앵커가 생길 수 없다
                alert(d.hullNo(), d.blockNo(), null, "UNMATCHED", d, pct);
                mark(d, null, "UNMATCHED");
                return "unmatched";
            }
            Boolean expired = jdbc.query(WAIT, rs -> rs.next() ? rs.getBoolean(1) : null, anchorWaitDays, d.scanId(), d.objectKey());
            if (Boolean.TRUE.equals(expired)) {
                // 이미 기다림이 끝난 오래된 실적 (재기동 · 적체) — 바로 확정
                jdbc.update("UPDATE hot.s_scan_detection SET judge_note = 'UNMATCHED' WHERE scan_id = ? AND object_key = ?",
                        d.scanId(), d.objectKey());
                alert(d.hullNo(), d.blockNo(), null, "UNMATCHED", d, pct);
                return "unmatched";
            }
            return "wait_anchor";
        }
        if (anchors.size() > 1) {
            mark(d, null, "BLOCK_ONLY(" + anchors.size() + " " + trade.anchorType() + ")");
            return "block_only";
        }
        Anchor a = anchors.get(0);
        // 판별 O / 레거시 W/O X → 완료 처리 금지 (held)
        String state = v == Verdict.DONE ? (a.legacyStarted() ? "done" : "held") : "inProgress";
        Double gap = a.accRate() == null ? null : pct - a.accRate();
        String evidence = "[{\"scan_id\":\"" + d.scanId() + "\",\"object_key\":\"" + d.objectKey().replace("\"", "") + "\"}]";
        LegacyRules.Match match = null;
        if ("S".equals(trade.trade())) {
            List<LegacyRules.Wo> wos = jdbc.query(CHILD_WO, (rs, i) -> new LegacyRules.Wo(rs.getString(1), date(rs, "plan_sd"),
                    date(rs, "plan_fd")), a.unitId());
            match = LegacyRules.matchWo(detectDay(d), a.planSd(), a.planFd(), wos, matchWeekDays);
            if ("MISMATCH".equals(match.state())) {
                alert(d.hullNo(), d.blockNo(), a.unitId(), "WO_MISMATCH", d, pct);
            }
        }
        jdbc.update(PROGRESS, a.unitId(), asOf, d.hullNo(), d.blockNo(), trade.anchorType(), state, pct, a.accRate(), gap,
                a.targetQty(), evidence, ts(d.receivedTs()), ts(d.receivedTs()), a.planMh(), a.site(),
                match == null ? "MATCHED" : match.state(), match == null ? null : match.pool(),
                match == null ? null : match.woNo(), match == null ? null : match.lead(), a.locationId());
        boolean mismatch = match != null && "MISMATCH".equals(match.state());
        mark(d, a.unitId(), mismatch ? state + "/WO_MISMATCH" : state);
        if (mismatch) {
            return "wo_mismatch";
        }
        return state;
    }

    /** 실적 기준일 = 인식 시각(현장 시간대)의 날짜. 시각이 없으면 오늘. */
    static LocalDate detectDay(Detection d) {
        return (d.receivedTs() == null ? Instant.now() : d.receivedTs()).atZone(SITE_ZONE).toLocalDate();
    }

    private void mark(Detection d, String anchorId, String note) {
        jdbc.update(MARK, anchorId, anchorId == null ? null : trade.anchorType(), note, d.scanId(), d.objectKey());
    }

    private void alert(String hull, String block, String anchorId, String subType, Detection d, Number value) {
        String detail = "{\"scan_id\":\"" + d.scanId() + "\",\"object_key\":\"" + d.objectKey().replace("\"", "")
                + "\",\"module\":\"" + module + "\"}";
        jdbc.update(ACTION, hull, block, anchorId, subType, detail, value, subType, hull, block);
    }

    private void refreshBacklog() {
        jdbc.query(BACKLOG, rs -> {
            pending.set(rs.getLong(1));
            oldestPendingSeconds.set(Math.round(rs.getDouble(2)));
        }, zoneCode);
    }

    private void remember(Key key) {
        if (failed.size() >= MAX_FAILED_KEYS) {
            log.warn("판정 실패 행이 {} 건을 넘어 기억을 비운다 — 다음 주기부터 다시 시도한다", MAX_FAILED_KEYS);
            failed.clear();
        }
        failed.add(key);
    }

    private static Detection map(ResultSet rs, int i) throws SQLException {
        return new Detection(rs.getString("scan_id"), rs.getString("object_key"), rs.getString("hull_no"),
                rs.getString("block_no"), rs.getString("object_type"), floatOf(rs.getObject("progress_est")),
                floatOf(rs.getObject("match_conf")), instant(rs, "received_ts"), rs.getString("event_type"));
    }

    private static Anchor anchor(ResultSet rs, int i) throws SQLException {
        return new Anchor(rs.getString("unit_id"), doubleOf(rs.getObject("plan_mh")), doubleOf(rs.getObject("target_qty")),
                rs.getString("site"), doubleOf(rs.getObject("acc_rate")), rs.getDate("actl_sd") != null,
                rs.getString("location_id"), date(rs, "plan_sd"), date(rs, "plan_fd"));
    }

    private static LocalDate date(ResultSet rs, String col) throws SQLException {
        java.sql.Date v = rs.getDate(col);
        return v == null ? null : v.toLocalDate();
    }

    private static Float floatOf(Object v) {
        return v == null ? null : ((Number) v).floatValue();
    }

    private static Double doubleOf(Object v) {
        return v == null ? null : ((Number) v).doubleValue();
    }

    private static Instant instant(ResultSet rs, String col) throws SQLException {
        Timestamp t = rs.getTimestamp(col);
        return t == null ? null : t.toInstant();
    }

    private static Timestamp ts(Instant i) {
        return i == null ? null : Timestamp.from(i);
    }

    @Override
    public void start() {
        log.info("센서 판별 단계 시작 — 모듈 {} · 규칙 {} · {}ms 마다 최대 {} 건", module, rule.name(), props.intervalMs(),
                props.batchSize());
        // part=judge 로 따로 센다 — 같은 프로세스의 CDC 엔진과 CPU · 할당을 나란히 본다 (hotdb_work_*)
        scheduler = Executors.newSingleThreadScheduledExecutor(WorkThreads.factory("judge", "judge-" + module, meters));
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                judgeOnce();
            } catch (RuntimeException e) {
                // DB 가 잠깐 안 될 때 — 다음 주기에 다시 본다
                log.warn("판별 주기 실패: {}", e.toString());
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
