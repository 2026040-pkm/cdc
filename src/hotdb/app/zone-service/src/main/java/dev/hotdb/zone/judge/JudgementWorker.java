package dev.hotdb.zone.judge;

import dev.hotdb.cdc.resource.WorkThreads;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
 * 판별 단계 — PENDING 실적을 집어 규칙으로 판정하고 judged_status · judged_at 을 쓴다.
 *
 * <p>CDC 반영(엔진 스레드)과 같은 프로세스의 다른 스레드다. 반영은 PENDING 으로 넣기만 하고 여기서 판정하므로,
 * 판정이 느리거나 규칙이 터져도 슬롯은 밀리지 않는다. 라우트가 judged_status 를 keep 으로 쓰기 때문에
 * 같은 실적이 다시 반영돼도 판정은 되돌아가지 않는다.
 *
 * <p>여러 인스턴스가 같은 모듈을 돌려도 FOR UPDATE SKIP LOCKED 라 한 행을 두 번 판정하지 않는다.
 */
public class JudgementWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JudgementWorker.class);
    /** 규칙이 터진 행을 이만큼 넘게 쌓으면 비우고 다시 시도한다 — 기억이 끝없이 크지 않게 */
    private static final int MAX_FAILED_KEYS = 10_000;

    private static final String CLAIM = """
            SELECT module, hull_no, block_id, scan_id, device_id, completed_at, block_progress_rate,
                   match_confidence, reference_cad_id, model_version, applied_at
              FROM svc.actual_result
             WHERE module = ? AND judged_status = 'PENDING'
             ORDER BY completed_at
             LIMIT ?
               FOR UPDATE SKIP LOCKED
            """;
    private static final String JUDGE = """
            UPDATE svc.actual_result SET judged_status = ?, judged_at = clock_timestamp()
             WHERE module = ? AND hull_no = ? AND block_id = ? AND scan_id = ? AND judged_status = 'PENDING'
            """;
    private static final String BACKLOG = """
            SELECT count(*), coalesce(extract(epoch FROM clock_timestamp() - min(applied_at)), 0)
              FROM svc.actual_result WHERE module = ? AND judged_status = 'PENDING'
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
    private ScheduledExecutorService scheduler;

    record Key(String hullNo, String blockId, UUID scanId) {}

    public JudgementWorker(String module, JudgementRule rule, JudgementProperties props, JdbcTemplate jdbc,
                           TransactionTemplate tx, MeterRegistry meters) {
        this.module = module;
        this.rule = rule;
        this.props = props;
        this.jdbc = jdbc;
        this.tx = tx;
        this.meters = meters;
        // 평소 0 인 지표도 시계열이 있어야 대시보드가 "없음"과 "0"을 구분한다
        for (String status : List.of(JudgementRule.CONFIRMED, JudgementRule.REJECTED)) {
            meters.counter("hotdb.judge.judged", "status", status);
        }
        meters.counter("hotdb.judge.failed");
        Gauge.builder("hotdb.judge.pending", pending, AtomicLong::get)
                .description("판정을 기다리는 실적 수").register(meters);
        Gauge.builder("hotdb.judge.pending.oldest.seconds", oldestPendingSeconds, AtomicLong::get)
                .description("가장 오래 기다린 실적의 대기(반영 → 지금, 초)").register(meters);
    }

    /** 한 번 훑는다. 테스트와 스케줄러가 부른다. @return 판정한 건수 */
    public int judgeOnce() {
        Map<String, Integer> byStatus = tx.execute(s -> {
            List<ActualResult> rows = jdbc.query(CLAIM, JudgementWorker::map, module, props.batchSize() + failed.size());
            List<Object[]> updates = new ArrayList<>();
            Map<String, Integer> counts = new LinkedHashMap<>();
            for (ActualResult r : rows) {
                if (updates.size() >= props.batchSize()) {
                    break;
                }
                Key key = new Key(r.hullNo(), r.blockId(), r.scanId());
                if (failed.contains(key)) {
                    continue;
                }
                String status;
                try {
                    status = rule.judge(r);
                    if (status == null || status.isBlank() || JudgementRule.PENDING.equals(status)) {
                        throw new IllegalStateException("판정 상태가 비었거나 PENDING: " + status);
                    }
                } catch (RuntimeException e) {
                    remember(key);
                    meters.counter("hotdb.judge.failed").increment();
                    log.warn("판정 실패 {}/{} scan={} 규칙={} — PENDING 으로 둔다: {}", r.hullNo(), r.blockId(),
                            r.scanId(), rule.name(), e.toString());
                    continue;
                }
                updates.add(new Object[] {status, module, r.hullNo(), r.blockId(), r.scanId()});
                counts.merge(status, 1, Integer::sum);
            }
            if (!updates.isEmpty()) {
                jdbc.batchUpdate(JUDGE, updates);
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

    private void refreshBacklog() {
        jdbc.query(BACKLOG, rs -> {
            pending.set(rs.getLong(1));
            oldestPendingSeconds.set(Math.round(rs.getDouble(2)));
        }, module);
    }

    private void remember(Key key) {
        if (failed.size() >= MAX_FAILED_KEYS) {
            log.warn("판정 실패 행이 {} 건을 넘어 기억을 비운다 — 다음 주기부터 다시 시도한다", MAX_FAILED_KEYS);
            failed.clear();
        }
        failed.add(key);
    }

    private static ActualResult map(ResultSet rs, int i) throws SQLException {
        return new ActualResult(rs.getString("module"), rs.getString("hull_no"), rs.getString("block_id"),
                rs.getObject("scan_id", UUID.class), rs.getString("device_id"), instant(rs, "completed_at"),
                (Float) rs.getObject("block_progress_rate"), (Float) rs.getObject("match_confidence"),
                rs.getString("reference_cad_id"), rs.getString("model_version"), instant(rs, "applied_at"));
    }

    private static Instant instant(ResultSet rs, String col) throws SQLException {
        Timestamp t = rs.getTimestamp(col);
        return t == null ? null : t.toInstant();
    }

    @Override
    public void start() {
        log.info("판별 단계 시작 — 모듈 {} · 규칙 {} · {}ms 마다 최대 {} 건", module, rule.name(), props.intervalMs(),
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
