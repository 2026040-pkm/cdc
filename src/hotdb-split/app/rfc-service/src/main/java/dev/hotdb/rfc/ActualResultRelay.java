package dev.hotdb.rfc;

import dev.hotdb.cdc.port.CdcSink;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * svc.actual_result 변경 → SAP. 실적 · 판정 상태 하나당 한 번만 보낸다.
 *
 * <p>한 건마다 한 트랜잭션: {@code ops.rfc_sent} 에 키를 넣고(이미 있으면 중복이라 건너뜀) → SAP 송신 → 커밋.
 * 송신이 실패하면 기록도 되돌려져 다음 시도에서 다시 보낸다. 송신은 됐는데 커밋 직전에 죽으면 한 번 더 간다 —
 * 그 한 건은 SAP 쪽 멱등 키(실적 키 + 판정 상태)로 막아야 한다.
 *
 * <p>실패 처리는 원인에 따라 다르다.
 * <ul>
 *   <li>내용 문제(SAP 거절 · 값이 표에 안 맞음) — 다시 해도 같으므로 바로 dead letter, 다음 건으로 간다
 *   <li>일시 장애(SAP · DB 접속) — 재시도, 소진하면 예외를 올린다. 엔진이 멈추고 컨테이너가 재시작되며,
 *       그동안 슬롯이 WAL 을 붙잡아 빠지는 실적이 없다. SAP 장애를 dead letter 로 흘려보내지 않으려는 것이다
 * </ul>
 */
public class ActualResultRelay implements CdcSink {

    private static final Logger log = LoggerFactory.getLogger(ActualResultRelay.class);
    private static final String SCHEMA = "svc";
    private static final String TABLE = "actual_result";
    private static final String ROUTE = "rfc-actual";
    private static final String LEDGER = """
            INSERT INTO ops.rfc_sent (zone, hull_no, block_id, scan_id, judged_status,
                                      src_received_at, src_applied_at, src_lsn)
            VALUES (?, ?, ?, CAST(? AS uuid), ?, CAST(? AS timestamptz), CAST(? AS timestamptz), ?)
            ON CONFLICT DO NOTHING
            """;

    private final String pipeline;
    private final RfcProperties policy;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final RfcSender sender;
    private final MeterRegistry meters;
    private final Map<String, Timer> lagTimers = new ConcurrentHashMap<>();

    public ActualResultRelay(String pipeline, RfcProperties policy, JdbcTemplate jdbc, TransactionTemplate tx,
                             RfcSender sender, MeterRegistry meters) {
        this.pipeline = pipeline;
        this.policy = policy;
        this.jdbc = jdbc;
        this.tx = tx;
        this.sender = sender;
        this.meters = meters;
        // 평소 0 인 지표도 시계열이 있어야 대시보드가 "없음"과 "0"을 구분한다
        meters.counter("hotdb.cdc.apply.retry");
        meters.counter("hotdb.cdc.dead.letter", "route", ROUTE);
    }

    @Override
    public boolean interestedIn(TableId table) {
        return TABLE.equals(table.table()) && SCHEMA.equals(table.schema());
    }

    @Override
    public void apply(List<CdcEvent> events) {
        for (CdcEvent e : events) {
            if (e.op() == Op.DELETE || e.op() == Op.TRUNCATE) {
                // 실적 삭제는 SAP 로 되돌리지 않는다 (취소 전표는 판정 상태로 표현한다)
                meters.counter("hotdb.rfc.skipped", "reason", "delete").increment();
                continue;
            }
            relay(e);
        }
    }

    private void relay(CdcEvent e) {
        Map<String, Object> row = e.after();
        String zone = String.valueOf(row.get("module"));   // 판별 모듈 (V9 — 스키마 하나 · module 컬럼)
        long backoff = policy.backoffMs();
        for (int attempt = 0; ; attempt++) {
            try {
                boolean sent = Boolean.TRUE.equals(tx.execute(s -> sendOnce(zone, row, e.lsn())));
                if (sent) {
                    meters.counter("hotdb.rfc.sent", "zone", zone).increment();
                    recordLag(zone, row.get("applied_at"));
                } else {
                    meters.counter("hotdb.rfc.duplicate", "zone", zone).increment();
                }
                return;
            } catch (RuntimeException ex) {
                if (isPoison(ex)) {
                    deadLetter(e, ex);
                    return;
                }
                if (attempt >= policy.maxRetries()) {
                    throw new IllegalStateException("SAP 송신 재시도 소진 — 재시작 후 같은 자리부터 다시 보낸다", ex);
                }
                meters.counter("hotdb.cdc.apply.retry").increment();
                log.warn("송신 실패 ({}/{}): {}", attempt + 1, policy.maxRetries() + 1, rootMessage(ex));
                sleep(backoff);
                backoff *= 2;
            }
        }
    }

    /** @return 이번에 보냈으면 true, 이미 보낸 키면 false */
    private boolean sendOnce(String zone, Map<String, Object> row, Long lsn) {
        int inserted = jdbc.update(LEDGER, zone, row.get("hull_no"), row.get("block_id"), row.get("scan_id"),
                row.get("judged_status"), row.get("src_received_at"), row.get("applied_at"), lsn);
        if (inserted == 0) {
            return false;
        }
        sender.send(zone, row);
        return true;
    }

    private static boolean isPoison(RuntimeException ex) {
        return ex instanceof RfcSender.RfcRejectedException
                || (ex instanceof NonTransientDataAccessException && !(ex instanceof DataAccessResourceFailureException));
    }

    private void deadLetter(CdcEvent e, RuntimeException ex) {
        String error = rootMessage(ex);
        log.error("dead letter: {} lsn={} : {}", e.physical(), e.lsn(), error);
        jdbc.update("""
                INSERT INTO ops.cdc_dead_letter (pipeline, route, source, lsn, payload, error)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb), ?)
                """, pipeline, ROUTE, e.table().toString(), e.lsn(), e.raw(), error);
        meters.counter("hotdb.cdc.dead.letter", "route", ROUTE).increment();
    }

    /** 권역 RDB 반영(applied_at) → SAP 송신. 앱 시계라 서버가 다르면 편차가 섞인다 — DB 시계 값은 ops.rfc_sent 로 본다. */
    private void recordLag(String zone, Object appliedAt) {
        if (appliedAt == null) {
            return;
        }
        try {
            Instant from = OffsetDateTime.parse(appliedAt.toString()).toInstant();
            lagTimers.computeIfAbsent(zone, z -> Timer.builder("hotdb.rfc.lag.source")
                            .description("SAP 송신 - 권역 RDB 반영(applied_at). 서버 시계 편차가 섞인다")
                            .tag("zone", z).publishPercentileHistogram().register(meters))
                    .record(Duration.between(from, Instant.now()));
        } catch (RuntimeException ignored) {
            // 지표용이라 형식이 달라도 송신은 막지 않는다
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
