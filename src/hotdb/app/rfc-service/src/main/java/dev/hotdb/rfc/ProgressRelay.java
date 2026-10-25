package dev.hotdb.rfc;

import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.port.CdcSink;
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
 * hot.p_progress 변경 → SAP. 앵커 × 기준일 × 상태 하나당 한 번만 보낸다.
 *
 * <p>통합안 · 정규화에서 SAP 로 가는 실적은 {@code hot.p_progress}(앵커 × 기준일) 한 표다 — PoC 의
 * svc.actual_result(스캔 1건)와 달리 판별이 끝난 값(state · auto_pct · legacy_pct · last_cp)을 보낸다.
 *
 * <p>한 건마다 한 트랜잭션: {@code ops.rfc_progress_sent} 에 키를 넣고(이미 있으면 중복이라 건너뜀) → SAP 송신 → 커밋.
 * 송신이 실패하면 기록도 되돌려져 다음 시도에서 다시 보낸다. 송신은 됐는데 커밋 직전에 죽으면 한 번 더 간다 —
 * 그 한 건은 SAP 쪽 멱등 키(anchor_id · as_of · state)로 막는다.
 *
 * <p>같은 기준일에 같은 상태로 auto_pct 만 갱신되는 UPDATE 는 보내지 않는다 (키가 같다). 상태가 바뀌면
 * (inProgress → done) 새 키라 다시 간다 — 앵커당 하루 상태 수만큼.
 *
 * <p>실패 처리는 원인에 따라 다르다.
 * <ul>
 *   <li>내용 문제(SAP 거절 · 값이 표에 안 맞음) — 다시 해도 같으므로 바로 dead letter, 다음 건으로 간다
 *   <li>일시 장애(SAP · DB 접속) — 재시도, 소진하면 예외를 올린다. 엔진이 멈추고 컨테이너가 재시작되며,
 *       그동안 슬롯이 WAL 을 붙잡아 빠지는 실적이 없다
 * </ul>
 */
public class ProgressRelay implements CdcSink {

    private static final Logger log = LoggerFactory.getLogger(ProgressRelay.class);
    private static final String SCHEMA = "hot";
    private static final String TABLE = "p_progress";
    private static final String ROUTE = "rfc-progress";
    private static final String LEDGER = """
            INSERT INTO ops.rfc_progress_sent (anchor_id, as_of, state, src_judged_at, src_lsn, payload_hash)
            VALUES (?, CAST(? AS date), ?, CAST(? AS timestamptz), ?, ?)
            ON CONFLICT DO NOTHING
            """;

    private final String pipeline;
    private final RfcProperties policy;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final RfcSender sender;
    private final MeterRegistry meters;
    private final Map<String, Timer> lagTimers = new ConcurrentHashMap<>();

    public ProgressRelay(String pipeline, RfcProperties policy, JdbcTemplate jdbc, TransactionTemplate tx,
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
                // 실적 삭제는 SAP 로 되돌리지 않는다 (취소는 상태로 표현한다)
                meters.counter("hotdb.rfc.skipped", "reason", "delete").increment();
                continue;
            }
            if (Boolean.TRUE.equals(e.after().get("excluded"))) {
                // 제외 앵커(도면 상태 D · CAS/PAS · T-BAR · SSYW …)는 실적이 아니다
                meters.counter("hotdb.rfc.skipped", "reason", "excluded").increment();
                continue;
            }
            relay(e);
        }
    }

    private void relay(CdcEvent e) {
        Map<String, Object> row = e.after();
        String anchorType = String.valueOf(row.get("anchor_type"));   // DWG · WSTG · GDO · PAINT_ORDER
        long backoff = policy.backoffMs();
        for (int attempt = 0; ; attempt++) {
            try {
                boolean sent = Boolean.TRUE.equals(tx.execute(s -> sendOnce(row, e.lsn())));
                if (sent) {
                    meters.counter("hotdb.rfc.sent", "zone", anchorType).increment();
                    recordLag(anchorType, row.get("judged_at"));
                } else {
                    meters.counter("hotdb.rfc.duplicate", "zone", anchorType).increment();
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

    /**
     * 키 = (앵커, 기준일, 내용 해시). 상태가 같아도 진척 · 레거시 진척 · 마지막 절점이 바뀌면 다시 보낸다 (V20 F3).
     * @return 이번에 보냈으면 true, 이미 보낸 내용이면 false
     */
    private boolean sendOnce(Map<String, Object> row, Long lsn) {
        String hash = payloadHash(row);
        int inserted = jdbc.update(LEDGER, row.get("anchor_id"), asOf(row.get("as_of")), row.get("state"),
                row.get("judged_at"), lsn, hash);
        if (inserted == 0) {
            return false;
        }
        Map<String, Object> out = new java.util.LinkedHashMap<>(row);
        out.put("payload_hash", hash);
        sender.send(out);
        return true;
    }

    /** SAP 에 보이는 판정 내용(state · auto_pct · legacy_pct · last_cp)의 해시 16자. 숫자는 표기(40 · 40.0 · "40.00")를 맞춘다. */
    static String payloadHash(Map<String, Object> row) {
        String body = String.join("|", String.valueOf(row.get("state")), num(row.get("auto_pct")), num(row.get("legacy_pct")),
                String.valueOf(row.get("last_cp")));
        try {
            byte[] d = java.security.MessageDigest.getInstance("SHA-256").digest(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(d).substring(0, 16);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String num(Object v) {
        if (v == null) {
            return "null";
        }
        try {
            return new java.math.BigDecimal(v.toString()).stripTrailingZeros().toPlainString();
        } catch (NumberFormatException e) {
            return v.toString();
        }
    }

    /** Debezium 은 date 열을 1970-01-01 부터의 일수(정수)로 준다. 스냅샷 · 다른 변환기는 ISO 문자열 — 둘 다 ISO 날짜로 맞춘다. */
    static String asOf(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return java.time.LocalDate.ofEpochDay(n.longValue()).toString();
        }
        return v.toString();
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

    /** 판별(judged_at) → SAP 송신. 앱 시계라 서버가 다르면 편차가 섞인다 — DB 시계 값은 ops.rfc_progress_sent 로 본다. */
    private void recordLag(String zone, Object judgedAt) {
        if (judgedAt == null) {
            return;
        }
        try {
            Instant from = OffsetDateTime.parse(judgedAt.toString()).toInstant();
            lagTimers.computeIfAbsent(zone, z -> Timer.builder("hotdb.rfc.lag.source")
                            .description("SAP 송신 - 판별(judged_at). 서버 시계 편차가 섞인다")
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
