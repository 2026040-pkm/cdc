package dev.hotdb.cdc.continuity;

import java.math.BigDecimal;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * {@code ops.cdc_checkpoint} — 이 파이프라인이 어디까지 처리했는지 DB 에 남긴다.
 *
 * <p>Debezium 오프셋은 서비스 볼륨에 있어서 볼륨이 날아가면 "처음부터 다시 읽는 것"과 "구간을 건너뛴 것"을
 * 구분할 수 없다. DB 에 남겨 두면 기동 때 슬롯과 대조할 수 있다 ({@link SlotContinuityGuard}).
 *
 * <p>남기는 값은 "처리한 이벤트의 최대 LSN" 과 "우리 슬롯이 확인한 위치(confirmed_flush_lsn)" 중 큰 쪽이다.
 * 원천이 걸러서 넘겨주지 않는 변경(include 밖의 표 · 압축 뭉치)과 heartbeat 로도 슬롯이 전진하는데,
 * 이벤트 LSN 만 남기면 그만큼 체크포인트가 슬롯보다 뒤처져 다음 기동에서 캡처 갭으로 오인한다
 * (이전 Java 판에서 유휴 36분 = 1.1MB 어긋남으로 실제로 겪었다). 값은 거꾸로 가지 않는다.
 */
public class CdcCheckpoints {

    private final String pipeline;
    private final String slot;
    private final JdbcTemplate jdbc;

    public CdcCheckpoints(String pipeline, String slot, JdbcTemplate jdbc) {
        this.pipeline = pipeline;
        this.slot = slot;
        this.jdbc = jdbc;
    }

    /**
     * 배치 하나를 끝낼 때 부른다. 이벤트마다 부르면 왕복이 배로 는다.
     *
     * @param maxEventLsn 이 배치에서 넘겨받은 이벤트의 최대 LSN. 없으면 null
     * @param slotMustBeActive true 면 슬롯이 지금 붙어 있을 때만 그 확인 위치를 쓴다 (돌고 있는 동안).
     *                         false 는 엔진을 닫은 직후 — 마지막 확인 위치를 남긴다
     */
    public void record(Long maxEventLsn, boolean slotMustBeActive) {
        jdbc.update("""
                INSERT INTO ops.cdc_checkpoint AS c (pipeline, slot_name, last_applied_lsn, updated_at)
                SELECT ?, ?, v.lsn, now()
                  FROM (SELECT GREATEST('0/0'::pg_lsn + CAST(? AS numeric),
                                        (SELECT confirmed_flush_lsn FROM pg_replication_slots
                                          WHERE slot_name = ? AND (active OR NOT ?))) AS lsn) v
                 WHERE v.lsn IS NOT NULL
                ON CONFLICT (pipeline) DO UPDATE
                   SET slot_name = EXCLUDED.slot_name,
                       last_applied_lsn = GREATEST(c.last_applied_lsn, EXCLUDED.last_applied_lsn),
                       updated_at = now()
                """, pipeline, slot, maxEventLsn == null ? null : BigDecimal.valueOf(maxEventLsn), slot,
                slotMustBeActive);
    }

    /** 마지막으로 처리한 위치 (LSN 표기). 없으면 처음 기동이다. */
    public Optional<String> lastApplied() {
        return jdbc.query("SELECT last_applied_lsn::text FROM ops.cdc_checkpoint WHERE pipeline = ?",
                rs -> rs.next() ? Optional.of(rs.getString(1)) : Optional.<String>empty(), pipeline);
    }
}
