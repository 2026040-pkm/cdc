package dev.hotdb.cdc.apply;

import dev.hotdb.cdc.event.CdcEvent;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * {@code ops.cdc_dead_letter} — 반영하지 못한 이벤트. 여기 든 것은 유실이 아니라 "추적되는 미반영"이다.
 *
 * <p>원문(payload)만으로 이벤트를 다시 만들 수 있어야 재처리가 된다. 원문이 JSON 이 아니면(해석 실패)
 * JSON 문자열로 감싸 넣고, 꺼낼 때 벗긴다.
 */
public class DeadLetters {

    /** 해석하지 못한 원문의 라우트 자리 */
    public static final String UNPARSABLE = "(decode)";

    /** 재처리가 행을 바꿨다 */
    public static final String APPLIED = "APPLIED";
    /** 더 새 값이 이미 있어 0행 — 복구는 끝난 것으로 본다 */
    public static final String STALE_SKIPPED = "STALE_SKIPPED";

    private final String pipeline;
    private final JdbcTemplate jdbc;

    public DeadLetters(String pipeline, JdbcTemplate jdbc) {
        this.pipeline = pipeline;
        this.jdbc = jdbc;
    }

    public void store(String route, CdcEvent e, String error) {
        jdbc.update("""
                INSERT INTO ops.cdc_dead_letter (pipeline, route, source, lsn, payload, error)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb), ?)
                """, pipeline, route, e.table().toString(), e.lsn(), e.raw(), error);
    }

    /** 원천 레코드를 이벤트로 바꾸지 못했다 — 깨진 한 건이 배치 전체를 막지 않게 원문째 남긴다. */
    public void storeUnparsable(String raw, String error) {
        jdbc.update("""
                INSERT INTO ops.cdc_dead_letter (pipeline, route, source, lsn, payload, error)
                VALUES (?, ?, '(unknown)', NULL, to_jsonb(CAST(? AS text)), ?)
                """, pipeline, UNPARSABLE, raw == null ? "" : raw, error);
    }

    public record Pending(long id, String route, String raw) {}

    /** 재처리 신청된 건. 파이프라인 하나는 슬롯 하나라 한 프로세스만 돈다 — 잠그지 않는다. */
    public List<Pending> claimRequested(int limit) {
        return jdbc.query("""
                SELECT id, route,
                       CASE WHEN jsonb_typeof(payload) = 'string' THEN payload #>> '{}' ELSE payload::text END
                  FROM ops.cdc_dead_letter
                 WHERE pipeline = ? AND status = 'RETRY_REQUESTED'
                 ORDER BY id
                 LIMIT ?
                """, (rs, i) -> new Pending(rs.getLong(1), rs.getString(2), rs.getString(3)), pipeline, limit);
    }

    public void resolved(long id, String resolution) {
        jdbc.update("""
                UPDATE ops.cdc_dead_letter
                   SET status = 'RESOLVED', resolution = ?, resolved_at = now(), attempts = attempts + 1
                 WHERE id = ?
                """, resolution, id);
    }

    /** 다시 해도 안 됐다 — PENDING 으로 돌려 사람이 다시 보게 한다. */
    public void retryFailed(long id, String error) {
        jdbc.update("""
                UPDATE ops.cdc_dead_letter
                   SET status = 'PENDING', last_error = ?, attempts = attempts + 1
                 WHERE id = ?
                """, error, id);
    }
}
