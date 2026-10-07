package dev.hotdb.rfc;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * SAP 쪽 "RDB field data" 표(Z 테이블)에 실적을 JDBC 로 바로 쓴다 — 운영은 HANA, 로컬은 SAP 대역 DB.
 *
 * <p>RFC 함수 호출(JCo)은 S-user 로 커넥터를 받아야 해서 아직 없다. 생기면 {@link RfcSender} 구현을 하나 더 두고
 * {@code hotdb.rfc.sender=jco} 로 바꾼다 — 나머지(CDC · 중복 차단 · 재시도)는 그대로다.
 *
 * <p>표의 PK(실적 키 + 판정 상태)가 SAP 쪽 멱등 키다. 송신 직후 커밋 전에 죽어 같은 건을 다시 보내면
 * 중복 키로 거절되는데, 이미 들어가 있는 것이므로 성공으로 본다.
 */
public class JdbcRfcSender implements RfcSender {

    private static final Logger log = LoggerFactory.getLogger(JdbcRfcSender.class);

    private final JdbcTemplate sap;
    private final String sql;

    public JdbcRfcSender(JdbcTemplate sap, String table) {
        this.sap = sap;
        this.sql = "INSERT INTO " + table + " (zone, hull_no, block_id, scan_id, judged_status, device_id, completed_at,"
                + " block_progress_rate, match_confidence, sent_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";
    }

    @Override
    public void send(String zone, Map<String, Object> row) {
        try {
            sap.update(sql, zone, row.get("hull_no"), row.get("block_id"), String.valueOf(row.get("scan_id")),
                    row.get("judged_status"), row.get("device_id"), timestamp(row.get("completed_at")),
                    number(row.get("block_progress_rate")), number(row.get("match_confidence")));
        } catch (DuplicateKeyException e) {
            log.debug("이미 SAP 에 있음 — 성공으로 본다: {}/{} {}", row.get("hull_no"), row.get("block_id"), row.get("judged_status"));
        } catch (DataIntegrityViolationException e) {
            // 값이 SAP 표에 안 맞음 (길이 · NOT NULL) — 다시 보내도 같다
            throw new RfcRejectedException("SAP 표가 거절: " + e.getMostSpecificCause().getMessage());
        }
    }

    // CDC 값은 ISO 문자열이다. HANA 는 오프셋이 붙은 문자열을 TIMESTAMP 로 못 읽어 UTC Timestamp 로 바꿔 넘긴다
    private static Timestamp timestamp(Object v) {
        return v == null ? null : Timestamp.from(OffsetDateTime.parse(v.toString()).toInstant());
    }

    private static Double number(Object v) {
        return v == null ? null : ((Number) v).doubleValue();
    }
}
