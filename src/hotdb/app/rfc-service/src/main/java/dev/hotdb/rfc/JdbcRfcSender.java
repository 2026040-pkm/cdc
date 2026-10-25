package dev.hotdb.rfc;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * SAP 쪽 "RDB field data" 표(Z 테이블 zhotdb_progress)에 실적을 JDBC 로 바로 쓴다 — 운영은 HANA, 로컬은 SAP 대역.
 *
 * <p>RFC 함수 호출(JCo)은 S-user 로 커넥터를 받아야 해서 아직 없다. 생기면 {@link RfcSender} 구현을 하나 더 두고
 * {@code hotdb.rfc.sender=jco} 로 바꾼다 — 나머지(CDC · 중복 차단 · 재시도)는 그대로다.
 *
 * <p>표의 PK(anchor_id · as_of · payload_hash)가 SAP 쪽 멱등 키다 (V20 — 같은 상태에서 진척이 바뀌면 새 행. SAP 는
 * 앵커 · 기준일의 마지막 sent_at 행을 읽는다). 송신 직후 커밋 전에 죽어 같은 건을 다시 보내면
 * 중복 키로 거절되는데, 이미 들어가 있는 것이므로 성공으로 본다.
 */
public class JdbcRfcSender implements RfcSender {

    private static final Logger log = LoggerFactory.getLogger(JdbcRfcSender.class);

    private final JdbcTemplate sap;
    private final String sql;

    public JdbcRfcSender(JdbcTemplate sap, String table) {
        this.sap = sap;
        this.sql = "INSERT INTO " + table + " (anchor_id, as_of, state, anchor_type, hull_no, block_no, auto_pct,"
                + " legacy_pct, gap_pct, last_cp, planned_qty, done_qty, site, judged_at, payload_hash, sent_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)";
    }

    @Override
    public void send(Map<String, Object> row) {
        try {
            sap.update(sql, row.get("anchor_id"), date(row.get("as_of")), row.get("state"), row.get("anchor_type"),
                    row.get("hull_no"), row.get("block_no"), number(row.get("auto_pct")), number(row.get("legacy_pct")),
                    number(row.get("gap_pct")), row.get("last_cp"), number(row.get("planned_qty")),
                    number(row.get("done_qty")), row.get("site"), timestamp(row.get("judged_at")), row.get("payload_hash"));
        } catch (DuplicateKeyException e) {
            log.debug("이미 SAP 에 있음 — 성공으로 본다: {} {} {}", row.get("anchor_id"), row.get("as_of"), row.get("state"));
        } catch (DataIntegrityViolationException e) {
            // 값이 SAP 표에 안 맞음 (길이 · NOT NULL) — 다시 보내도 같다
            throw new RfcRejectedException("SAP 표가 거절: " + e.getMostSpecificCause().getMessage());
        }
    }

    // CDC 값은 ISO 문자열이다. HANA 는 오프셋이 붙은 문자열을 TIMESTAMP 로 못 읽어 UTC Timestamp 로 바꿔 넘긴다
    private static Timestamp timestamp(Object v) {
        return v == null ? null : Timestamp.from(OffsetDateTime.parse(v.toString()).toInstant());
    }

    // Debezium 은 date 를 epoch 일수(정수) 또는 ISO 문자열로 준다
    private static Date date(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return Date.valueOf(LocalDate.ofEpochDay(n.longValue()));
        }
        return Date.valueOf(LocalDate.parse(v.toString()));
    }

    private static Double number(Object v) {
        if (v == null) {
            return null;
        }
        return v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
    }
}
