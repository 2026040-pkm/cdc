package dev.hotdb.poll;

import dev.hotdb.cdc.route.SqlValues;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * JDBC 원천 값 → 대상 파라미터 문자열. 대상은 {@code CAST(? AS 실제 타입)} 으로 받으므로 원천 DB 의
 * 타입 체계(HANA · Oracle · PostgreSQL)를 몰라도 된다.
 */
final class PollValues {

    private PollValues() {}

    static String toSql(Object v) throws SQLException {
        if (v == null) {
            return null;
        }
        if (v instanceof java.sql.Timestamp t) {
            return t.toLocalDateTime().toString();
        }
        if (v instanceof java.sql.Date d) {
            return d.toLocalDate().toString();
        }
        if (v instanceof java.sql.Time t) {
            return t.toLocalTime().toString();
        }
        if (v instanceof BigDecimal b) {
            return b.stripTrailingZeros().toPlainString();
        }
        if (v instanceof Clob c) {
            return c.getSubString(1, (int) c.length());
        }
        // Oracle TIMESTAMP 등 드라이버 고유 타입은 toString 이 ISO 에 가깝지 않을 수 있다 — 쓰이면 여기에 더한다
        return SqlValues.toSql(v);
    }
}
