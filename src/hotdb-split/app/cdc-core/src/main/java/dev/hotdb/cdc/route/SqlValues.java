package dev.hotdb.cdc.route;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map;

/**
 * JSON 값 → SQL 파라미터 문자열.
 *
 * <p>모든 값을 문자열로 넘기고 SQL 쪽에서 {@code CAST(? AS 대상타입)} 한다. PostgreSQL 은 텍스트에서
 * 어떤 타입으로든 입출력 변환이 되므로 timestamptz · uuid · real[] · jsonb 를 타입별 코드 없이 받는다.
 * 대상 컬럼 타입이 바뀌어도 코드는 그대로다.
 */
public final class SqlValues {

    private static final ObjectMapper JSON = new ObjectMapper();

    private SqlValues() {}

    public static String toSql(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof String s) {
            return s;
        }
        if (v instanceof Double d) {
            return BigDecimal.valueOf(d).toPlainString();
        }
        if (v instanceof Float f) {
            return new BigDecimal(Float.toString(f)).toPlainString();
        }
        if (v instanceof Number || v instanceof Boolean) {
            return v.toString();
        }
        if (v instanceof Collection<?> c) {
            return arrayLiteral(c);
        }
        if (v instanceof Map<?, ?> m) {
            try {
                return JSON.writeValueAsString(m);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return v.toString();
    }

    /** PostgreSQL 배열 리터럴 {a,b}. 문자열 원소는 따옴표로 감싼다. */
    static String arrayLiteral(Collection<?> c) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Object o : c) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            if (o == null) {
                sb.append("NULL");
            } else if (o instanceof Number || o instanceof Boolean) {
                sb.append(toSql(o));
            } else {
                sb.append('"').append(toSql(o).replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
            }
        }
        return sb.append('}').toString();
    }
}
