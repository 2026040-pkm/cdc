package dev.embeddedcdc.infrastructure.persistence;

import dev.embeddedcdc.domain.port.out.PassthroughRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * PassthroughRepository 의 JDBC 구현.
 *
 * JPA 를 쓰지 않는 이유: 엔티티가 없다. 컬럼 목록이 값으로 들어오므로 문장을 조립해야 하고,
 * ON CONFLICT 는 어차피 네이티브다. JdbcTemplate 은 JPA 와 같은 DataSource 를 쓰고
 * JpaTransactionManager 가 같은 커넥션을 노출하므로 BatchApplier 의 트랜잭션에 그대로 참여한다.
 *
 * <b>값은 전부 문자열로 보내고 타입은 DB 가 정한다.</b> Types.OTHER 로 바인딩하면 pgjdbc 가
 * OID 를 비워 보내고, 서버가 컬럼 타입(timestamptz·jsonb·uuid·boolean…)에 맞춰 캐스팅한다.
 * Debezium 이 timestamptz 를 ISO-8601 문자열로, NUMERIC 을 문자열로 보내므로 손실이 없다.
 *
 * 테이블·컬럼 이름은 핸들러 설정의 코드 상수다. 이벤트 본문에서 온 값이 SQL 텍스트에
 * 섞이는 경로는 없다 — 값은 언제나 바인딩 파라미터다.
 */
@Repository
@Transactional
@RequiredArgsConstructor
public class JdbcPassthroughRepository implements PassthroughRepository {

    private static final String LSN = "source_lsn";

    private final JdbcTemplate jdbc;
    private final Map<String, String> upsertSql = new ConcurrentHashMap<>();
    private final Map<String, String> deleteSql = new ConcurrentHashMap<>();

    @Override
    public int upsertIfNewer(String table, List<String> keyColumns, List<String> columns,
                             Map<String, String> values, long sourceLsn) {
        String sql = upsertSql.computeIfAbsent(table, t -> buildUpsert(t, keyColumns, columns));
        List<Object> params = new ArrayList<>(columns.size() + 1);
        for (String column : columns) {
            params.add(new SqlParameterValue(Types.OTHER, values.get(column)));
        }
        params.add(sourceLsn);
        return jdbc.update(sql, params.toArray());
    }

    @Override
    public int deleteIfNewer(String table, List<String> keyColumns, Map<String, String> keyValues, long sourceLsn) {
        String sql = deleteSql.computeIfAbsent(table, t -> buildDelete(t, keyColumns));
        List<Object> params = new ArrayList<>(keyColumns.size() + 1);
        for (String column : keyColumns) {
            params.add(new SqlParameterValue(Types.OTHER, keyValues.get(column)));
        }
        params.add(sourceLsn);
        return jdbc.update(sql, params.toArray());
    }

    /**
     * <pre>
     * INSERT INTO t (c1, c2, ..., source_lsn) VALUES (?, ?, ..., ?)
     * ON CONFLICT (k1, k2) DO UPDATE SET c1 = EXCLUDED.c1, ..., source_lsn = EXCLUDED.source_lsn
     * WHERE EXCLUDED.source_lsn > t.source_lsn
     * </pre>
     * car 의 upsertIfNewer 와 같은 모양이다. 판정이 한 문장 안에 있어야 하는 이유도 같다.
     */
    private static String buildUpsert(String table, List<String> keyColumns, List<String> columns) {
        String cols = String.join(", ", columns) + ", " + LSN;
        String marks = columns.stream().map(c -> "?").collect(Collectors.joining(", ")) + ", ?";
        String sets = columns.stream()
                .filter(c -> !keyColumns.contains(c))
                .map(c -> c + " = EXCLUDED." + c)
                .collect(Collectors.joining(", "));
        sets = sets.isEmpty() ? LSN + " = EXCLUDED." + LSN : sets + ", " + LSN + " = EXCLUDED." + LSN;
        return "INSERT INTO " + table + " (" + cols + ") VALUES (" + marks + ")"
                + " ON CONFLICT (" + String.join(", ", keyColumns) + ") DO UPDATE SET " + sets
                + " WHERE EXCLUDED." + LSN + " > " + table + "." + LSN;
    }

    private static String buildDelete(String table, List<String> keyColumns) {
        String where = keyColumns.stream().map(c -> c + " = ?").collect(Collectors.joining(" AND "));
        return "DELETE FROM " + table + " WHERE " + where + " AND " + LSN + " < ?";
    }
}
