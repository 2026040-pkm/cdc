package dev.hotdb.cdc.route;

import dev.hotdb.cdc.event.TableId;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.jdbc.core.JdbcTemplate;

/** 라우트 대조에 쓰는 DB 메타데이터. 테스트에서는 가짜로 바꾼다. */
public interface Catalog {

    /** 컬럼 이름 → 타입(format_type). 표가 없으면 empty. */
    Optional<Map<String, String>> columnsOf(TableId table);

    /** 질의 결과 컬럼 이름. */
    Set<String> columnsOfQuery(String sql);

    static Catalog jdbc(JdbcTemplate jdbc) {
        return new Catalog() {
            @Override
            public Optional<Map<String, String>> columnsOf(TableId table) {
                Map<String, String> cols = new LinkedHashMap<>();
                jdbc.query("""
                        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                          FROM pg_attribute a
                         WHERE a.attrelid = to_regclass(?) AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum
                        """, rs -> {
                    cols.put(rs.getString(1), rs.getString(2));
                }, SqlBuilder.q(table.schema()) + "." + SqlBuilder.q(table.table()));
                return cols.isEmpty() ? Optional.empty() : Optional.of(cols);
            }

            @Override
            public Set<String> columnsOfQuery(String sql) {
                return jdbc.query("SELECT * FROM (" + sql + ") q LIMIT 0", rs -> {
                    Set<String> out = new LinkedHashSet<>();
                    var md = rs.getMetaData();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        out.add(md.getColumnLabel(i));
                    }
                    return out;
                });
            }
        };
    }
}
