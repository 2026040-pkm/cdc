package dev.hotdb.cdc.route;

import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.route.CompiledRoute.Column;
import dev.hotdb.cdc.route.ExprParser.Policy;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** 라우트 → SQL 한 문장. 순수 함수라 단위 테스트로 결과 SQL 을 그대로 확인한다. */
public final class SqlBuilder {

    private SqlBuilder() {}

    public record Built(String sql, List<Column> params) {}

    public static Built build(RouteMode mode, TableId target, List<Column> cols, List<String> keys,
                              String newerThan, List<String> changeOf, List<String> partitionBy, String orderBy) {
        String table = q(target.schema()) + "." + q(target.table());
        return switch (mode) {
            case UPSERT -> upsert(table, cols, keys, newerThan);
            case INSERT -> insert(table, cols);
            case INSERT_ON_CHANGE -> insertOnChange(table, cols, changeOf, partitionBy, orderBy);
            case DELETE -> delete(table, cols, keys);
        };
    }

    private static Built upsert(String table, List<Column> cols, List<String> keys, String newerThan) {
        List<Column> params = new ArrayList<>();
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" AS t (")
                .append(names(cols, "")).append(") VALUES (").append(values(cols, params)).append(")")
                .append(" ON CONFLICT (").append(keys.stream().map(SqlBuilder::q).collect(Collectors.joining(", "))).append(")");
        List<String> sets = new ArrayList<>();
        for (Column c : cols) {
            if (keys.contains(c.name()) || c.policy() == Policy.KEEP) {
                continue;
            }
            String col = q(c.name());
            sets.add(switch (c.policy()) {
                case COALESCE -> col + " = COALESCE(EXCLUDED." + col + ", t." + col + ")";
                // LEAST · GREATEST 는 NULL 을 건너뛴다 — 한쪽이 비어도 다른 쪽 값이 남는다
                case MIN -> col + " = LEAST(t." + col + ", EXCLUDED." + col + ")";
                case MAX -> col + " = GREATEST(t." + col + ", EXCLUDED." + col + ")";
                default -> col + " = EXCLUDED." + col;
            });
        }
        if (sets.isEmpty()) {
            sb.append(" DO NOTHING");
        } else {
            sb.append(" DO UPDATE SET ").append(String.join(", ", sets));
            if (newerThan != null) {
                String n = q(newerThan);
                sb.append(" WHERE t.").append(n).append(" IS NULL OR t.").append(n).append(" <= EXCLUDED.").append(n);
            }
        }
        return new Built(sb.toString(), params);
    }

    private static Built insert(String table, List<Column> cols) {
        List<Column> params = new ArrayList<>();
        String sql = "INSERT INTO " + table + " (" + names(cols, "") + ") VALUES (" + values(cols, params)
                + ") ON CONFLICT DO NOTHING";
        return new Built(sql, params);
    }

    private static Built insertOnChange(String table, List<Column> cols, List<String> changeOf,
                                        List<String> partitionBy, String orderBy) {
        List<Column> params = new ArrayList<>();
        String sameGroup = partitionBy.stream().map(p -> "x." + q(p) + " = v." + q(p)).collect(Collectors.joining(" AND "));
        String sameValue = changeOf.stream().map(c -> "l." + q(c) + " IS NOT DISTINCT FROM v." + q(c))
                .collect(Collectors.joining(" AND "));
        String sql = "INSERT INTO " + table + " (" + names(cols, "") + ")"
                + " SELECT " + names(cols, "v.") + " FROM (VALUES (" + values(cols, params) + ")) AS v (" + names(cols, "") + ")"
                + " WHERE NOT EXISTS (SELECT 1 FROM (SELECT " + changeOf.stream().map(c -> "x." + q(c)).collect(Collectors.joining(", "))
                + " FROM " + table + " x WHERE " + sameGroup + " ORDER BY x." + q(orderBy) + " DESC LIMIT 1) l WHERE " + sameValue + ")"
                + " ON CONFLICT DO NOTHING";
        return new Built(sql, params);
    }

    private static Built delete(String table, List<Column> cols, List<String> keys) {
        List<Column> params = new ArrayList<>();
        List<String> where = new ArrayList<>();
        for (String k : keys) {
            Column c = cols.stream().filter(x -> x.name().equals(k)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("delete 키 " + k + " 의 식이 columns 에 없습니다"));
            where.add(q(k) + " = " + placeholder(c, params));
        }
        return new Built("DELETE FROM " + table + " WHERE " + String.join(" AND ", where), params);
    }

    private static String names(List<Column> cols, String prefix) {
        return cols.stream().map(c -> prefix + q(c.name())).collect(Collectors.joining(", "));
    }

    private static String values(List<Column> cols, List<Column> params) {
        return cols.stream().map(c -> placeholder(c, params)).collect(Collectors.joining(", "));
    }

    private static String placeholder(Column c, List<Column> params) {
        String literal = c.expr().sqlLiteral();
        if (literal != null) {
            return literal;
        }
        params.add(c);
        return "CAST(? AS " + c.sqlType() + ")";
    }

    static String q(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }
}
