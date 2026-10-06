package dev.hotdb.cdc.route;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 라우트 설정의 값 식.
 *
 * <pre>
 *   row.status            원천 행 (삭제면 before)
 *   before.status         변경 전 행
 *   tag.device_id         참조(lookup) 행
 *   meta.lsn · meta.op · meta.table · meta.commit_time
 *   'ASSEMBLY' · 123 · null   상수
 *   $now                  DB 의 clock_timestamp() — 반영 시각
 *   식 if 조건            조건이 거짓이면 null
 * </pre>
 */
public sealed interface Expr {

    Object eval(EvalContext ctx);

    /** SQL 에 파라미터가 아니라 식으로 들어가야 하는 값($now). */
    default String sqlLiteral() {
        return null;
    }

    record RowField(String column, boolean before) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            Map<String, Object> row = before ? ctx.event().before() : ctx.event().row();
            return row == null ? null : row.get(column);
        }
    }

    record LookupField(String lookup, String column) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            Map<String, Object> row = ctx.lookups().get(lookup);
            return row == null ? null : row.get(column);
        }
    }

    record Meta(String name) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            var e = ctx.event();
            return switch (name) {
                case "lsn" -> e.lsn();
                case "op" -> e.op().code();
                case "table" -> e.table().toString();
                case "commit_time" -> e.commitTsMs() == null ? null : Instant.ofEpochMilli(e.commitTsMs()).toString();
                default -> throw new IllegalStateException("모르는 meta: " + name);
            };
        }
    }

    record Literal(Object value) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            return value;
        }
    }

    record Now() implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            return null;
        }

        @Override
        public String sqlLiteral() {
            return "clock_timestamp()";
        }
    }

    record Conditional(Expr value, Condition condition) implements Expr {
        @Override
        public Object eval(EvalContext ctx) {
            return condition.test(ctx) ? value.eval(ctx) : null;
        }
    }

    /** 조건 하나. 값 비교는 문자열로 한다 (JSON 숫자 47 과 설정의 47 을 같게 보려고). */
    record Condition(Expr left, Kind kind, List<String> values) {
        public enum Kind { IN, NOT_IN, IS_NULL, NOT_NULL }

        public boolean test(EvalContext ctx) {
            Object v = left.eval(ctx);
            return switch (kind) {
                case IS_NULL -> v == null;
                case NOT_NULL -> v != null;
                case IN -> v != null && values.contains(Objects.toString(v));
                case NOT_IN -> v == null || !values.contains(Objects.toString(v));
            };
        }
    }
}
