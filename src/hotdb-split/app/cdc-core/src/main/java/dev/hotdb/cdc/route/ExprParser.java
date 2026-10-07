package dev.hotdb.cdc.route;

import dev.hotdb.cdc.route.Expr.Condition;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** {@link Expr} · {@link Condition} 문자열 파서. 설정 오류는 기동 시점에 여기서 드러난다. */
public final class ExprParser {

    private static final Pattern FIELD = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)");
    private static final Pattern NUMBER = Pattern.compile("-?\\d+(\\.\\d+)?");
    private static final Pattern POLICY = Pattern.compile("^(.*?)\\s+\\|\\s*(keep|coalesce|overwrite|min|max)\\s*$");
    private static final Set<String> META = Set.of("lsn", "op", "table", "commit_time");

    private final Set<String> lookupNames;

    public ExprParser(Set<String> lookupNames) {
        this.lookupNames = lookupNames;
    }

    /** 컬럼 정의 = 식 + 충돌 시 갱신 정책. */
    public record ColumnDef(Expr expr, Policy policy) {}

    /** upsert 충돌 시: 새 값으로 덮기 · 기존 값 유지 · 새 값이 null 이면 기존 값 유지 · 둘 중 작은 값 · 큰 값. */
    public enum Policy { OVERWRITE, KEEP, COALESCE, MIN, MAX }

    public ColumnDef column(String text) {
        Matcher m = POLICY.matcher(text.trim());
        if (m.matches()) {
            return new ColumnDef(expr(m.group(1)), Policy.valueOf(m.group(2).toUpperCase()));
        }
        return new ColumnDef(expr(text), Policy.OVERWRITE);
    }

    public Expr expr(String text) {
        String t = text.trim();
        int ifAt = t.lastIndexOf(" if ");
        if (ifAt > 0) {
            return new Expr.Conditional(expr(t.substring(0, ifAt)), condition(t.substring(ifAt + 4)));
        }
        if (t.equals("$now")) {
            return new Expr.Now();
        }
        if (t.equals("null")) {
            return new Expr.Literal(null);
        }
        if (t.length() >= 2 && t.startsWith("'") && t.endsWith("'")) {
            return new Expr.Literal(t.substring(1, t.length() - 1));
        }
        if (NUMBER.matcher(t).matches()) {
            return new Expr.Literal(t);
        }
        Matcher f = FIELD.matcher(t);
        if (!f.matches()) {
            throw new IllegalArgumentException("식을 읽을 수 없습니다: '" + text + "'");
        }
        String scope = f.group(1);
        String name = f.group(2);
        return switch (scope) {
            case "row" -> new Expr.RowField(name, false);
            case "before" -> new Expr.RowField(name, true);
            case "meta" -> {
                if (!META.contains(name)) {
                    throw new IllegalArgumentException("모르는 meta." + name + " (쓸 수 있는 것: " + META + ")");
                }
                yield new Expr.Meta(name);
            }
            default -> {
                if (!lookupNames.contains(scope)) {
                    throw new IllegalArgumentException(
                            "'" + scope + "' 는 row/before/meta 도, 이 라우트의 lookup 도 아닙니다: " + text);
                }
                yield new Expr.LookupField(scope, name);
            }
        };
    }

    public Condition condition(String text) {
        String t = text.trim();
        if (t.endsWith(" is null")) {
            return new Condition(expr(t.substring(0, t.length() - 8)), Condition.Kind.IS_NULL, List.of());
        }
        if (t.endsWith(" not null")) {
            return new Condition(expr(t.substring(0, t.length() - 9)), Condition.Kind.NOT_NULL, List.of());
        }
        int ne = t.indexOf("!=");
        if (ne > 0) {
            return new Condition(expr(t.substring(0, ne)), Condition.Kind.NOT_IN, values(t.substring(ne + 2)));
        }
        int eq = t.indexOf('=');
        if (eq > 0) {
            return new Condition(expr(t.substring(0, eq)), Condition.Kind.IN, values(t.substring(eq + 1)));
        }
        throw new IllegalArgumentException("조건을 읽을 수 없습니다: '" + text + "' (예: tag.zone=ASSEMBLY)");
    }

    private static List<String> values(String text) {
        return Arrays.stream(text.split("\\|")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
}
