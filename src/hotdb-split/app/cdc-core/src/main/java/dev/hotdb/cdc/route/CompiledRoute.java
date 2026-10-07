package dev.hotdb.cdc.route;

import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.route.Expr.Condition;
import dev.hotdb.cdc.route.ExprParser.Policy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 설정에서 만들어지고 DB 와 대조를 마친 라우트. SQL 은 기동 때 한 번 만든다.
 *
 * @param paramColumns SQL 의 ? 순서대로 값을 뽑을 컬럼
 * @param keyParams    upsert 충돌 키가 params 의 몇 번째인지. 상수로 박힌 키는 빠진다 (행마다 같으니 구분에 안 쓰인다)
 */
public record CompiledRoute(
        String name,
        TableId source,
        Set<Op> ops,
        Map<String, Expr> lookupKeys,
        List<Condition> when,
        RouteMode mode,
        TableId target,
        List<Column> columns,
        List<Column> paramColumns,
        String sql,
        Expr lagFrom,
        List<Integer> keyParams) {

    /** 대상 컬럼 하나. sqlType 은 대상 표의 실제 타입(format_type) — 값은 CAST(? AS sqlType) 로 들어간다. */
    public record Column(String name, Expr expr, Policy policy, String sqlType) {}

    public boolean accepts(Op op) {
        return ops.contains(op);
    }

    /** 이벤트에 붙일 참조 키 값들 (참조 이름 → 키 값). */
    public Map<String, Object> lookupKeyValues(EvalContext keyCtx) {
        Map<String, Object> out = new HashMap<>();
        lookupKeys.forEach((name, expr) -> out.put(name, expr.eval(keyCtx)));
        return out;
    }

    public boolean matches(EvalContext ctx) {
        for (Condition c : when) {
            if (!c.test(ctx)) {
                return false;
            }
        }
        return true;
    }

    /**
     * upsert 충돌 키 값. 한 다중 행 INSERT 안에 같은 키가 두 번 있으면 PG 가 문장 전체를 거부하므로
     * 반영기는 이 값으로 배치를 나눈다. upsert 가 아니면 null.
     */
    public List<Object> conflictKey(Object[] params) {
        if (mode != RouteMode.UPSERT) {
            return null;
        }
        List<Object> key = new ArrayList<>(keyParams.size());
        for (int i : keyParams) {
            key.add(params[i]);
        }
        return key;
    }

    public Object[] params(EvalContext ctx) {
        Object[] out = new Object[paramColumns.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = SqlValues.toSql(paramColumns.get(i).expr().eval(ctx));
        }
        return out;
    }
}
