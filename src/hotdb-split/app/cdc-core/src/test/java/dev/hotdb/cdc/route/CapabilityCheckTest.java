package dev.hotdb.cdc.route;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.port.SourceCapabilities;
import dev.hotdb.cdc.route.Expr.Condition;
import dev.hotdb.cdc.route.ExprParser.Policy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** 엔진을 버전 컬럼 폴링으로 바꿨을 때 — 그 원천으로는 안 되는 라우트를 기동에서 잡는다. */
class CapabilityCheckTest {

    static CompiledRoute route(String name, RouteMode mode, Expr column, List<Condition> when) {
        return new CompiledRoute(name, TableId.parse("tsdb.device"), Set.of(Op.CREATE, Op.UPDATE, Op.DELETE), Map.of(),
                when, mode, TableId.parse("svc.x"), List.of(new CompiledRoute.Column("c", column, Policy.OVERWRITE, "text")),
                List.of(), "", null, List.of());
    }

    static final CompiledRoute PLAIN = route("plain", RouteMode.UPSERT, new Expr.RowField("status", false), List.of());
    static final CompiledRoute DELETE = route("gone", RouteMode.DELETE, new Expr.RowField("device_id", false), List.of());
    static final CompiledRoute BEFORE = route("was", RouteMode.INSERT,
            new Expr.Conditional(new Expr.Literal("x"), new Condition(new Expr.RowField("status", true),
                    Condition.Kind.NOT_NULL, List.of())), List.of());

    @Test
    void 논리_복제는_다_된다() {
        assertThat(CapabilityCheck.problems(List.of(PLAIN, DELETE, BEFORE), SourceCapabilities.logicalReplication("pgoutput")))
                .isEmpty();
    }

    @Test
    void 버전_컬럼_폴링은_삭제와_변경_전_값을_못_준다() {
        List<String> problems = CapabilityCheck.problems(List.of(PLAIN, DELETE, BEFORE),
                SourceCapabilities.versionPolling("polling"));

        assertThat(problems).hasSize(2);
        assertThat(problems.get(0)).contains("'gone'", "삭제");
        assertThat(problems.get(1)).contains("'was'", "변경 전");   // 조건식 안에 숨은 before.* 도 찾는다
    }
}
