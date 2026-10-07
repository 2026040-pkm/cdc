package dev.hotdb.cdc.route;

import dev.hotdb.cdc.port.SourceCapabilities;
import dev.hotdb.cdc.route.Expr.Condition;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** 라우트가 원천에게 바라는 것과 원천이 주는 것({@link SourceCapabilities})을 맞춰 본다. */
public final class CapabilityCheck {

    private CapabilityCheck() {}

    public static List<String> problems(List<CompiledRoute> routes, SourceCapabilities source) {
        List<String> out = new ArrayList<>();
        for (CompiledRoute r : routes) {
            if (!source.deletes() && r.mode() == RouteMode.DELETE) {
                out.add("라우트 '" + r.name() + "': mode delete — 원천(" + source.adapter() + ")은 삭제를 주지 않는다");
            }
            if (!source.beforeImage() && exprs(r).anyMatch(e -> e instanceof Expr.RowField f && f.before())) {
                out.add("라우트 '" + r.name() + "': before.* — 원천(" + source.adapter() + ")은 변경 전 값을 주지 않는다");
            }
            if (!source.commitTime() && exprs(r).anyMatch(e -> e instanceof Expr.Meta m && m.name().equals("commit_time"))) {
                out.add("라우트 '" + r.name() + "': meta.commit_time — 원천(" + source.adapter() + ")은 커밋 시각을 주지 않는다");
            }
            if (!"pg-lsn".equals(source.positionKind()) && exprs(r).anyMatch(e -> e instanceof Expr.Meta m && m.name().equals("lsn"))) {
                out.add("라우트 '" + r.name() + "': meta.lsn — 원천 위치가 " + source.positionKind() + " 다");
            }
        }
        return out;
    }

    /** 라우트가 쓰는 식 전부 (조건 · 값 · 참조 키 · 지연 기준을 펼쳐서) */
    private static Stream<Expr> exprs(CompiledRoute r) {
        Stream<Expr> roots = Stream.of(
                r.columns().stream().map(CompiledRoute.Column::expr),
                r.when().stream().map(Condition::left),
                r.lookupKeys().values().stream(),
                Stream.ofNullable(r.lagFrom())).flatMap(s -> s);
        return roots.flatMap(CapabilityCheck::flatten);
    }

    private static Stream<Expr> flatten(Expr e) {
        if (e instanceof Expr.Conditional c) {
            return Stream.concat(Stream.of(e), Stream.concat(flatten(c.value()), flatten(c.condition().left())));
        }
        return Stream.of(e);
    }
}
