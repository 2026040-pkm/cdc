package dev.hotdb.cdc.route;

import dev.hotdb.cdc.event.Op;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.route.CompiledRoute.Column;
import dev.hotdb.cdc.route.Expr.Condition;
import dev.hotdb.cdc.route.ExprParser.ColumnDef;
import dev.hotdb.cdc.route.RoutingProperties.RouteSpec;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 라우트 설정 → {@link CompiledRoute}. 원천 · 대상 표의 실제 컬럼과 대조한다.
 *
 * <p>스키마가 바뀐 뒤 설정을 안 고쳤을 때 조용히 null 을 쓰며 도는 대신, 어긋난 곳을 전부 모아
 * 한 번에 보여 주고 기동을 거부한다. 원천 쪽 어긋남은 {@code strictSource=false} 로 경고만 낼 수 있다
 * (원천 컬럼을 먼저 지우고 서비스 설정을 나중에 배포하는 순서일 때).
 */
public class RouteCompiler {

    private static final Logger log = LoggerFactory.getLogger(RouteCompiler.class);

    private final Catalog catalog;
    private final Map<String, Set<String>> lookupColumns;
    private final boolean strictSource;

    /** @param lookupColumns 참조 이름 → 그 참조 질의의 결과 컬럼 */
    public RouteCompiler(Catalog catalog, Map<String, Set<String>> lookupColumns, boolean strictSource) {
        this.catalog = catalog;
        this.lookupColumns = lookupColumns;
        this.strictSource = strictSource;
    }

    public List<CompiledRoute> compile(List<RouteSpec> specs) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        List<CompiledRoute> out = new ArrayList<>();
        Set<String> names = new java.util.HashSet<>();
        for (RouteSpec spec : specs) {
            if (!spec.enabled()) {
                log.info("라우트 {} 꺼짐 (enabled=false)", spec.name());
                continue;
            }
            String where = "라우트 '" + spec.name() + "'";
            if (spec.name() == null || !names.add(spec.name())) {
                errors.add(where + ": name 이 비었거나 겹칩니다");
                continue;
            }
            try {
                compileOne(spec, where, errors, warnings).ifPresent(out::add);
            } catch (IllegalArgumentException e) {
                errors.add(where + ": " + e.getMessage());
            }
        }
        warnings.forEach(w -> log.warn("[라우트 대조] {}", w));
        if (!errors.isEmpty()) {
            throw new IllegalStateException("라우트 설정이 DB 스키마와 맞지 않습니다 (" + errors.size() + "건)\n  - "
                    + String.join("\n  - ", errors));
        }
        out.forEach(r -> log.info("라우트 {}: {} → {} [{}]\n  {}", r.name(), r.source(), r.target(), r.mode(), r.sql()));
        return out;
    }

    private Optional<CompiledRoute> compileOne(RouteSpec spec, String where, List<String> errors, List<String> warnings) {
        TableId source = TableId.parse(spec.source());
        TableId target = TableId.parse(spec.target());
        RouteMode mode = RouteMode.of(spec.mode());
        ExprParser parser = new ExprParser(spec.lookups().keySet());

        Optional<Map<String, String>> sourceCols = catalog.columnsOf(source);
        Optional<Map<String, String>> targetCols = catalog.columnsOf(target);
        if (sourceCols.isEmpty()) {
            errors.add(where + ": 원천 표 " + source + " 가 없습니다");
        }
        if (targetCols.isEmpty()) {
            errors.add(where + ": 대상 표 " + target + " 가 없습니다");
            return Optional.empty();
        }
        Map<String, String> tcols = targetCols.get();

        for (String lk : spec.lookups().keySet()) {
            if (!lookupColumns.containsKey(lk)) {
                errors.add(where + ": lookup '" + lk + "' 가 hotdb.lookups 에 정의되어 있지 않습니다");
            }
        }

        Map<String, Expr> lookupKeys = new LinkedHashMap<>();
        spec.lookups().forEach((k, v) -> lookupKeys.put(k, parser.expr(v)));
        List<Condition> when = spec.when().stream().map(parser::condition).toList();

        List<Column> cols = new ArrayList<>();
        spec.columns().forEach((name, text) -> {
            ColumnDef def = parser.column(text);
            String type = tcols.get(name);
            if (type == null) {
                errors.add(where + ": 대상 " + target + " 에 컬럼 " + name + " 가 없습니다 (있는 것: " + tcols.keySet() + ")");
                return;
            }
            cols.add(new Column(name, def.expr(), def.policy(), type));
        });

        // 원천 · 참조 필드 대조
        List<Expr> all = new ArrayList<>(lookupKeys.values());
        when.forEach(c -> all.add(c.left()));
        cols.forEach(c -> all.add(c.expr()));
        Expr lagFrom = spec.lagFrom() == null ? null : parser.expr(spec.lagFrom());
        if (lagFrom != null) {
            all.add(lagFrom);
        }
        for (Expr e : flatten(all)) {
            if (e instanceof Expr.RowField f && sourceCols.isPresent() && !sourceCols.get().containsKey(f.column())) {
                String msg = where + ": 원천 " + source + " 에 컬럼 " + f.column() + " 가 없습니다";
                (strictSource ? errors : warnings).add(msg);
            }
            if (e instanceof Expr.LookupField f && lookupColumns.containsKey(f.lookup())
                    && !lookupColumns.get(f.lookup()).contains(f.column())) {
                errors.add(where + ": lookup '" + f.lookup() + "' 결과에 컬럼 " + f.column() + " 가 없습니다");
            }
        }

        // 대상 쪽 키 · 모드 요구사항
        Set<String> colNames = cols.stream().map(Column::name).collect(Collectors.toSet());
        List<String> mustExist = new ArrayList<>(spec.keys());
        mustExist.addAll(spec.changeOf());
        mustExist.addAll(spec.partitionBy());
        if (spec.newerThan() != null) {
            mustExist.add(spec.newerThan());
        }
        if (spec.orderBy() != null) {
            mustExist.add(spec.orderBy());
        }
        for (String c : mustExist) {
            if (!colNames.contains(c)) {
                errors.add(where + ": " + c + " 는 columns 에 값 식이 있어야 합니다");
            }
        }
        switch (mode) {
            case UPSERT, DELETE -> {
                if (spec.keys().isEmpty()) {
                    errors.add(where + ": " + mode + " 는 keys 가 필요합니다");
                }
            }
            case INSERT_ON_CHANGE -> {
                if (spec.changeOf().isEmpty() || spec.partitionBy().isEmpty() || spec.orderBy() == null) {
                    errors.add(where + ": insert-on-change 는 change-of · partition-by · order-by 가 필요합니다");
                }
            }
            case INSERT -> { }
        }
        if (cols.isEmpty()) {
            errors.add(where + ": columns 가 비었습니다");
        }

        Set<Op> ops = EnumSet.noneOf(Op.class);
        spec.ops().forEach(o -> ops.add(Op.of(o.trim())));

        if (errors.stream().anyMatch(e -> e.startsWith(where))) {
            return Optional.empty();
        }
        SqlBuilder.Built built = SqlBuilder.build(mode, target, cols, spec.keys(), spec.newerThan(),
                spec.changeOf(), spec.partitionBy(), spec.orderBy());
        List<String> paramNames = built.params().stream().map(Column::name).toList();
        List<Integer> keyParams = spec.keys().stream().map(paramNames::indexOf).filter(i -> i >= 0).toList();
        return Optional.of(new CompiledRoute(spec.name(), source, ops, lookupKeys, when, mode, target,
                List.copyOf(cols), built.params(), built.sql(), lagFrom, keyParams));
    }

    private static List<Expr> flatten(List<Expr> exprs) {
        List<Expr> out = new ArrayList<>();
        for (Expr e : exprs) {
            if (e instanceof Expr.Conditional c) {
                out.addAll(flatten(List.of(c.value(), c.condition().left())));
            } else {
                out.add(e);
            }
        }
        return out;
    }
}
