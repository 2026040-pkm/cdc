package dev.hotdb.cdc.apply;

import dev.hotdb.cdc.HotdbCdcProperties;
import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.TableId;
import dev.hotdb.cdc.port.CdcSink;
import dev.hotdb.cdc.port.SourceCapabilities;
import dev.hotdb.cdc.route.CapabilityCheck;
import dev.hotdb.cdc.route.CompiledRoute;
import dev.hotdb.cdc.route.EvalContext;
import dev.hotdb.cdc.route.LookupCache;
import dev.hotdb.cdc.route.SqlValues;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 이벤트 배치 → 라우트별 JDBC 배치, 한 트랜잭션.
 *
 * <p>upsert 는 같은 키가 한 문장에 두 번 들어가지 않게 배치를 차례로 나눠 쓴다 — 접속 URL 의
 * {@code reWriteBatchedInserts} 가 배치를 다중 행 INSERT 하나로 합치는데, PG 는 그 안에 같은 키가 두 번 있으면
 * 문장 전체를 거부한다("cannot affect row a second time"). 밀려서 배치가 커질수록 같은 장비가 겹치므로
 * 나누지 않으면 밀림이 재시도를 부르고 재시도가 다시 밀림을 부른다.
 *
 * <p>실패는 {@link FailureVerdict} 셋으로 가른다 (이전 Java 판 embedded-cdc 의 V4 검증 그대로):
 * <ul>
 *   <li>RETRY — 배치 전체를 백오프하며 다시 한다. 소진하면 건 단위로 좁힌다
 *   <li>DEAD_LETTER — 다시 해도 같으므로 바로 건 단위로 좁혀 실패한 건만 {@code ops.cdc_dead_letter} 로 격리한다.
 *       독이 든 행 하나 때문에 파이프라인 전체가 멈추지 않는다
 *   <li>HALT — 표 · 컬럼이 없어졌거나 권한이 바뀌었다. {@link PipelineHaltedException} 을 던져 위치를 넘기지 않고 멈춘다
 * </ul>
 * 건 단위로 좁혔는데 실패 비율이 임계를 넘으면 그것도 구조 문제로 보고 멈춘다 — 격리하면 dead letter 가
 * 전체 트래픽을 삼키기 때문이다. 멈출 때는 dead letter 를 쓰지 않는다 (재기동 뒤 정상 반영돼도 남아 회계가 어긋난다).
 * DB 자체에 못 붙는 경우는 dead letter 도 못 쓰므로 예외가 원천까지 올라가 프로세스가 재시작된다.
 */
public class RouteApplier implements CdcSink {

    private static final Logger log = LoggerFactory.getLogger(RouteApplier.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Map<TableId, List<CompiledRoute>> routesBySource;
    /** 설정에 적힌 순서. 한 배치 안에서 이 순서로 쓴다 — 부모 표(scan) 라우트를 자식(artifact)보다 위에 둔다 */
    private final List<CompiledRoute> routeOrder;
    private final Map<String, LookupCache> lookups;
    private final Map<TableId, List<LookupCache>> reloadOn;
    private final HotdbCdcProperties.Apply policy;
    private final DeadLetters deadLetters;
    private final MeterRegistry meters;
    private final Map<String, Timer> lagTimers = new ConcurrentHashMap<>();

    public RouteApplier(JdbcTemplate jdbc, TransactionTemplate tx, List<CompiledRoute> routes,
                        Map<String, LookupCache> lookups, HotdbCdcProperties.Apply policy, DeadLetters deadLetters,
                        MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.routeOrder = List.copyOf(routes);
        this.routesBySource = routes.stream().collect(Collectors.groupingBy(CompiledRoute::source, LinkedHashMap::new,
                Collectors.toList()));
        this.lookups = lookups;
        this.reloadOn = new HashMap<>();
        lookups.values().forEach(c -> c.spec().reloadOn().forEach(t ->
                reloadOn.computeIfAbsent(TableId.parse(t), k -> new ArrayList<>()).add(c)));
        this.policy = policy;
        this.deadLetters = deadLetters;
        this.meters = meters;
        // 평소 0 인 지표도 시계열이 있어야 대시보드가 "없음"과 "0"을 구분한다
        meters.counter("hotdb.cdc.apply.retry");
        List.of("UNRECOVERABLE", "DLQ_RATIO").forEach(r -> meters.counter("hotdb.cdc.pipeline.halts", "reason", r));
        routes.forEach(r -> meters.counter("hotdb.cdc.dead.letter", "route", r.name()));
    }

    /** 라우트가 이 원천으로 될 일인지 — 버전 컬럼 폴링에 delete 라우트를 걸면 여기서 걸린다. */
    @Override
    public List<String> incompatibilities(SourceCapabilities source) {
        return CapabilityCheck.problems(routeOrder, source);
    }

    /** 이 원천 표를 쓰는 라우트나 참조가 있는지. 없으면 엔진 쪽에서 무시 이벤트로 센다. */
    @Override
    public boolean interestedIn(TableId table) {
        return routesBySource.containsKey(table) || reloadOn.containsKey(table);
    }

    private record Pending(CdcEvent event, Object[] params, Instant lagFrom) {}

    @Override
    public void apply(List<CdcEvent> events) {
        refreshLookups(events);
        Map<CompiledRoute, List<Pending>> work = plan(events);
        if (work.isEmpty()) {
            return;
        }
        if (!applyWithRetry(work)) {
            applyOneByOne(work);
        }
        Instant now = Instant.now();
        work.forEach((r, items) -> {
            meters.counter("hotdb.cdc.route.applied", "route", r.name()).increment(items.size());
            Timer lag = lagTimers.computeIfAbsent(r.name(), n -> Timer.builder("hotdb.cdc.lag.source")
                    .description("반영 시각 - 원천 기준 시각(lag-from). 서버 시계 편차가 섞인다")
                    .tag("route", n).publishPercentileHistogram().register(meters));
            for (Pending p : items) {
                if (p.lagFrom() != null) {
                    lag.record(Duration.between(p.lagFrom(), now));
                }
            }
        });
    }

    /**
     * dead letter 한 건을 다시 반영한다 (재처리기용). 실패하면 예외를 그대로 던진다 — 다시 격리하지 않는다.
     * 위치(체크포인트)는 건드리지 않는다 — 지나간 이벤트를 다시 쓰는 것이다.
     *
     * @param route 격리될 때의 라우트. 해석 실패({@link DeadLetters#UNPARSABLE})였으면 이 이벤트의 모든 라우트
     * @return 바뀐 행 수. 0 이면 더 새 값이 이미 있어 newer-than · ON CONFLICT 에 막힌 것이다
     */
    public int reapply(String route, CdcEvent event) {
        Map<CompiledRoute, List<Pending>> work = plan(List.of(event));
        if (!DeadLetters.UNPARSABLE.equals(route)) {
            work.keySet().removeIf(r -> !r.name().equals(route));
        }
        Integer rows = tx.execute(s -> work.entrySet().stream()
                .flatMap(w -> w.getValue().stream().map(p -> jdbc.update(w.getKey().sql(), p.params())))
                .mapToInt(Integer::intValue).sum());
        return rows == null ? 0 : rows;
    }

    /** 이벤트 → 라우트별 쓸 값. 설정 순서로 — 자식 표의 FK(지연 검사)가 같은 트랜잭션의 부모 행을 보게. */
    private Map<CompiledRoute, List<Pending>> plan(List<CdcEvent> events) {
        Map<CompiledRoute, List<Pending>> work = new LinkedHashMap<>();
        for (CdcEvent e : events) {
            List<CompiledRoute> routes = routesBySource.get(e.table());
            if (routes == null) {
                continue;
            }
            for (CompiledRoute r : routes) {
                if (!r.accepts(e.op())) {
                    continue;
                }
                EvalContext ctx = context(r, e);
                if (!r.matches(ctx)) {
                    meters.counter("hotdb.cdc.route.filtered", "route", r.name()).increment();
                    continue;
                }
                work.computeIfAbsent(r, k -> new ArrayList<>())
                        .add(new Pending(e, r.params(ctx), r.lagFrom() == null ? null : instant(r.lagFrom().eval(ctx))));
            }
        }
        Map<CompiledRoute, List<Pending>> ordered = new LinkedHashMap<>();
        for (CompiledRoute r : routeOrder) {
            List<Pending> items = work.get(r);
            if (items != null) {
                ordered.put(r, items);
            }
        }
        return ordered;
    }

    private EvalContext context(CompiledRoute r, CdcEvent e) {
        if (r.lookupKeys().isEmpty()) {
            return new EvalContext(e, Map.of());
        }
        EvalContext keyCtx = new EvalContext(e, Map.of());
        Map<String, Map<String, Object>> rows = new HashMap<>();
        r.lookupKeyValues(keyCtx).forEach((name, key) -> {
            Map<String, Object> row = lookups.get(name).get(key);
            if (row != null) {
                rows.put(name, row);
            }
        });
        return new EvalContext(e, rows);
    }

    private void refreshLookups(List<CdcEvent> events) {
        events.stream().map(CdcEvent::table).distinct()
                .flatMap(t -> reloadOn.getOrDefault(t, List.of()).stream()).distinct()
                .forEach(c -> c.reload("cdc"));
    }

    private boolean applyWithRetry(Map<CompiledRoute, List<Pending>> work) {
        long backoff = policy.backoffMs();
        for (int attempt = 0; attempt <= policy.maxRetries(); attempt++) {
            String[] current = {null};
            try {
                tx.executeWithoutResult(s -> work.forEach((r, items) -> {
                    current[0] = r.name();
                    for (List<Pending> wave : splitByKey(items, p -> r.conflictKey(p.params()))) {
                        jdbc.batchUpdate(r.sql(), wave.stream().map(Pending::params).toList());
                    }
                }));
                return true;
            } catch (RuntimeException ex) {
                switch (FailureVerdict.of(ex)) {
                    case HALT -> throw halt("UNRECOVERABLE", "라우트 " + current[0] + " 구조 문제 — 표 · 컬럼 · 권한을 확인한다", ex);
                    case DEAD_LETTER -> {
                        log.warn("배치 반영 실패 — 라우트 {} 데이터 오류라 건 단위로 좁힌다: {}", current[0], rootMessage(ex));
                        return false;
                    }
                    case RETRY -> { }
                }
                meters.counter("hotdb.cdc.apply.retry").increment();
                log.warn("배치 반영 실패 ({}/{}) — 라우트 {}: {}", attempt + 1, policy.maxRetries() + 1, current[0],
                        rootMessage(ex));
                sleep(backoff);
                backoff *= 2;
            }
        }
        return false;
    }

    /**
     * 같은 키가 한 묶음에 한 번만 있게 나눈다. 키마다 n 번째로 나온 건이 n 번째 묶음에 가므로
     * 같은 키끼리는 온 순서 그대로 쓰인다(newer-than · min 같은 정책이 순서대로 적용된다).
     * 키가 null(upsert 아님)이면 통째로 한 묶음.
     */
    static <T> List<List<T>> splitByKey(List<T> items, Function<T, Object> key) {
        if (items.isEmpty() || key.apply(items.get(0)) == null) {
            return List.of(items);
        }
        Map<Object, Integer> seen = new HashMap<>();
        List<List<T>> waves = new ArrayList<>();
        for (T item : items) {
            int n = seen.merge(key.apply(item), 1, Integer::sum) - 1;
            if (n == waves.size()) {
                waves.add(new ArrayList<>());
            }
            waves.get(n).add(item);
        }
        return waves;
    }

    private record Failure(CompiledRoute route, Pending item, String error) {}

    /** 건 단위로 반영하고 실패한 것만 격리한다. 정지 판단이 격리 기록보다 먼저다. */
    private void applyOneByOne(Map<CompiledRoute, List<Pending>> work) {
        List<Failure> failures = new ArrayList<>();
        int total = 0;
        for (Map.Entry<CompiledRoute, List<Pending>> w : work.entrySet()) {
            CompiledRoute r = w.getKey();
            for (Pending p : w.getValue()) {
                total++;
                try {
                    tx.executeWithoutResult(s -> jdbc.update(r.sql(), p.params()));
                } catch (RuntimeException ex) {
                    if (FailureVerdict.of(ex) == FailureVerdict.HALT) {
                        throw halt("UNRECOVERABLE", "라우트 " + r.name() + " 구조 문제 — 표 · 컬럼 · 권한을 확인한다", ex);
                    }
                    failures.add(new Failure(r, p, rootMessage(ex)));
                }
            }
        }
        if (shouldHalt(failures.size(), total, policy)) {
            throw halt("DLQ_RATIO", String.format("건 단위 반영 %d건 중 %d건 실패 (임계 %.0f%%) — 개별 데이터가 아니라 구조 문제로 본다",
                    total, failures.size(), policy.haltRatio() * 100), null);
        }
        for (Failure f : failures) {
            log.error("dead letter: 라우트 {} · {} lsn={} : {}", f.route().name(), f.item().event().physical(),
                    f.item().event().lsn(), f.error());
            deadLetters.store(f.route().name(), f.item().event(), f.error());
            meters.counter("hotdb.cdc.dead.letter", "route", f.route().name()).increment();
        }
    }

    /** 실패가 최소 건수 이상이고 비율이 임계를 넘으면 멈춘다 — 한두 건짜리 배치의 독이 든 행 하나로는 멈추지 않는다. */
    static boolean shouldHalt(int failures, int total, HotdbCdcProperties.Apply policy) {
        return failures >= policy.haltMinFailures() && failures > total * policy.haltRatio();
    }

    private PipelineHaltedException halt(String reason, String message, Throwable cause) {
        meters.counter("hotdb.cdc.pipeline.halts", "reason", reason).increment();
        String full = message + (cause == null ? "" : ": " + rootMessage(cause))
                + " — 위치를 넘기지 않고 멈춘다. 원인을 고친 뒤 재기동하면 이 배치부터 다시 받는다";
        log.error("파이프라인 정지 ({}): {}", reason, full);
        return new PipelineHaltedException(reason, full, cause);
    }

    private static Instant instant(Object v) {
        if (v == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(SqlValues.toSql(v)).toInstant();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
