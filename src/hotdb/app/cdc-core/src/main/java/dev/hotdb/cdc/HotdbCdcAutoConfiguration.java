package dev.hotdb.cdc;

import dev.hotdb.cdc.adapter.debezium.DebeziumAdapterConfiguration;
import dev.hotdb.cdc.apply.DeadLetterReprocessor;
import dev.hotdb.cdc.apply.DeadLetters;
import dev.hotdb.cdc.apply.RouteApplier;
import dev.hotdb.cdc.continuity.CdcCheckpoints;
import dev.hotdb.cdc.continuity.SlotContinuityGuard;
import dev.hotdb.cdc.port.CdcSink;
import dev.hotdb.cdc.port.CdcSource;
import dev.hotdb.cdc.route.Catalog;
import dev.hotdb.cdc.route.CompiledRoute;
import dev.hotdb.cdc.route.LookupCache;
import dev.hotdb.cdc.route.RouteCompiler;
import dev.hotdb.cdc.route.RoutingProperties;
import dev.hotdb.cdc.timescale.HypertableResolver;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * cdc-core 를 의존성에 넣고 {@code hotdb.cdc.pipeline} 을 주면 켜진다.
 * 서비스는 설정(application.yml)만 들고, 라우팅 · 반영 코드는 여기서 받는다.
 *
 * <p>포트 두 개로 나뉜다:
 * <ul>
 *   <li>{@link CdcSource} (입력) — 어댑터가 채운다. {@code hotdb.cdc.adapter} 로 고르며 기본은 debezium.
 *       새 어댑터는 {@code adapter/<이름>} 패키지에 구현 + 설정 클래스를 두고 아래 {@code @Import} 에 더한다.
 *   <li>{@link CdcSink} (출력) — 기본은 설정 라우트로 표에 쓰는 {@link RouteApplier}.
 *       표가 아닌 곳으로 보내는 서비스는 이 빈을 직접 두면 그것을 쓴다.
 * </ul>
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration",
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration"})
@ConditionalOnProperty("hotdb.cdc.pipeline")
@EnableConfigurationProperties({HotdbCdcProperties.class, RoutingProperties.class})
@Import(DebeziumAdapterConfiguration.class)
public class HotdbCdcAutoConfiguration {

    /** DB 를 읽는 빈들은 전부 이 빈을 받는다 — DB 가 응답할 때까지 기동을 멈춰 두고, 죽지는 않는다. */
    @Bean
    DbReadyGate dbReadyGate(HotdbCdcProperties props, JdbcTemplate jdbc) {
        return new DbReadyGate(props.pipeline(), jdbc, props.dbWait());
    }

    @Bean
    HypertableResolver hypertableResolver(JdbcTemplate jdbc, MeterRegistry meters, DbReadyGate gate) {
        return new HypertableResolver(jdbc, meters);
    }

    @Bean
    Map<String, LookupCache> hotdbLookups(RoutingProperties routing, JdbcTemplate jdbc, MeterRegistry meters, DbReadyGate gate) {
        Map<String, LookupCache> out = new LinkedHashMap<>();
        routing.lookups().forEach((name, spec) -> {
            LookupCache c = new LookupCache(name, spec, jdbc, meters);
            c.reload("startup");
            out.put(name, c);
        });
        return out;
    }

    @Bean
    List<CompiledRoute> hotdbRoutes(RoutingProperties routing, HotdbCdcProperties props, JdbcTemplate jdbc,
                                    Map<String, LookupCache> hotdbLookups, DbReadyGate gate) {
        Catalog catalog = Catalog.jdbc(jdbc);
        Map<String, Set<String>> lookupCols = new LinkedHashMap<>();
        hotdbLookups.forEach((n, c) -> lookupCols.put(n, catalog.columnsOfQuery(c.spec().sql())));
        return new RouteCompiler(catalog, lookupCols, props.strictSource()).compile(routing.routes());
    }

    @Bean
    DeadLetters deadLetters(HotdbCdcProperties props, JdbcTemplate jdbc, DbReadyGate gate) {
        return new DeadLetters(props.pipeline(), jdbc);
    }

    /** 처리 위치 기록 · 캡처 갭 검사 — 어느 어댑터든 PostgreSQL 슬롯을 읽으므로 여기 둔다. */
    @Bean
    CdcCheckpoints cdcCheckpoints(HotdbCdcProperties props, JdbcTemplate jdbc, DbReadyGate gate) {
        return new CdcCheckpoints(props.pipeline(), props.slot(), jdbc);
    }

    @Bean
    SlotContinuityGuard slotContinuityGuard(HotdbCdcProperties props, JdbcTemplate jdbc, DbReadyGate gate) {
        return new SlotContinuityGuard(props.pipeline(), props.slot(), jdbc);
    }

    @Bean
    @ConditionalOnMissingBean(CdcSink.class)
    RouteApplier routeApplier(HotdbCdcProperties props, JdbcTemplate jdbc, TransactionTemplate tx,
                              List<CompiledRoute> hotdbRoutes, Map<String, LookupCache> hotdbLookups,
                              DeadLetters deadLetters, MeterRegistry meters) {
        return new RouteApplier(jdbc, tx, hotdbRoutes, hotdbLookups, props.apply(), deadLetters, meters);
    }

    /**
     * 사람이 RETRY_REQUESTED 로 표시한 dead letter 를 다시 반영한다. 라우트 반영기를 쓰는 서비스만 —
     * 싱크를 직접 둔 서비스(RFC Provider)는 dead letter 의 뜻이 달라(SAP 거절) 그 서비스가 다룬다.
     */
    @Bean
    @ConditionalOnProperty(name = "hotdb.cdc.dead-letter.reprocess-enabled", havingValue = "true", matchIfMissing = true)
    DeadLetterReprocessor deadLetterReprocessor(ObjectProvider<CdcSink> sink, DeadLetters deadLetters,
                                                ObjectProvider<CdcSource> source, HotdbCdcProperties props,
                                                MeterRegistry meters) {
        return sink.getIfAvailable() instanceof RouteApplier applier
                ? new DeadLetterReprocessor(deadLetters, applier, source.getObject(), props.deadLetter(), meters)
                : null;
    }

    /** 어댑터가 무엇이든 같은 모양 — 대시보드 · 점검 스크립트가 이 키를 본다. */
    @Bean
    HealthIndicator cdcHealthIndicator(ObjectProvider<CdcSource> sources, HotdbCdcProperties props) {
        CdcSource source = sources.getIfAvailable(() -> {
            throw new IllegalStateException("hotdb.cdc.adapter=" + props.adapter() + " 에 맞는 CdcSource 어댑터가 없습니다");
        });
        return () -> {
            CdcSource.Status s = source.status();
            Health.Builder b = s.healthy() ? Health.up() : Health.down();
            b.withDetail("state", s.state()).withDetail("pipeline", s.pipeline()).withDetail("slot", s.position());
            Instant last = s.lastBatchAt();
            if (last != null) {
                b.withDetail("lastBatchAt", last.toString());
            }
            if (s.failure() != null) {
                b.withDetail("failure", s.failure());
            }
            return b.build();
        };
    }
}
