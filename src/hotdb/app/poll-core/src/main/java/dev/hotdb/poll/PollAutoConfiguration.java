package dev.hotdb.poll;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.hotdb.cdc.resource.WorkThreads;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * poll-core 를 의존성에 넣고 {@code hotdb.poll.agent} 를 주면 켜진다 — RFC Service · DB Agent 가 같이 쓴다.
 */
@AutoConfiguration(afterName = "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration")
@ConditionalOnProperty("hotdb.poll.agent")
@EnableConfigurationProperties(PollProperties.class)
public class PollAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(PollAutoConfiguration.class);

    /** 폴링 작업 묶음. 기동 때 대상 표와 키를 대조한다 — 어긋나면 기동을 거부한다. */
    @Bean
    PollRunner pollRunner(PollProperties props, DataSource primary, MeterRegistry meters) {
        // 기본 대상 하나 + 이름 붙인 대상들. 작업은 to 로 고른다 (없으면 기본)
        DataSource defaultDs = props.target() == null || props.target().url() == null
                ? primary : pool("hotdb-poll-target", props.target(), 4, true);
        Map<String, Target> targets = new LinkedHashMap<>();
        targets.put("", Target.of(defaultDs));
        props.targets().forEach((name, conn) -> targets.put(name, Target.of(pool("poll-target-" + name, conn, 4, true))));
        // 기동 때 닿지 않는 대상 DB 는 그 대상의 작업만 빼고 뜬다 — 레거시 서버가 내려가 있어도 Hot DB 적재 · SAP 송신은 돈다.
        // 닿는데 표 · 키가 어긋난 것은 그대로 기동 거부다. 빠진 작업은 재기동하면 다시 붙는다
        Map<String, String> unreachable = new LinkedHashMap<>();
        targets.forEach((name, t) -> {
            try {
                t.jdbc().queryForObject("SELECT 1", Integer.class);
            } catch (org.springframework.dao.DataAccessException e) {
                unreachable.put(name, rootMessage(e));
            }
        });
        unreachable.forEach((name, why) -> log.warn("폴링 대상 '{}' 에 닿지 않아 그 대상 작업을 뺀다 (재기동하면 다시 붙는다): {}",
                name.isEmpty() ? "기본" : name, why));

        Map<String, DataSource> sources = new LinkedHashMap<>();
        props.sources().forEach((name, conn) -> sources.put(name, pool("poll-src-" + name, conn, Math.max(2, props.threads()), true)));

        List<String> errors = new ArrayList<>();
        List<PollJob> jobs = new ArrayList<>();
        int skipped = 0;
        for (PollProperties.JobSpec spec : props.jobs()) {
            if (!spec.enabled()) {
                continue;
            }
            try {
                DataSource src = sources.get(spec.source());
                if (src == null) {
                    throw new IllegalStateException("작업 " + spec.jobName() + ": 원천 '" + spec.source() + "' 가 hotdb.poll.sources 에 없습니다");
                }
                String to = spec.to() == null ? "" : spec.to();
                Target t = targets.get(to);
                if (t == null) {
                    throw new IllegalStateException("작업 " + spec.jobName() + ": 대상 '" + spec.to() + "' 가 hotdb.poll.targets 에 없습니다");
                }
                if (unreachable.containsKey(to)) {
                    skipped++;
                    continue;
                }
                jobs.add(new PollJob(props.agent(), spec, src, t.jdbc(), t.tx(),
                        PollJob.targetColumns(t.jdbc(), spec.target()), PollJob.targetPk(t.jdbc(), spec.target()),
                        props.fetchSize(), props.batchSize(), meters));
            } catch (RuntimeException e) {
                errors.add(e.getMessage());
            }
        }
        if (!errors.isEmpty()) {
            throw new IllegalStateException("폴링 설정이 HotDB 와 맞지 않습니다 (" + errors.size() + "건)\n  - "
                    + String.join("\n  - ", errors));
        }
        jobs.forEach(j -> log.info("폴링 작업 {} (주기 {}ms)", j.name(), j.intervalMs(props.defaultIntervalMs())));
        if (skipped > 0) {
            log.warn("닿지 않는 대상 때문에 뺀 폴링 작업 {}개", skipped);
        }
        return new PollRunner(jobs, props, meters);
    }

    /** 대상 DB 하나 — 쓰기 접속과 그 트랜잭션. ops.poll_state 도 이 DB 에 있다. */
    record Target(JdbcTemplate jdbc, TransactionTemplate tx) {
        static Target of(DataSource ds) {
            return new Target(new JdbcTemplate(ds), new TransactionTemplate(new DataSourceTransactionManager(ds)));
        }
    }

    @Bean
    HealthIndicator pollHealthIndicator(PollRunner runner) {
        return () -> Health.up().withDetail("jobs", runner.jobs().stream().map(PollJob::name).toList()).build();
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getMessage();
    }

    // 원천은 읽기 전용 · 작은 풀. 원천이 기동 때 죽어 있어도 서비스는 뜬다 (첫 접속을 미룬다)
    private static DataSource pool(String name, PollProperties.Conn c, int size, boolean lazy) {
        HikariConfig cfg = new HikariConfig();
        cfg.setPoolName(name);
        cfg.setJdbcUrl(c.url());
        cfg.setUsername(c.user());
        cfg.setPassword(c.password());
        cfg.setMaximumPoolSize(size);
        cfg.setMinimumIdle(0);
        cfg.setConnectionTimeout(10_000);   // 닿지 않는 서버를 기동 때 30초씩 기다리지 않게
        if (lazy) {
            cfg.setInitializationFailTimeout(-1);
        }
        return new HikariDataSource(cfg);
    }

    /** 작업마다 고정 지연으로 돈다. 앞 주기가 길어지면 다음 주기는 그만큼 늦게 시작한다 (겹치지 않음). */
    public static class PollRunner implements SmartLifecycle {
        private final List<PollJob> jobs;
        private final PollProperties props;
        private final MeterRegistry meters;
        private ScheduledExecutorService scheduler;

        PollRunner(List<PollJob> jobs, PollProperties props, MeterRegistry meters) {
            this.jobs = jobs;
            this.props = props;
            this.meters = meters;
        }

        public List<PollJob> jobs() {
            return jobs;
        }

        @Override
        public void start() {
            // part=poll 로 따로 센다 — 같은 프로세스에 CDC 엔진이 있으면(RFC Service) 둘이 자원을 나눠 쓰는 모양이 보인다
            scheduler = Executors.newScheduledThreadPool(Math.max(1, props.threads()),
                    WorkThreads.factory("poll", "poll", meters));
            long stagger = 0;
            for (PollJob j : jobs) {
                scheduler.scheduleWithFixedDelay(j::runOnce, stagger, j.intervalMs(props.defaultIntervalMs()),
                        TimeUnit.MILLISECONDS);
                stagger += 500;
            }
        }

        @Override
        public void stop() {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        }

        @Override
        public boolean isRunning() {
            return scheduler != null && !scheduler.isShutdown();
        }
    }
}
