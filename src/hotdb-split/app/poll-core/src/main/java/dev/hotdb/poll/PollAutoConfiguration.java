package dev.hotdb.poll;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
        DataSource targetDs = props.target() == null || props.target().url() == null
                ? primary : pool("hotdb-poll-target", props.target(), 4, false);
        JdbcTemplate target = new JdbcTemplate(targetDs);
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(targetDs));

        Map<String, DataSource> sources = new LinkedHashMap<>();
        props.sources().forEach((name, conn) -> sources.put(name, pool("poll-src-" + name, conn, 2, true)));

        List<String> errors = new ArrayList<>();
        List<PollJob> jobs = new ArrayList<>();
        for (PollProperties.JobSpec spec : props.jobs()) {
            if (!spec.enabled()) {
                continue;
            }
            try {
                DataSource src = sources.get(spec.source());
                if (src == null) {
                    throw new IllegalStateException("작업 " + spec.jobName() + ": 원천 '" + spec.source() + "' 가 hotdb.poll.sources 에 없습니다");
                }
                jobs.add(new PollJob(props.agent(), spec, src, target, tx,
                        PollJob.targetColumns(target, spec.target()), PollJob.targetPk(target, spec.target()),
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
        return new PollRunner(jobs, props);
    }

    @Bean
    HealthIndicator pollHealthIndicator(PollRunner runner) {
        return () -> Health.up().withDetail("jobs", runner.jobs().stream().map(PollJob::name).toList()).build();
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
        if (lazy) {
            cfg.setInitializationFailTimeout(-1);
        }
        return new HikariDataSource(cfg);
    }

    /** 작업마다 고정 지연으로 돈다. 앞 주기가 길어지면 다음 주기는 그만큼 늦게 시작한다 (겹치지 않음). */
    public static class PollRunner implements SmartLifecycle {
        private final List<PollJob> jobs;
        private final PollProperties props;
        private ScheduledExecutorService scheduler;

        PollRunner(List<PollJob> jobs, PollProperties props) {
            this.jobs = jobs;
            this.props = props;
        }

        public List<PollJob> jobs() {
            return jobs;
        }

        @Override
        public void start() {
            scheduler = Executors.newScheduledThreadPool(Math.max(1, props.threads()), r -> new Thread(r, "poll"));
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
