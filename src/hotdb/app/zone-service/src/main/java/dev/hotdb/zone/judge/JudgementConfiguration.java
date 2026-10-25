package dev.hotdb.zone.judge;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 판별 단계 둘.
 * <ul>
 *   <li>센서 판별 {@link JudgementWorker} — 모듈 규칙 빈({@code @Profile("<ZONE>")})이 있으면 그것, 없으면 임계값 규칙
 *   <li>절점 판별 {@link CheckpointWorker} — {@code hotdb.judge.checkpoint.enabled} (가공 모듈이 켠다)
 *   <li>레거시 실적 판별 {@link LegacyProgressWorker} — {@code hotdb.judge.legacy.enabled} (도장 · 의장 모듈이 켠다)
 * </ul>
 */
@Configuration
@EnableConfigurationProperties(JudgementProperties.class)
class JudgementConfiguration {

    @Bean
    @ConditionalOnProperty(name = "hotdb.judge.enabled", havingValue = "true", matchIfMissing = true)
    JudgementWorker judgementWorker(@Value("${hotdb.zone.module}") String module, ObjectProvider<JudgementRule> rules,
                                    JudgementProperties props, JdbcTemplate jdbc, TransactionTemplate tx,
                                    MeterRegistry meters) {
        // 한 모듈에 규칙 빈이 둘이면 여기서 기동이 멈춘다 — 어느 쪽으로 판정했는지 모르는 것보다 낫다
        JudgementRule rule = rules.getIfAvailable(() -> new ThresholdRule(props.threshold()));
        return new JudgementWorker(module, rule, props, jdbc, tx, meters);
    }

    @Bean
    @ConditionalOnProperty(name = "hotdb.judge.checkpoint.enabled", havingValue = "true")
    CheckpointWorker checkpointWorker(@Value("${hotdb.zone.module}") String module, JudgementProperties props,
                                      JdbcTemplate jdbc, TransactionTemplate tx, MeterRegistry meters) {
        return new CheckpointWorker(module, props.checkpoint(), jdbc, tx, meters);
    }

    @Bean
    @ConditionalOnProperty(name = "hotdb.judge.legacy.enabled", havingValue = "true")
    LegacyProgressWorker legacyProgressWorker(@Value("${hotdb.zone.module}") String module, JudgementProperties props,
                                              JdbcTemplate jdbc, TransactionTemplate tx, MeterRegistry meters) {
        return new LegacyProgressWorker(module, props.legacy(), jdbc, tx, meters);
    }
}
