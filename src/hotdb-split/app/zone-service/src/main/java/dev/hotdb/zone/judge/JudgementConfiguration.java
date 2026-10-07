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

/** 판별 단계. 모듈 규칙 빈({@code @Profile("<ZONE>")})이 있으면 그것, 없으면 임계값 규칙. */
@Configuration
@EnableConfigurationProperties(JudgementProperties.class)
@ConditionalOnProperty(name = "hotdb.judge.enabled", havingValue = "true", matchIfMissing = true)
class JudgementConfiguration {

    @Bean
    JudgementWorker judgementWorker(@Value("${hotdb.zone.module}") String module, @Value("${hotdb.zone.schema}") String schema,
                                    ObjectProvider<JudgementRule> rules,
                                    JudgementProperties props, JdbcTemplate jdbc, TransactionTemplate tx,
                                    MeterRegistry meters) {
        // 한 모듈에 규칙 빈이 둘이면 여기서 기동이 멈춘다 — 어느 쪽으로 판정했는지 모르는 것보다 낫다
        JudgementRule rule = rules.getIfAvailable(() -> new ThresholdRule(props.threshold()));
        return new JudgementWorker(module, schema, rule, props, jdbc, tx, meters);
    }
}
