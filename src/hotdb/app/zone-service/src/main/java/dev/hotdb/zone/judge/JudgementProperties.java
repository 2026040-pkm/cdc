package dev.hotdb.zone.judge;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 실적 판별 단계 ({@code hotdb.judge.*}).
 *
 * @param enabled    false 면 판정하지 않는다 — 실적은 PENDING 으로만 남는다 (판별 전 동작)
 * @param intervalMs 훑는 간격
 * @param batchSize  한 번에 판정하는 최대 건수 (한 트랜잭션)
 * @param threshold  모듈 규칙 빈이 없을 때 쓰는 기본 규칙의 임계값
 */
@ConfigurationProperties("hotdb.judge")
public record JudgementProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("1000") long intervalMs,
        @DefaultValue("200") int batchSize,
        @DefaultValue Threshold threshold) {

    /** @param minProgressRate 블록 진척률(0~100) · @param minMatchConfidence 정합도(0~1) */
    public record Threshold(@DefaultValue("100") double minProgressRate,
                            @DefaultValue("0.9") double minMatchConfidence) {}
}
