package dev.hotdb.zone.judge;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 실적 판별 단계 ({@code hotdb.judge.*}).
 *
 * @param enabled    false 면 센서 판정을 하지 않는다 — 인식 객체는 judged = false 로만 남는다
 * @param intervalMs 훑는 간격
 * @param batchSize  한 번에 판정하는 최대 건수 (한 트랜잭션)
 * @param threshold  모듈 규칙 빈이 없을 때 쓰는 기본 규칙의 임계값
 * @param checkpoint 절점 판별 (가공) — l_event · l_cut_part 로 p_progress 를 쓴다
 * @param legacy     레거시 실적 판별 (도장 · 의장) — l_wo_daily · l_paint_element · l_outfit_item 으로 p_progress 를 쓴다
 */
@ConfigurationProperties("hotdb.judge")
public record JudgementProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("1000") long intervalMs,
        @DefaultValue("200") int batchSize,
        @DefaultValue Threshold threshold,
        @DefaultValue Checkpoint checkpoint,
        @DefaultValue Legacy legacy) {

    /** @param minProgressRate 진척률(0~100) · @param minMatchConfidence 정합도(0~1) */
    public record Threshold(@DefaultValue("100") double minProgressRate,
                            @DefaultValue("0.9") double minMatchConfidence) {}

    /**
     * @param enabled    절점 판별을 돌릴지 (application-mch.yml 이 켠다)
     * @param intervalMs 훑는 간격
     * @param batchSize  한 주기에 다시 판정하는 앵커 수
     */
    public record Checkpoint(@DefaultValue("false") boolean enabled,
                             @DefaultValue("30000") long intervalMs,
                             @DefaultValue("500") int batchSize) {}

    /**
     * @param enabled        레거시 실적 판별을 돌릴지 (application-pnt.yml · application-oft.yml 이 켠다)
     * @param intervalMs     훑는 간격
     * @param batchSize      한 주기에 다시 판정하는 앵커 수
     * @param refreshMinutes 오늘 판정이 있어도 이만큼 지나면 다시 본다 (레거시 일별 실적이 하루 중 쌓인다)
     */
    public record Legacy(@DefaultValue("false") boolean enabled,
                         @DefaultValue("60000") long intervalMs,
                         @DefaultValue("500") int batchSize,
                         @DefaultValue("10") int refreshMinutes) {}
}
