package dev.hotdb.zone.judge;

/**
 * 기본 규칙 — 블록 진척률과 정합도가 둘 다 임계 이상이면 CONFIRMED, 아니면 REJECTED.
 * 값이 없으면(장비가 안 보냄) 근거가 없으므로 REJECTED.
 */
public class ThresholdRule implements JudgementRule {

    // 컬럼이 real(float4)이라 float 로 비교한다 — double 0.9 와 비교하면 장비가 보낸 0.9(0.8999999…)가 반려된다
    private final float minProgressRate;
    private final float minMatchConfidence;

    public ThresholdRule(JudgementProperties.Threshold t) {
        this.minProgressRate = (float) t.minProgressRate();
        this.minMatchConfidence = (float) t.minMatchConfidence();
    }

    @Override
    public String judge(ActualResult r) {
        if (r.blockProgressRate() == null || r.matchConfidence() == null) {
            return REJECTED;
        }
        return r.blockProgressRate() >= minProgressRate && r.matchConfidence() >= minMatchConfidence
                ? CONFIRMED : REJECTED;
    }

    @Override
    public String name() {
        return "threshold(progress>=" + minProgressRate + ",confidence>=" + minMatchConfidence + ")";
    }
}
