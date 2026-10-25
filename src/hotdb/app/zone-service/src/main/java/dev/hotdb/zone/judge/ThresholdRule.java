package dev.hotdb.zone.judge;

/**
 * 기본 규칙 — 정합도가 임계 이상이면 판정에 쓰고(진척률이 임계 이상이면 DONE, 아니면 IN_PROGRESS),
 * 정합도가 모자라거나 값이 없으면(장비가 안 보냄) 근거가 없으므로 LOW_CONFIDENCE.
 */
public class ThresholdRule implements JudgementRule {

    // 원천 컬럼이 real(float4)이라 float 로 비교한다 — double 0.9 와 비교하면 장비가 보낸 0.9(0.8999999…)가 반려된다
    private final float minProgressRate;
    private final float minMatchConfidence;

    public ThresholdRule(JudgementProperties.Threshold t) {
        this.minProgressRate = (float) t.minProgressRate();
        this.minMatchConfidence = (float) t.minMatchConfidence();
    }

    @Override
    public Verdict judge(Detection d) {
        if (d.progressEst() == null || d.matchConf() == null || d.matchConf() < minMatchConfidence) {
            return Verdict.LOW_CONFIDENCE;
        }
        return d.progressEst() >= minProgressRate ? Verdict.DONE : Verdict.IN_PROGRESS;
    }

    @Override
    public String name() {
        return "threshold(progress>=" + minProgressRate + ",confidence>=" + minMatchConfidence + ")";
    }
}
