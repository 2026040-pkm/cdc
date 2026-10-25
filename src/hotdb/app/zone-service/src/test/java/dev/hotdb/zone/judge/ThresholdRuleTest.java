package dev.hotdb.zone.judge;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hotdb.zone.judge.JudgementRule.Verdict;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** 기본 임계값 규칙. DB 없이 돈다. */
class ThresholdRuleTest {

    final ThresholdRule rule = new ThresholdRule(new JudgementProperties.Threshold(100, 0.9));

    @Test
    void 진척률과_정합도가_둘_다_임계_이상이면_DONE() {
        assertThat(rule.judge(detection(100f, 0.9f))).isEqualTo(Verdict.DONE);
        assertThat(rule.judge(detection(100f, 0.99f))).isEqualTo(Verdict.DONE);
    }

    @Test
    void 정합도는_넘는데_진척이_모자라면_IN_PROGRESS() {
        assertThat(rule.judge(detection(99.9f, 0.99f))).isEqualTo(Verdict.IN_PROGRESS);
        assertThat(rule.judge(detection(55f, 0.9f))).isEqualTo(Verdict.IN_PROGRESS);
    }

    @Test
    void 정합도가_모자라거나_값이_없으면_근거가_없어_LOW_CONFIDENCE() {
        assertThat(rule.judge(detection(100f, 0.89f))).isEqualTo(Verdict.LOW_CONFIDENCE);
        assertThat(rule.judge(detection(null, 0.99f))).isEqualTo(Verdict.LOW_CONFIDENCE);
        assertThat(rule.judge(detection(100f, null))).isEqualTo(Verdict.LOW_CONFIDENCE);
    }

    static Detection detection(Float progress, Float confidence) {
        return new Detection("scan-1", "B731", "5486", "731", "BLOCK", progress, confidence, Instant.now(), "COMPLETE");
    }
}
