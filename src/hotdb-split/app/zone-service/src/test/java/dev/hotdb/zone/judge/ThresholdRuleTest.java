package dev.hotdb.zone.judge;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** 기본 임계값 규칙. DB 없이 돈다. */
class ThresholdRuleTest {

    final ThresholdRule rule = new ThresholdRule(new JudgementProperties.Threshold(100, 0.9));

    @Test
    void 진척률과_정합도가_둘_다_임계_이상이면_확정() {
        assertThat(rule.judge(result(100f, 0.9f))).isEqualTo(JudgementRule.CONFIRMED);
        assertThat(rule.judge(result(100f, 0.99f))).isEqualTo(JudgementRule.CONFIRMED);
    }

    @Test
    void 하나라도_모자라면_반려() {
        assertThat(rule.judge(result(99.9f, 0.99f))).isEqualTo(JudgementRule.REJECTED);
        assertThat(rule.judge(result(100f, 0.89f))).isEqualTo(JudgementRule.REJECTED);
    }

    @Test
    void 값이_없으면_근거가_없어_반려() {
        assertThat(rule.judge(result(null, 0.99f))).isEqualTo(JudgementRule.REJECTED);
        assertThat(rule.judge(result(100f, null))).isEqualTo(JudgementRule.REJECTED);
    }

    static ActualResult result(Float progress, Float confidence) {
        return new ActualResult("asm", "H1", "B1", UUID.randomUUID(), "D1", Instant.now(), progress, confidence,
                null, null, Instant.now());
    }
}
