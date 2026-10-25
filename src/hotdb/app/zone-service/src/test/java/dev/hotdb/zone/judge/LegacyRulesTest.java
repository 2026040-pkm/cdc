package dev.hotdb.zone.judge;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hotdb.zone.judge.CheckpointEvaluator.Reached;
import dev.hotdb.zone.judge.LegacyRules.Match;
import dev.hotdb.zone.judge.LegacyRules.Wo;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 레거시를 끼는 판별 규칙 — W/O 매칭 · 도장 실적률 · 의장 품목 상태 · 차이 알림 · 가공 계획 절점. DB 없이 돈다. */
class LegacyRulesTest {

    static final LocalDate D = LocalDate.of(2026, 10, 24);
    static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");

    // ── 조립 W/O 매칭 ──
    @Test
    void 하루치_W_O_가_있으면_DAY_취부_F_를_먼저() {
        List<Wo> wos = List.of(
                new Wo("2378154A0001S0W01", D.minusDays(2), D.plusDays(2)),
                new Wo("2378154A0001S0F01", D.minusDays(1), D.plusDays(1)),
                new Wo("2378154A0001S0X01", D.plusDays(10), D.plusDays(12)));
        Match m = LegacyRules.matchWo(D, D.minusDays(2), D.plusDays(12), wos, 28);
        assertThat(m).isEqualTo(new Match("DAY", "2378154A0001S0F01", "ON_TIME", "MATCHED"));
    }

    @Test
    void 하루치가_없고_앵커_계획이_4주_안이면_WEEK4_가까운_W_O() {
        List<Wo> wos = List.of(new Wo("W1", D.plusDays(5), D.plusDays(7)), new Wo("W2", D.plusDays(20), D.plusDays(22)));
        Match m = LegacyRules.matchWo(D, D.plusDays(5), D.plusDays(22), wos, 28);
        assertThat(m).isEqualTo(new Match("WEEK4", "W1", "EARLY", "MATCHED"));
    }

    @Test
    void 계획이_4주_밖이면_불일치_LATE() {
        List<Wo> wos = List.of(new Wo("W1", D.minusDays(60), D.minusDays(50)));
        Match m = LegacyRules.matchWo(D, D.minusDays(60), D.minusDays(50), wos, 28);
        assertThat(m).isEqualTo(new Match("NONE", "W1", "LATE", "MISMATCH"));
    }

    @Test
    void 하위_W_O_가_없어도_앵커_계획으로_판정() {
        assertThat(LegacyRules.matchWo(D, null, null, List.of(), 28).state()).isEqualTo("MISMATCH");
        assertThat(LegacyRules.matchWo(D, D.minusDays(3), D.plusDays(3), List.of(), 28))
                .isEqualTo(new Match("WEEK4", null, "ON_TIME", "MATCHED"));
    }

    // ── 도장 블록 실적률 = Σ413 ÷ Σ212 ──
    @Test
    void 도장_실적률_예시_블록_2378_154() {
        // ship-brain wiki 예시: Σ212 ACTY_MH_1 4002 · Σ413 DLY_PRGS_MH 3331 → 83.2%
        assertThat(LegacyRules.paintPct(3331, 4002.0)).isEqualTo(83.2);
        assertThat(LegacyRules.paintPct(5000, 4002.0)).isEqualTo(100.0);
        assertThat(LegacyRules.paintPct(10, null)).isNull();
        assertThat(LegacyRules.paintPct(10, 0.0)).isNull();
    }

    // ── 의장 품목 상태 ──
    @Test
    void 의장_품목_상태() {
        assertThat(LegacyRules.itemState(4.0, 4.0, 0.0, 0.0, true)).isEqualTo("INSTALLED");
        assertThat(LegacyRules.itemState(4.0, 1.0, 2.0, 0.0, true)).isEqualTo("FIXED");
        assertThat(LegacyRules.itemState(4.0, 1.0, 0.0, 0.0, true)).isEqualTo("TEMP");
        assertThat(LegacyRules.itemState(4.0, 0.0, 0.0, 1.0, false)).isEqualTo("TEMP");
        assertThat(LegacyRules.itemState(4.0, 0.0, 0.0, 0.0, true)).isEqualTo("ISSUED");
        assertThat(LegacyRules.itemState(4.0, null, null, null, false)).isEqualTo("WAITING");
        assertThat(LegacyRules.itemState(0.0, 0.0, 0.0, 0.0, false)).isEqualTo("NA");
    }

    @Test
    void 상태와_차이_알림() {
        assertThat(LegacyRules.state(100.0, false, 100)).isEqualTo("done");
        assertThat(LegacyRules.state(40.0, true, 100)).isEqualTo("done");
        assertThat(LegacyRules.state(40.0, false, 100)).isEqualTo("inProgress");
        assertThat(LegacyRules.state(null, false, 100)).isEqualTo("notDue");
        assertThat(LegacyRules.gapAlert(80.0, 69.0, 10)).isTrue();
        assertThat(LegacyRules.gapAlert(80.0, 70.0, 10)).isFalse();
        assertThat(LegacyRules.gapAlert(null, 70.0, 10)).isFalse();
        assertThat(LegacyRules.pct(1, 3)).isEqualTo(33.3);
    }

    // ── 가공 계획 절점 · 반입예정 경과 ──
    @Test
    void 가공_계획일_대비_늦은_절점() {
        LocalDate sd = D.minusDays(5);
        LocalDate fd = D.minusDays(3);
        Instant onSd = sd.atStartOfDay(SEOUL).plusHours(9).toInstant();
        Instant afterFd = fd.plusDays(2).atStartOfDay(SEOUL).plusHours(9).toInstant();
        // 마킹은 절단계획일에 · 절단 완료는 아직 (완료 계획일 지남) → 늦은 절점 1
        CheckpointEvaluator.Plan p = CheckpointEvaluator.plan(sd, fd, Map.of("MARK_DONE", new Reached(onSd, "310M", "HIGH")), D, SEOUL);
        assertThat(p.plan()).containsEntry("MARK_DONE", sd.toString()).containsEntry("CUT_DONE", fd.toString());
        assertThat(p.late()).isEqualTo(1);
        // 절단 완료가 완료 계획일 안 → 0 (절단계획일보다는 늦어도)
        Instant onFd = fd.atStartOfDay(SEOUL).plusHours(9).toInstant();
        assertThat(CheckpointEvaluator.plan(sd, fd, Map.of("MARK_DONE", new Reached(onSd, "310M", "HIGH"),
                "CUT_DONE", new Reached(onFd, "WO_CT", "HIGH")), D, SEOUL).late()).isZero();
        // 둘 다 계획일 뒤 → 2
        assertThat(CheckpointEvaluator.plan(sd, fd, Map.of("MARK_DONE", new Reached(afterFd, "310M", "HIGH"),
                "CUT_DONE", new Reached(afterFd, "WO_CT", "HIGH")), D, SEOUL).late()).isEqualTo(2);
        // 완료 계획일이 없으면 절단계획일로, 계획일이 아직이면 0, 계획이 없으면 빈 값
        assertThat(CheckpointEvaluator.plan(sd, null, Map.of(), D, SEOUL).plan()).containsEntry("CUT_DONE", sd.toString());
        assertThat(CheckpointEvaluator.plan(D.plusDays(5), D.plusDays(6), Map.of(), D, SEOUL).late()).isZero();
        assertThat(CheckpointEvaluator.plan(null, null, Map.of(), D, SEOUL).plan()).isEmpty();
    }

    @Test
    void 반입예정_경과() {
        Instant now = Instant.parse("2026-10-24T00:00:00Z");
        Instant due = now.minusSeconds(86_400);
        assertThat(CheckpointEvaluator.carryOverdue(due, false, false, now)).isTrue();
        assertThat(CheckpointEvaluator.carryOverdue(due, true, false, now)).isFalse();
        assertThat(CheckpointEvaluator.carryOverdue(due, false, true, now)).isFalse();
        assertThat(CheckpointEvaluator.carryOverdue(now.plusSeconds(60), false, false, now)).isFalse();
        assertThat(CheckpointEvaluator.carryOverdue(null, false, false, now)).isFalse();
    }
}
