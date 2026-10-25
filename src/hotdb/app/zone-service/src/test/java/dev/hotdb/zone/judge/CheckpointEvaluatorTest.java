package dev.hotdb.zone.judge;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hotdb.zone.judge.CheckpointEvaluator.Checkpoint;
import dev.hotdb.zone.judge.CheckpointEvaluator.PartStat;
import dev.hotdb.zone.judge.CheckpointEvaluator.Reached;
import dev.hotdb.zone.judge.CheckpointEvaluator.Result;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 가공 절점 10개 (2026-10-23 단계 정의) — 분모 세 구간 · 사외 도면 · 긴급 불출 · 순서 위반. DB 없이 돈다. */
class CheckpointEvaluatorTest {

    // V17 m_code CHECKPOINT 의 가공 절점 (사상 GRIND 는 in_scope = false 라 빠진다)
    static final List<Checkpoint> CUT = List.of(
            new Checkpoint("STEEL_IN", 1, "MAT", "STOCK", false, false, "YARD"),
            new Checkpoint("PICK1", 2, "MAT", "STOCK", false, true, "YARD"),
            new Checkpoint("PICK2", 3, "MAT", "STOCK", false, true, "YARD"),
            new Checkpoint("STEEL_ISSUE", 4, "MAT", "STOCK", false, false, "LINE"),
            new Checkpoint("PRETREAT", 5, "DWG", "CUT", false, false, "LINE"),
            new Checkpoint("MARK_DONE", 6, "DWG", "CUT", false, false),
            new Checkpoint("CUT_DONE", 7, "DWG", "CUT", true, false),
            new Checkpoint("BEND_DONE", 8, "PART", "CUT", false, false),
            new Checkpoint("GROUP", 9, "PART", "CUT", false, false),
            new Checkpoint("PART_ISSUE", 10, "PART", "CUT", false, false));

    static final Instant T = Instant.parse("2026-10-23T01:00:00Z");

    static Reached at(String source, String conf) {
        return new Reached(T, source, conf);
    }

    @Test
    void 아무_절점도_없으면_notDue() {
        Result r = CheckpointEvaluator.evaluate(CUT, Map.of(), Map.of(), false, false);
        assertThat(r.state()).isEqualTo("notDue");
        assertThat(r.lastCp()).isNull();
        assertThat(r.autoPct()).isZero();
        assertThat(r.stats()).containsKeys("STEEL_IN", "PART_ISSUE");
    }

    @Test
    void 절점을_순서대로_닿으면_마지막_절점과_절점_기준_진척() {
        Result r = CheckpointEvaluator.evaluate(CUT, Map.of(
                "STEEL_IN", at("SSY", "MID"), "PICK1", at("SSY", "MID"), "PICK2", at("SSY", "MID"),
                "STEEL_ISSUE", at("140M", "HIGH"), "PRETREAT", at("140M", "HIGH")), Map.of(), false, false);
        assertThat(r.state()).isEqualTo("inProgress");
        assertThat(r.lastCp()).isEqualTo("PRETREAT");
        // 해당되는 절점 = MAT 4 + DWG 3 = 7 (PART 3 은 대상 부재가 없어 분모 밖) · 닿은 것 5
        assertThat(r.autoPct()).isEqualTo(71.4);
        assertThat(r.history()).extracting(h -> h.get("cp")).containsExactly("STEEL_IN", "PICK1", "PICK2", "STEEL_ISSUE", "PRETREAT");
        assertThat(r.reversed()).isEmpty();
    }

    @Test
    void 절단_완료에_닿으면_done_이고_100() {
        Map<String, Reached> reached = Map.of("STEEL_IN", at("SSY", "MID"), "PICK1", at("SSY", "MID"), "PICK2", at("SSY", "MID"),
                "STEEL_ISSUE", at("140M", "HIGH"), "PRETREAT", at("140M", "HIGH"), "MARK_DONE", at("310M", "LOW"),
                "CUT_DONE", at("WO_CT", "HIGH"));
        Result r = CheckpointEvaluator.evaluate(CUT, reached, Map.of(), false, false);
        assertThat(r.state()).isEqualTo("done");
        assertThat(r.autoPct()).isEqualTo(100.0);
        assertThat(r.lastCp()).isEqualTo("CUT_DONE");
    }

    @Test
    void 사외_도면은_적치_절점이_해당_없음이라_분모에서_빠진다() {
        Result r = CheckpointEvaluator.evaluate(CUT, Map.of("PRETREAT", at("140M", "HIGH"), "MARK_DONE", at("310M", "HIGH")),
                Map.of(), true, false);
        assertThat(r.state()).isEqualTo("inProgress");
        assertThat(r.lastCp()).isEqualTo("MARK_DONE");
        assertThat(r.reversed()).as("적치 절점이 비어도 순서 위반이 아니다").isEmpty();
        assertThat(((Map<?, ?>) r.stats().get("STEEL_IN")).get("na")).isEqualTo(true);
        // 해당되는 절점 = DWG 3 (적치 4 는 사외라 밖, PART 3 은 대상 없음), 닿은 것 2
        assertThat(r.autoPct()).isEqualTo(66.7);
    }

    @Test
    void 긴급_불출은_선별_절점을_건너뛴_정상_경로() {
        Result r = CheckpointEvaluator.evaluate(CUT, Map.of("STEEL_IN", at("SSY", "MID"), "STEEL_ISSUE", at("140M", "HIGH")),
                Map.of(), false, true);
        assertThat(r.lastCp()).isEqualTo("STEEL_ISSUE");
        assertThat(r.reversed()).isEmpty();
        // 긴급이 아니면 같은 자료가 순서 위반이다
        Result normal = CheckpointEvaluator.evaluate(CUT, Map.of("STEEL_IN", at("SSY", "MID"), "STEEL_ISSUE", at("140M", "HIGH")),
                Map.of(), false, false);
        assertThat(normal.lastCp()).isEqualTo("STEEL_IN");
        assertThat(normal.reversed()).containsExactly("STEEL_ISSUE");
    }

    @Test
    void 부재_절점은_대상_부재가_전부_끝나야_닿고_대상이_없으면_분모_밖() {
        Map<String, Reached> toCut = Map.of("STEEL_IN", at("SSY", "MID"), "PICK1", at("SSY", "MID"), "PICK2", at("SSY", "MID"),
                "STEEL_ISSUE", at("140M", "HIGH"), "PRETREAT", at("140M", "HIGH"), "MARK_DONE", at("310M", "HIGH"),
                "CUT_DONE", at("WO_CT", "HIGH"));
        Result partial = CheckpointEvaluator.evaluate(CUT, toCut,
                Map.of("BEND_DONE", new PartStat(0, 0), "GROUP", new PartStat(3, 5), "PART_ISSUE", new PartStat(0, 5)), false, false);
        assertThat(partial.state()).isEqualTo("done");
        assertThat(partial.lastCp()).as("곡가공은 대상 없음(na) · 모둠은 3/5 라 미도달").isEqualTo("CUT_DONE");
        Map<?, ?> group = (Map<?, ?>) partial.stats().get("GROUP");
        assertThat(group.get("done")).isEqualTo(3);
        assertThat(group.get("target")).isEqualTo(5);
        assertThat(((Map<?, ?>) partial.stats().get("BEND_DONE")).get("na")).isEqualTo(true);

        Result all = CheckpointEvaluator.evaluate(CUT, toCut,
                Map.of("BEND_DONE", new PartStat(2, 2), "GROUP", new PartStat(5, 5), "PART_ISSUE", new PartStat(5, 5)), false, false);
        assertThat(all.lastCp()).isEqualTo("PART_ISSUE");
        assertThat(all.history()).hasSize(10);
    }

    @Test
    void 앞_절점이_비었는데_뒤_절점이_찍히면_순서_위반() {
        Result r = CheckpointEvaluator.evaluate(CUT, Map.of("STEEL_IN", at("SSY", "MID"), "CUT_DONE", at("310M", "LOW")),
                Map.of(), false, false);
        assertThat(r.lastCp()).isEqualTo("STEEL_IN");
        assertThat(r.reversed()).containsExactly("CUT_DONE");
        assertThat(r.state()).as("done 절점에 닿았으니 done — 순서 위반은 알림으로 따로").isEqualTo("done");
        assertThat(r.history()).extracting(h -> h.get("conf")).containsExactly("MID", "LOW");
    }

    @Test
    void 기록_묶음에_기록이_하나도_없으면_해당_없음이고_순서_위반이_아니다() {
        // 야드(ProSSYS) · 라인(140M) 기록이 모두 없는 옛 도면 — 마킹 · 절단만 있다
        Result r = CheckpointEvaluator.evaluate(CUT, Map.of("MARK_DONE", at("310M", "HIGH"), "CUT_DONE", at("WO_CT", "HIGH")),
                Map.of(), false, false);
        assertThat(r.reversed()).as("원천에 기록이 없는 것 — 뒤집힌 것이 아니다").isEmpty();
        assertThat(r.lastCp()).isEqualTo("CUT_DONE");
        assertThat(r.stats()).containsEntry("no_record", List.of("YARD", "LINE"));
        assertThat(((Map<?, ?>) r.stats().get("PRETREAT")).get("na")).isEqualTo(true);
        // 라인 기록(불출)은 있는데 전처리가 비면 — 겹장 N 같은 실제 빈칸이라 순서 위반
        Result line = CheckpointEvaluator.evaluate(CUT, Map.of("STEEL_ISSUE", at("140M", "HIGH"), "MARK_DONE", at("310M", "HIGH")),
                Map.of(), false, false);
        assertThat(line.reversed()).containsExactly("MARK_DONE");
        assertThat(line.lastCp()).isEqualTo("STEEL_ISSUE");
        assertThat(line.stats()).containsEntry("no_record", List.of("YARD"));
        // 야드 기록이 하나라도 있으면 빈 선별은 순서 위반 (긴급 아님)
        Result yard = CheckpointEvaluator.evaluate(CUT, Map.of("STEEL_IN", at("SSY", "MID"), "STEEL_ISSUE", at("140M", "HIGH"),
                "PRETREAT", at("140M", "HIGH")), Map.of(), false, false);
        assertThat(yard.reversed()).containsExactly("STEEL_ISSUE", "PRETREAT");
        assertThat(yard.stats()).doesNotContainKey("no_record");
    }
}
