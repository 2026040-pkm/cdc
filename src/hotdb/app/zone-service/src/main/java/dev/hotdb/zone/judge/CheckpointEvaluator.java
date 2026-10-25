package dev.hotdb.zone.judge;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 절점 판정 — 앵커 하나의 절점 도달을 순서대로 읽어 마지막 절점 · 상태 · 절점 기준 진척 · 절점별 통계를 낸다. 순수 함수.
 *
 * <p>2026-10-23 가공 단계 정의를 그대로 따른다.
 * <ul>
 *   <li>분모는 절점마다 다르다 — basis MAT(자재, 1~4) · DWG(도면, 5~7) · PART(부재, 8~10). 세 구간의 퍼센트를 섞지 않는다
 *   <li>MAT · DWG 절점은 l_event 의 사건 시각으로 도달, PART 절점은 l_cut_part 의 대상 부재가 전부 끝나야 도달
 *   <li>사외 절단 도면(site OUT)은 적치 절점(phase STOCK)이 해당 없음 — 분모에서 뺀다
 *   <li>긴급 불출(E)은 선별 절점(skipIf EMERGENCY)을 건너뛴 정상 경로
 *   <li>절점은 순서대로만 — 앞 절점이 비었는데 뒤 절점이 찍히면 순서 위반으로 적는다 (알림 대상)
 *   <li>기록 묶음(attrs.record_group — YARD = ProSSYS 입고 · 선별, LINE = 140M 불출 · 전처리)에 이 도면의 기록이 하나도 없으면
 *       그 묶음의 절점은 해당 없음 — 그 원천에 기록이 없는 것이지 순서가 뒤집힌 것이 아니다. stats.no_record 에 묶음을 남긴다.
 *       묶음에 기록이 하나라도 있으면 빈 절점은 그대로 순서 위반
 *   <li>state: 아무 절점도 없으면 notDue, done 절점(절단 완료)에 닿으면 done, 그 사이는 inProgress
 *   <li>auto_pct: 절점 기준 진척 = 연속으로 닿은 절점 수 ÷ 해당되는 절점 수 (done 이면 100). 블록 롤업은 시수 가중이 따로 한다
 * </ul>
 */
public final class CheckpointEvaluator {

    private CheckpointEvaluator() {}

    /** m_code CHECKPOINT 한 행 (attrs 를 풀어서). */
    public record Checkpoint(String code, int seq, String basis, String phase, boolean done, boolean skipIfEmergency,
                             String recordGroup) {
        public Checkpoint(String code, int seq, String basis, String phase, boolean done, boolean skipIfEmergency) {
            this(code, seq, basis, phase, done, skipIfEmergency, null);
        }
    }

    /** l_event 로 닿은 절점 (MAT · DWG). */
    public record Reached(Instant ts, String source, String conf) {}

    /** l_cut_part 로 세는 절점 (PART) — 대상 부재 중 끝난 부재. */
    public record PartStat(int done, int target) {}

    /** 절점 하나의 결과. na = 해당 없음(분모 밖). */
    public record CpState(String code, String basis, boolean passed, boolean na, Instant ts, String source, String conf,
                          Integer done, Integer target) {}

    /**
     * @param lastCp     순서대로 연속으로 닿은 마지막 절점 (없으면 null)
     * @param state      notDue · inProgress · done
     * @param autoPct    절점 기준 진척 (0~100)
     * @param history    cp_history — 닿은 절점의 [{cp, ts, source, conf}]
     * @param stats      cp_stats — 절점마다 {basis, passed, na, done, target}
     * @param reversed   앞 절점이 비었는데 닿은 절점 (순서 위반)
     */
    public record Result(String lastCp, String state, double autoPct, List<Map<String, Object>> history,
                         Map<String, Object> stats, List<String> reversed) {}

    public static Result evaluate(List<Checkpoint> cps, Map<String, Reached> reached, Map<String, PartStat> parts,
                                  boolean siteOut, boolean emergency) {
        // 기록이 하나도 없는 묶음
        Set<String> noRecord = new LinkedHashSet<>();
        for (Checkpoint c : cps) {
            if (c.recordGroup() != null) {
                noRecord.add(c.recordGroup());
            }
        }
        for (Checkpoint c : cps) {
            if (c.recordGroup() != null && reached.containsKey(c.code())) {
                noRecord.remove(c.recordGroup());
            }
        }
        List<CpState> states = new ArrayList<>();
        for (Checkpoint cp : cps) {
            boolean na = (siteOut && "STOCK".equals(cp.phase())) || (emergency && cp.skipIfEmergency())
                    || (cp.recordGroup() != null && noRecord.contains(cp.recordGroup()));
            if ("PART".equals(cp.basis())) {
                PartStat p = parts.get(cp.code());
                int target = p == null ? 0 : p.target();
                int done = p == null ? 0 : p.done();
                states.add(new CpState(cp.code(), cp.basis(), target > 0 && done >= target, na || target == 0, null,
                        "l_cut_part", null, done, target));
            } else {
                Reached r = reached.get(cp.code());
                states.add(new CpState(cp.code(), cp.basis(), r != null, na, r == null ? null : r.ts(),
                        r == null ? null : r.source(), r == null ? null : r.conf(), null, null));
            }
        }

        String lastCp = null;
        boolean gap = false;
        int applicable = 0;
        int consecutive = 0;
        boolean done = false;
        List<String> reversed = new ArrayList<>();
        List<Map<String, Object>> history = new ArrayList<>();
        Map<String, Object> stats = new LinkedHashMap<>();
        for (int i = 0; i < states.size(); i++) {
            CpState s = states.get(i);
            Checkpoint cp = cps.get(i);
            Map<String, Object> st = new LinkedHashMap<>();
            st.put("basis", s.basis());
            st.put("passed", s.passed());
            st.put("na", s.na());
            if (s.done() != null) {
                st.put("done", s.done());
                st.put("target", s.target());
            }
            if (s.ts() != null) {
                st.put("ts", s.ts().toString());
            }
            stats.put(s.code(), st);
            if (s.na() && !s.passed()) {
                continue;   // 분모 밖 — 순서에도 세지 않는다
            }
            applicable++;
            if (s.passed()) {
                Map<String, Object> h = new LinkedHashMap<>();
                h.put("cp", s.code());
                h.put("ts", s.ts() == null ? null : s.ts().toString());
                h.put("source", s.source());
                h.put("conf", s.conf());
                history.add(h);
                if (gap) {
                    reversed.add(s.code());
                } else {
                    lastCp = s.code();
                    consecutive++;
                }
                if (cp.done()) {
                    done = true;
                }
            } else {
                gap = true;
            }
        }
        if (!noRecord.isEmpty()) {
            stats.put("no_record", List.copyOf(noRecord));
        }
        String state = done ? "done" : lastCp == null && reversed.isEmpty() ? "notDue" : "inProgress";
        double pct = done ? 100 : applicable == 0 ? 0 : Math.round(1000.0 * consecutive / applicable) / 10.0;
        return new Result(lastCp, state, pct, history, stats, reversed);
    }

    /**
     * 계획 절점과 그 계획일 — 마킹 = 절단계획일(YPWC310M cut_pln_date = 앵커 plan_sd),
     * 절단 완료 = 절단완료 계획일(cut_cmp_pln_date = plan_fd, 없으면 절단계획일). wiki: 계획 대비 완료 지연은 절단계획 기준
     */
    public static final List<String> PLANNED_CPS = List.of("MARK_DONE", "CUT_DONE");

    /** @param plan cp_plan — 절점마다 계획일 · @param late 늦은 절점 수 (계획일 뒤에 닿았거나, 계획일이 지났는데 아직) */
    public record Plan(Map<String, String> plan, int late) {}

    public static Plan plan(LocalDate planSd, LocalDate planFd, Map<String, Reached> reached, LocalDate today, ZoneId zone) {
        Map<String, String> plan = new LinkedHashMap<>();
        int late = 0;
        for (String cp : PLANNED_CPS) {
            LocalDate due = "CUT_DONE".equals(cp) && planFd != null ? planFd : planSd;
            if (due == null) {
                continue;
            }
            plan.put(cp, due.toString());
            Reached r = reached.get(cp);
            LocalDate at = r == null || r.ts() == null ? null : r.ts().atZone(zone).toLocalDate();
            if (at != null ? at.isAfter(due) : today.isAfter(due)) {
                late++;
            }
        }
        return new Plan(plan, late);
    }

    /** 강재 반입예정 경과 — 반입예정일이 지났는데 불출(STEEL_ISSUE) 전. 사외 절단 도면은 해당 없음. */
    public static boolean carryOverdue(Instant caryDue, boolean steelIssued, boolean siteOut, Instant now) {
        return caryDue != null && !steelIssued && !siteOut && caryDue.isBefore(now);
    }
}
