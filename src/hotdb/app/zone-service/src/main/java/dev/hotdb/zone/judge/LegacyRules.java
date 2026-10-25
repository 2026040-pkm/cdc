package dev.hotdb.zone.judge;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;

/**
 * 레거시를 끼고 하는 판별 규칙 — 순수 함수. 출처: ship-brain wiki (실적 산식 · 조립 W/O 매칭 · 의장 설치율 · 도장 실적률).
 *
 * <ul>
 *   <li>조립 W/O 매칭 (ASM-F09/F10): 인식일이 계획 기간에 드는 하위 W/O(하루치) → 앵커 계획이 인식일 ±N일 안(4주 풀) → 불일치
 *   <li>도장 블록 실적률 = Σ413 DLY_PRGS_MH ÷ Σ212 ACTY_MH_1 (공정당 1회) — 100 을 넘기지 않는다
 *   <li>의장 GDO 설치율 = Σ설치 ÷ Σ소요. 품목 상태 = 설치 · 고정 · 가설 · 불출 · 대기
 *   <li>차이 알림: |자동 − 레거시| 가 기준 초과
 * </ul>
 */
public final class LegacyRules {

    private LegacyRules() {}

    /** 앵커의 하위 W/O 한 건 (l_work_unit level WO). */
    public record Wo(String woNo, LocalDate planSd, LocalDate planFd) {}

    /**
     * @param pool  DAY (하루치 W/O) · WEEK4 (앵커 계획 ±N일) · NONE (불일치)
     * @param woNo  고른 W/O (WEEK4 · NONE 이면 계획 시작이 인식일에 가장 가까운 것, 없으면 null)
     * @param lead  EARLY · ON_TIME · LATE (계획 대비 인식일)
     * @param state MATCHED · MISMATCH
     */
    public record Match(String pool, String woNo, String lead, String state) {}

    /** 조립 취부 W/O — W/O 번호 15번째 글자 F. 하루치에 여럿이면 먼저 고른다. */
    static boolean isFitting(String woNo) {
        return woNo != null && woNo.length() >= 15 && woNo.charAt(14) == 'F';
    }

    public static Match matchWo(LocalDate detect, LocalDate anchorSd, LocalDate anchorFd, List<Wo> wos, int weekDays) {
        List<Wo> day = wos.stream()
                .filter(w -> w.planSd() != null && w.planFd() != null && !detect.isBefore(w.planSd()) && !detect.isAfter(w.planFd()))
                .sorted(Comparator.comparing((Wo w) -> !isFitting(w.woNo())).thenComparing(Wo::planSd).thenComparing(Wo::woNo))
                .toList();
        if (!day.isEmpty()) {
            return new Match("DAY", day.get(0).woNo(), "ON_TIME", "MATCHED");
        }
        Wo nearest = wos.stream().filter(w -> w.planSd() != null)
                .min(Comparator.comparingLong((Wo w) -> Math.abs(w.planSd().toEpochDay() - detect.toEpochDay()))
                        .thenComparing(Wo::woNo))
                .orElse(null);
        String lead = lead(detect, anchorSd, anchorFd);
        boolean inWeek = anchorSd != null
                && !detect.isBefore(anchorSd.minusDays(weekDays))
                && !detect.isAfter((anchorFd == null ? anchorSd : anchorFd).plusDays(weekDays));
        if (inWeek) {
            return new Match("WEEK4", nearest == null ? null : nearest.woNo(), lead, "MATCHED");
        }
        return new Match("NONE", nearest == null ? null : nearest.woNo(), lead, "MISMATCH");
    }

    static String lead(LocalDate detect, LocalDate sd, LocalDate fd) {
        if (sd != null && detect.isBefore(sd)) {
            return "EARLY";
        }
        if (fd != null && detect.isAfter(fd)) {
            return "LATE";
        }
        return "ON_TIME";
    }

    /** 도장 블록 실적률 (0~100, 소수 1자리). 분모가 없으면 null. */
    public static Double paintPct(double dailyMhSum, Double mh212) {
        if (mh212 == null || mh212 <= 0) {
            return null;
        }
        return Math.min(100.0, Math.round(1000.0 * dailyMhSum / mh212) / 10.0);
    }

    /** 비율 (0~100, 소수 1자리). 분모가 0 이하면 null. */
    public static Double pct(double num, double den) {
        return den <= 0 ? null : Math.min(100.0, Math.round(1000.0 * num / den) / 10.0);
    }

    /** 의장 품목 상태. 소요가 0 이하면 해당 없음(NA). */
    public static String itemState(Double req, Double inst, Double fixed, Double temp, boolean issued) {
        double r = req == null ? 0 : req;
        if (r <= 0) {
            return "NA";
        }
        if (nz(inst) >= r) {
            return "INSTALLED";
        }
        if (nz(fixed) > 0) {
            return "FIXED";
        }
        if (nz(temp) > 0 || nz(inst) > 0) {
            return "TEMP";
        }
        return issued ? "ISSUED" : "WAITING";
    }

    /** 실적 상태 — 레거시 실적률 · 완료일로. */
    public static String state(Double pct, boolean legacyDone, double donePct) {
        if (legacyDone || (pct != null && pct >= donePct)) {
            return "done";
        }
        return pct != null && pct > 0 ? "inProgress" : "notDue";
    }

    /** 자동 − 레거시 차이가 기준(퍼센트 포인트)을 넘는가. */
    public static boolean gapAlert(Double auto, Double legacy, double limit) {
        return auto != null && legacy != null && Math.abs(auto - legacy) > limit;
    }

    private static double nz(Double d) {
        return d == null ? 0 : d;
    }
}
