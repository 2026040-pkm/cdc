package dev.hotdb.zone.judge;

import java.time.Instant;
import java.util.UUID;

/** 판정 대상 — svc.actual_result 한 행 (COMPLETE 된 실적). 수치는 장비가 안 보냈으면 null 이다. */
public record ActualResult(
        String module,
        String hullNo,
        String blockId,
        UUID scanId,
        String deviceId,
        Instant completedAt,
        Float blockProgressRate,
        Float matchConfidence,
        String referenceCadId,
        String modelVersion,
        Instant appliedAt) {
}
