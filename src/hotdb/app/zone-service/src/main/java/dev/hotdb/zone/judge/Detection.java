package dev.hotdb.zone.judge;

import java.time.Instant;

/**
 * 판정 대상 — hot.s_scan_detection 한 행 (judged = false 인 인식 객체). 수치는 필드가 안 보냈으면 null 이다.
 *
 * <p>지금 태그 실적 메시지에는 객체 ID · 개수가 없어 인식 객체 = 블록이다 (object_key = 블록 번호).
 */
public record Detection(
        String scanId,
        String objectKey,
        String hullNo,
        String blockNo,
        String objectType,
        Float progressEst,
        Float matchConf,
        Instant receivedTs,
        String eventType) {
}
