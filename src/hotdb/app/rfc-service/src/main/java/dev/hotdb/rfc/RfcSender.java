package dev.hotdb.rfc;

import java.util.Map;

/** SAP 로 실적(hot.p_progress 한 행)을 보낸다. */
public interface RfcSender {

    /**
     * @param row hot.p_progress 한 행 (컬럼 이름 → 값). 키는 anchor_id · as_of · state
     * @throws RfcRejectedException SAP 가 내용 때문에 거절 — 다시 보내도 안 되므로 dead letter 로 간다.
     *                              그 밖의 예외는 일시 장애로 보고 재시도한다
     */
    void send(Map<String, Object> row);

    /** SAP 가 내용을 거절했다 (접속 · 시간 초과가 아니라). */
    class RfcRejectedException extends RuntimeException {
        public RfcRejectedException(String message) {
            super(message);
        }
    }
}
