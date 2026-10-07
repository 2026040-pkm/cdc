package dev.hotdb.cdc.route;

/** 대상 표에 쓰는 방식. 전부 재실행해도 결과가 같다 — CDC 는 at-least-once 라 같은 이벤트가 다시 온다. */
public enum RouteMode {
    /** 키 충돌 시 갱신 (최신값 표) */
    UPSERT,
    /** 키 충돌 시 무시 (이력 · 산출물) */
    INSERT,
    /** 묶음(partition-by)의 직전 행과 change-of 컬럼이 다를 때만 넣기 (상태 전이) */
    INSERT_ON_CHANGE,
    /** 키로 지우기 */
    DELETE;

    public static RouteMode of(String text) {
        return valueOf(text.trim().toUpperCase().replace('-', '_'));
    }
}
