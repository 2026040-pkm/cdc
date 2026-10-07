package dev.hotdb.cdc.port;

/**
 * 원천 어댑터가 무엇을 줄 수 있는지 — 엔진을 바꿀 때 라우트가 조용히 틀리지 않게 하는 계약.
 *
 * <p>논리 복제 계열(Debezium · pgoutput 직접 수신)은 다 준다. 버전 컬럼 폴링은 지워진 행을 볼 수 없고
 * 변경 전 값도 없다 — 그런 원천에 {@code mode: delete} 나 {@code before.*} 를 쓰는 라우트를 걸면
 * 기동에서 거부한다({@link CdcSink#incompatibilities}).
 *
 * @param adapter      어댑터 이름 (debezium · pgoutput · polling · log)
 * @param positionKind 위치 표기 — {@code pg-lsn}(슬롯 LSN) · {@code watermark}(버전 컬럼 값 + 키) · {@code log-seq}
 * @param deletes      삭제를 이벤트로 준다
 * @param beforeImage  변경 전 행(before)을 준다 — PG 는 REPLICA IDENTITY FULL 일 때만 전체 컬럼
 * @param commitOrder  원천 커밋 순서대로 준다 (같은 키의 순서가 뒤집히지 않는다)
 * @param commitTime   원천 커밋 시각(meta.commit_time · 지연 지표)을 준다
 */
public record SourceCapabilities(String adapter, String positionKind, boolean deletes, boolean beforeImage,
                                 boolean commitOrder, boolean commitTime) {

    /** PostgreSQL 논리 복제 — Debezium 이든 pgoutput 직접 수신이든 같다 */
    public static SourceCapabilities logicalReplication(String adapter) {
        return new SourceCapabilities(adapter, "pg-lsn", true, true, true, true);
    }

    /** 버전 컬럼 폴링 — 삭제 · 변경 전 값 없음, 순서는 버전 컬럼 순 */
    public static SourceCapabilities versionPolling(String adapter) {
        return new SourceCapabilities(adapter, "watermark", false, false, true, false);
    }
}
