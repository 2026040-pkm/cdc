package dev.hotdb.cdc.event;

import java.util.Map;

/**
 * 원천 행 변경 한 건. 값은 컬럼 이름 → JSON 값(String · Number · Boolean · List · Map) 이다.
 *
 * <p>행을 도메인 객체로 바꾸지 않고 맵으로 들고 다니는 이유: 스키마가 아직 바뀌는 중이라,
 * 컬럼이 늘거나 이름이 바뀔 때 코드가 아니라 라우트 설정만 고치게 하려는 것이다.
 *
 * @param table      논리 테이블 (청크면 하이퍼테이블 부모로 바꾼 뒤)
 * @param physical   WAL 에 실제로 찍힌 테이블 (청크 이름일 수 있다)
 * @param lsn        커밋 LSN — dead letter 추적용
 * @param commitTsMs 원천 커밋 시각 (DB 시계)
 * @param raw        원문 JSON — dead letter 에 그대로 남긴다
 */
public record CdcEvent(TableId table, TableId physical, Op op,
                       Map<String, Object> before, Map<String, Object> after,
                       Long lsn, Long commitTsMs, String raw) {

    /** 삭제면 before, 아니면 after. 라우트의 row.* 가 읽는 쪽이다. */
    public Map<String, Object> row() {
        return op == Op.DELETE ? before : after;
    }
}
