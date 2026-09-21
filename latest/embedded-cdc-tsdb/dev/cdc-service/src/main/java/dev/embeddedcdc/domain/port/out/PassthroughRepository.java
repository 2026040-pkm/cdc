package dev.embeddedcdc.domain.port.out;

import java.util.List;
import java.util.Map;

/**
 * source 와 target 의 컬럼이 같은 테이블을 그대로 옮기는 저장소(outbound port).
 *
 * car 처럼 테이블마다 도메인 모델·매퍼·엔티티를 두는 방식은 "변환이 있는 테이블" 을 위한 것이다.
 * lidar 스택의 표는 변환이 없고 컬럼이 열 개를 넘어, 같은 틀을 세 번 반복하면
 * 바뀌는 것은 컬럼 이름 목록뿐이다. 그래서 컬럼 목록을 값으로 받는 저장소를 하나 둔다.
 *
 * 순서 역전 방어는 car 와 같다 — <b>저장된 source_lsn 보다 새로운 이벤트일 때만</b> 반영한다.
 * 반환값 0 은 오류가 아니라 차단이다.
 */
public interface PassthroughRepository {

    /**
     * @param table      target 테이블 이름 (코드 상수. 이벤트에서 오는 값이 아니다)
     * @param keyColumns 충돌 판정 키 — target 의 PK 컬럼
     * @param columns    옮길 컬럼 전체 (키 포함). 값은 이벤트에 실린 문자열 그대로이며,
     *                   타입 변환은 target DB 가 컬럼 타입에 맞춰 한다
     * @param values     컬럼명 → 문자열 값 (null 허용)
     */
    int upsertIfNewer(String table, List<String> keyColumns, List<String> columns,
                      Map<String, String> values, long sourceLsn);

    int deleteIfNewer(String table, List<String> keyColumns, Map<String, String> keyValues, long sourceLsn);
}
