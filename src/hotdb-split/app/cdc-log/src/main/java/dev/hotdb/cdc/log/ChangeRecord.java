package dev.hotdb.cdc.log;

import dev.hotdb.cdc.event.CdcEvent;

/**
 * 변경 로그 한 줄 — 캡처부가 원천에서 받은 변경 한 건에 순번을 매긴 것.
 *
 * <p>전달부는 {@link #seq} 만 위치로 쓴다. 원천 위치({@link #position}: 슬롯 LSN · 폴링 워터마크)는 추적용으로만
 * 싣고 해석하지 않는다 — 그래서 캡처부의 엔진을 바꿔도 전달부는 그대로다.
 *
 * @param seq      캡처 로그 안의 순번. 1 부터, 빈틈없이 하나씩 는다
 * @param position 원천 위치 표기 ({@code lsn:<숫자>} · {@code wm:<값>}). 없으면 null
 * @param event    엔진에 매이지 않는 변경 ({@link CdcEvent}; raw 는 이 줄의 JSON)
 */
public record ChangeRecord(long seq, String position, CdcEvent event) {}
