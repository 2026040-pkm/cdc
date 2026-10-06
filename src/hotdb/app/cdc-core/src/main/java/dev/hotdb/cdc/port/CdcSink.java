package dev.hotdb.cdc.port;

import dev.hotdb.cdc.event.CdcEvent;
import dev.hotdb.cdc.event.TableId;
import java.util.List;

/**
 * 출력 포트 — {@link CdcSource} 가 배치를 넘기는 곳. 기본은 설정 라우트로 표에 쓰는
 * {@link dev.hotdb.cdc.apply.RouteApplier} 이고, 서비스가 이 타입의 빈을 직접 두면 그것을 쓴다
 * (예: RFC Provider — 표가 아니라 SAP 로 보낸다).
 *
 * <p>{@link #apply} 가 돌아온 뒤에 원천 위치(오프셋)가 전진한다. 예외를 던지면 원천이 멈추고 프로세스가 재시작돼
 * 같은 배치를 다시 받는다(at-least-once) — 구현은 멱등이어야 한다.
 */
public interface CdcSink {

    /** 이 원천 표를 받는지. 아니면 원천이 무시 이벤트(no_route)로 센다. */
    boolean interestedIn(TableId table);

    void apply(List<CdcEvent> events);
}
