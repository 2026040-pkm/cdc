package dev.hotdb.cdc.port;

import dev.hotdb.cdc.event.CdcEvent;
import java.time.Instant;
import java.util.Optional;

/**
 * 입력 포트 — 원천 DB 의 행 변경을 읽어 {@link CdcSink} 로 넘긴다.
 *
 * <p>서비스(판별 모듈 · RFC Service)는 이 인터페이스만 안다. 무엇으로 읽는지(Debezium Embedded,
 * pgoutput 직접 수신, Kafka 토픽 …)는 어댑터가 정하고 {@code hotdb.cdc.adapter} 로 고른다.
 *
 * <p>어댑터가 지켜야 할 약속:
 * <ul>
 *   <li>원천 레코드를 {@link dev.hotdb.cdc.event.CdcEvent} 로 바꿔 넘긴다. 하이퍼테이블 청크는 논리 표 이름으로 되돌린다.
 *   <li>배치 하나를 {@link CdcSink#apply} 가 끝낸 <b>뒤에</b> 위치를 넘긴다 — 그 사이에 죽으면 같은 배치를 다시 넘긴다.
 *   <li>{@link CdcSink#interestedIn} 이 아닌 표의 이벤트는 넘기지 않는다.
 *   <li>시작 · 정지는 어댑터가 맡는다 (스프링 빈이면 생명주기에 맞춰 스스로 돈다).
 *   <li>기동 전에 캡처 연결고리를 확인하고({@link dev.hotdb.cdc.continuity.SlotContinuityGuard}), 배치마다 처리 위치를
 *       남긴다({@link dev.hotdb.cdc.continuity.CdcCheckpoints}).
 *   <li>원천 레코드를 해석하지 못하면 엔진을 죽이지 않고 원문째 dead letter 로 격리한다.
 *   <li>싱크가 {@link dev.hotdb.cdc.apply.PipelineHaltedException} 을 던지면 위치를 넘기지 않고 멈추되 프로세스는 살려 둔다
 *       ({@link State#HALTED}) — 재시작해도 같은 이유로 멈추므로, 살아서 health DOWN 과 지표로 알린다.
 * </ul>
 */
public interface CdcSource {

    /** 순서가 지표 값(hotdb_cdc_state)이다 — 뒤에만 더한다. HALTED = 스스로 멈춤 (정지 판정 · 캡처 갭) */
    enum State { STOPPED, STARTING, RUNNING, FAILED, HALTED }

    /**
     * @param pipeline          인스턴스 이름 (지표 · heartbeat · dead letter 에 찍힌다)
     * @param position          어디서 읽는지 — PostgreSQL 이면 복제 슬롯 이름
     * @param lastBatchAt       마지막 배치를 넘긴 시각. 아직 없으면 null
     * @param lastEventCommitAt 마지막으로 넘긴 이벤트의 원천 커밋 시각. 없으면 null
     * @param failure           멈춘 이유. 정상이면 null
     */
    record Status(State state, String pipeline, String position, Instant lastBatchAt, Instant lastEventCommitAt,
                  String failure) {

        /** 받고 있거나 받을 준비 중인지. */
        public boolean healthy() {
            return state == State.STARTING || state == State.RUNNING;
        }
    }

    Status status();

    /** dead letter 에 남긴 원문을 다시 이벤트로 (재처리용). 변경이 아닌 레코드면 empty. 원문 형식은 어댑터만 안다. */
    Optional<CdcEvent> decode(String raw);
}
