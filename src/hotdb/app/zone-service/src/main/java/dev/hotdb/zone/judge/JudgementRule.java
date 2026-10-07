package dev.hotdb.zone.judge;

/**
 * 실적 판정 규칙 — PENDING 실적 하나를 받아 판정 상태를 돌려준다.
 *
 * <p>모듈마다 규칙이 다르면 그 모듈 프로필로 빈을 둔다. 프로필은 ZONE 과 같다:
 * <pre>
 * &#64;Component &#64;Profile("pnt")
 * class PaintRule implements JudgementRule { ... }
 * </pre>
 * 빈이 없는 모듈은 설정 임계값 규칙({@link ThresholdRule}, {@code hotdb.judge.threshold})을 쓴다.
 *
 * <p>판정 단계가 행을 잠근 트랜잭션 안에서 부른다 — 오래 걸리는 외부 호출은 넣지 않는다.
 * 예외를 던지면 그 행은 PENDING 으로 남고 이 프로세스가 도는 동안 다시 집지 않는다 (재기동하면 다시 본다).
 */
public interface JudgementRule {

    String PENDING = "PENDING";
    String CONFIRMED = "CONFIRMED";
    String REJECTED = "REJECTED";

    /** @return 판정 상태. PENDING · null 은 허용하지 않는다 — 판정을 미룰 수 없다 */
    String judge(ActualResult r);

    /** 로그 · 지표에 남는 규칙 이름 */
    default String name() {
        return getClass().getSimpleName();
    }
}
