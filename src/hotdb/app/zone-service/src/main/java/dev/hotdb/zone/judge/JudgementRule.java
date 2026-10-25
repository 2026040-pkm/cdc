package dev.hotdb.zone.judge;

/**
 * 센서 판정 규칙 — judged = false 인 인식 객체 하나를 받아 판정을 돌려준다.
 *
 * <p>모듈마다 규칙이 다르면 그 모듈 프로필로 빈을 둔다. 프로필은 ZONE 과 같다:
 * <pre>
 * &#64;Component &#64;Profile("pnt")
 * class PaintRule implements JudgementRule { ... }
 * </pre>
 * 빈이 없는 모듈은 설정 임계값 규칙({@link ThresholdRule}, {@code hotdb.judge.threshold})을 쓴다.
 *
 * <p>판정 단계가 행을 잠근 트랜잭션 안에서 부른다 — 오래 걸리는 외부 호출은 넣지 않는다.
 * 예외를 던지면 그 행은 judged = false 로 남고 이 프로세스가 도는 동안 다시 집지 않는다 (재기동하면 다시 본다).
 */
public interface JudgementRule {

    /**
     * 판정.
     * <ul>
     *   <li>{@link #DONE} — 진척 · 정합도가 임계 이상. 앵커가 하나로 잡히고 레거시 W/O 가 착수돼 있으면 p_progress done,
     *       W/O 가 없으면 held (판별 O / 레거시 X → 완료 보류, ASM-F10)
     *   <li>{@link #IN_PROGRESS} — 정합도는 넘지만 진척이 모자람 → p_progress inProgress
     *   <li>{@link #LOW_CONFIDENCE} — 정합도가 모자라거나 값이 없음 → 실적에 반영하지 않고 알림(p_action LOW_CONFIDENCE)
     * </ul>
     */
    enum Verdict { DONE, IN_PROGRESS, LOW_CONFIDENCE }

    /** @return 판정. null 은 허용하지 않는다 — 판정을 미룰 수 없다 */
    Verdict judge(Detection d);

    /** 로그 · 지표에 남는 규칙 이름 */
    default String name() {
        return getClass().getSimpleName();
    }
}
