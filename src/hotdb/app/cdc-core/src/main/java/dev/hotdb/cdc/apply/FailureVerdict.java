package dev.hotdb.cdc.apply;

import java.sql.SQLException;
import java.util.Set;

/**
 * 반영 실패를 어떻게 다룰지. 성공/실패 둘이 아니라 셋이다 — 모든 실패를 격리하면 DB 재기동 30초 동안
 * 전체 트래픽이 dead letter 로 쏟아진다. 그건 유실을 격리한 게 아니라 옮겨 담은 것이다.
 *
 * <p>스프링 예외 계층이 아니라 SQLSTATE 로 가른다 — 접속 실패(DataAccessResourceFailureException)가
 * NonTransient 밑에 있어서 instanceof 로 가르면 접속 장애가 격리 대상이 된다.
 */
public enum FailureVerdict {

    /** 일시 장애 — 시간이 지나면 풀린다 (접속 끊김 · 교착 · 자원 부족). 배치 전체를 다시 한다 */
    RETRY,
    /** 그 건 하나의 데이터 문제 — 몇 번을 넣어도 같다 (제약 위반 · 값 형식 · 같은 키 중복). 건 단위로 좁혀 격리한다 */
    DEAD_LETTER,
    /** 구조 문제 — 모든 건이 같은 이유로 실패한다 (표 · 컬럼 없음 · 권한 · 인증). 오프셋을 넘기지 않고 멈춘다 */
    HALT;

    private static final Set<String> RETRY_CODES = Set.of("40001", "40P01", "55P03");
    private static final Set<String> RETRY_CLASSES = Set.of("08", "53", "57");
    private static final Set<String> DEAD_LETTER_CLASSES = Set.of("21", "22", "23");
    private static final Set<String> HALT_CLASSES = Set.of("28", "3D", "3F", "42");

    /** 원인 사슬에서 SQLSTATE 를 찾아 판정한다. 모르는 것은 RETRY — 한 번 더 해 보고, 안 되면 건 단위 격리로 간다. */
    public static FailureVerdict of(Throwable t) {
        String state = sqlState(t);
        if (state == null || state.length() < 2) {
            return RETRY;
        }
        if (RETRY_CODES.contains(state)) {
            return RETRY;
        }
        String cls = state.substring(0, 2);
        if (RETRY_CLASSES.contains(cls)) {
            return RETRY;
        }
        if (DEAD_LETTER_CLASSES.contains(cls)) {
            return DEAD_LETTER;
        }
        if (HALT_CLASSES.contains(cls)) {
            return HALT;
        }
        return RETRY;
    }

    static String sqlState(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (c instanceof SQLException sql && sql.getSQLState() != null) {
                return sql.getSQLState();
            }
        }
        return null;
    }
}
