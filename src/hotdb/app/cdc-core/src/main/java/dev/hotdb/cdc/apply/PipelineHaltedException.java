package dev.hotdb.cdc.apply;

/**
 * 계속 돌리면 안 되는 실패. {@link dev.hotdb.cdc.port.CdcSink#apply} 밖으로 던지면 원천이 위치를 넘기지 않고 멈춘다 —
 * 유실은 없고, 원인을 고친 뒤 재기동하면 같은 배치부터 다시 받는다.
 *
 * @param reason 지표 라벨 (UNRECOVERABLE · DLQ_RATIO)
 */
public class PipelineHaltedException extends RuntimeException {

    private final String reason;

    public PipelineHaltedException(String reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }

    /** 원인 사슬 어딘가에 정지 판정이 있는지 — 엔진이 감싸서 돌려줘도 알아본다. */
    public static PipelineHaltedException find(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (c instanceof PipelineHaltedException h) {
                return h;
            }
        }
        return null;
    }
}
