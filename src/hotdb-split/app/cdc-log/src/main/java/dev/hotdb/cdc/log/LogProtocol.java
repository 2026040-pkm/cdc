package dev.hotdb.cdc.log;

import dev.hotdb.cdc.port.SourceCapabilities;
import java.util.Map;

/**
 * 캡처부가 여는 HTTP — 전달부가 당겨 간다(pull). 캡처부는 전달부가 있는지 모른다.
 *
 * <pre>
 * GET  /log/info                                  → {@link Info} (JSON)
 * GET  /log/records?from=&lt;seq&gt;&max=&lt;n&gt;&waitMs=&lt;ms&gt; → text/plain, 한 줄 = seq\tjson (없으면 waitMs 동안 기다렸다 빈 응답)
 *                                                   410 = from 이 이미 지워진 구간 (전달부는 멈춘다)
 * POST /log/ack?consumer=&lt;이름&gt;&seq=&lt;seq&gt;        → 204. 이 소비자가 seq 까지 반영을 끝냈다 — 보존 기준
 * </pre>
 */
public final class LogProtocol {

    public static final String INFO = "/log/info";
    public static final String RECORDS = "/log/records";
    public static final String ACK = "/log/ack";
    /** records 응답 헤더 — 지금 로그의 마지막 순번 (전달부 밀림 지표) */
    public static final String LAST_SEQ = "X-Log-Last-Seq";
    public static final String FIRST_SEQ = "X-Log-First-Seq";

    private LogProtocol() {}

    /**
     * @param firstSeq     아직 남아 있는 가장 앞 순번 (보존 정리로 앞이 지워진다)
     * @param lastSeq      마지막 순번 (0 = 비었다)
     * @param source       캡처부 원천이 주는 것 — 전달부 라우트가 이것으로 될 일인지 기동 때 맞춰 본다
     * @param acked        소비자별 반영 확인 순번
     * @param bytes        로그 디스크 크기
     */
    public record Info(long firstSeq, long lastSeq, SourceCapabilities source, Map<String, Long> acked, long bytes) {}
}
