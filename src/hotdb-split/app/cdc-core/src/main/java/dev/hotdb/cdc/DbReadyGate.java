package dev.hotdb.cdc;

import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 기동 때 Hot DB 가 응답할 때까지 기다린다 — DB 보다 서비스가 먼저 떠도, DB 가 잠깐 내려가 있어도 프로세스가 죽지 않게.
 *
 * <p>라우트 대조 · 참조 캐시 · 슬롯 검사는 전부 DB 를 읽으므로, DB 없이 기동하면 컨텍스트가 실패하고 컨테이너가
 * 재시작을 반복했다 (2026-10-06 Hot DB 재기동 때 zone-asm 9회). 그 대신 이 빈이 먼저 만들어지며 {@code SELECT 1} 이
 * 될 때까지 기다리고, DB 를 쓰는 빈들은 이 빈 뒤에 만들어진다. 기다리는 동안은 로그만 남긴다
 * ({@code hotdb.cdc.db-wait.interval-ms} 마다 시도, {@code timeout-ms} 0 = 무한).
 *
 * <p>DB 가 온 뒤 라우트가 스키마와 안 맞는 것은 그대로 기동 거부다 — 그건 기다려서 풀리는 문제가 아니다.
 */
public class DbReadyGate {

    private static final Logger log = LoggerFactory.getLogger(DbReadyGate.class);

    public DbReadyGate(String pipeline, JdbcTemplate jdbc, HotdbCdcProperties.DbWait wait) {
        Instant start = Instant.now();
        Instant lastLog = Instant.EPOCH;
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                jdbc.queryForObject("SELECT 1", Integer.class);
                if (attempt > 1) {
                    log.info("[{}] Hot DB 응답 — {}초 기다린 뒤 기동을 계속한다", pipeline,
                            Duration.between(start, Instant.now()).toSeconds());
                }
                return;
            } catch (RuntimeException e) {
                long waited = Duration.between(start, Instant.now()).toSeconds();
                if (wait.timeoutMs() > 0 && waited * 1000 >= wait.timeoutMs()) {
                    throw new IllegalStateException("Hot DB 가 " + waited + "초 안에 응답하지 않았습니다: " + root(e), e);
                }
                if (Duration.between(lastLog, Instant.now()).toSeconds() >= 30) {
                    log.warn("[{}] Hot DB 대기 중 ({}초, 시도 {}): {}", pipeline, waited, attempt, root(e));
                    lastLog = Instant.now();
                }
                try {
                    Thread.sleep(wait.intervalMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Hot DB 대기 중 중단", ie);
                }
            }
        }
    }

    private static String root(Throwable e) {
        Throwable r = e;
        while (r.getCause() != null && r.getCause() != r) {
            r = r.getCause();
        }
        String m = r.getMessage() == null ? r.getClass().getSimpleName() : r.getMessage();
        return m.length() > 160 ? m.substring(0, 160) : m;
    }
}
