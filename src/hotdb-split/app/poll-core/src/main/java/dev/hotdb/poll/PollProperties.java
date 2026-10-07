package dev.hotdb.poll;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 레거시 폴링 설정 ({@code hotdb.poll.*}). 원천 표 → HotDB 레거시 표를 작업(job) 단위로 적는다.
 *
 * <p>표가 늘면 작업 한 블록만 더한다. 컬럼은 원천과 대상에 둘 다 있는 것만 옮기므로 한쪽에 컬럼이 늘어도
 * 코드는 그대로다 — 기동 때 빠지는 컬럼을 로그로 알려 준다.
 *
 * @param agent      진행 상태(ops.poll_state)의 주인 이름 — rfc-service · db-agent
 * @param target     HotDB 쓰기 접속. 비우면 서비스의 기본 DataSource 를 쓴다
 * @param sources    원천 접속 (이름 → 접속). 작업이 이름으로 고른다
 * @param fetchSize  원천에서 한 번에 가져오는 행 수
 * @param batchSize  대상에 한 번에 쓰는 행 수 (한 트랜잭션)
 */
@ConfigurationProperties("hotdb.poll")
public record PollProperties(
        String agent,
        Conn target,
        Map<String, Conn> sources,
        List<JobSpec> jobs,
        @DefaultValue("60000") long defaultIntervalMs,
        @DefaultValue("1000") int fetchSize,
        @DefaultValue("500") int batchSize,
        @DefaultValue("2") int threads) {

    public record Conn(String url, String user, String password) {}

    /**
     * @param name       작업 이름 (지표 · 상태 키). 비우면 target
     * @param source     sources 의 이름
     * @param from       원천 표 (스키마.표). HANA · Oracle 은 따옴표 없이 쓰면 대문자로 접힌다
     * @param target     HotDB 표 (스키마.표)
     * @param keys       UPSERT 키. 비우면 대상 표의 PK
     * @param mode       incremental (워터마크 이상만) · full (전부 UPSERT) · replace (지우고 다시 — PK 없는 표)
     * @param watermark  incremental 의 워터마크 식 (원천 SQL). 예: {@code upd_date || upd_time}. 문자열 비교로 순서를 정한다
     * @param intervalMs 주기. 비우면 defaultIntervalMs
     */
    public record JobSpec(String name, String source, String from, String target, List<String> keys,
                          @DefaultValue("incremental") String mode, String watermark, Long intervalMs,
                          @DefaultValue("true") boolean enabled) {

        public String jobName() {
            return name == null || name.isBlank() ? target : name;
        }

        @Override
        public List<String> keys() {
            return keys == null ? List.of() : keys;
        }
    }

    @Override
    public Map<String, Conn> sources() {
        return sources == null ? Map.of() : sources;
    }

    @Override
    public List<JobSpec> jobs() {
        return jobs == null ? List.of() : jobs;
    }
}
