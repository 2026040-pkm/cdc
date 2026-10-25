package dev.hotdb.poll;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 레거시 폴링 설정 ({@code hotdb.poll.*}). 원천 표 → 대상 표를 작업(job) 단위로 적는다.
 *
 * <p>표가 늘면 작업 한 블록만 더한다. 컬럼은 원천과 대상에 둘 다 있는 것만 옮기므로 한쪽에 컬럼이 늘어도
 * 코드는 그대로다 — 기동 때 빠지는 컬럼을 로그로 알려 준다.
 *
 * <p>대상은 둘 이상일 수 있다 (RFC Service — 레거시 DB erp 사본 · Hot DB hot.l_* 적재). {@code targets} 에
 * 이름을 붙여 두고 작업의 {@code to} 로 고른다. 비우면 {@code target}(없으면 서비스 기본 DataSource).
 * 진행 상태(ops.poll_state)는 그 대상 DB 에 남는다.
 *
 * @param agent      진행 상태(ops.poll_state)의 주인 이름 — rfc-service · db-agent
 * @param target     기본 대상 접속. 비우면 서비스의 기본 DataSource 를 쓴다
 * @param targets    이름 붙인 대상 접속 (이름 → 접속). 작업이 to 로 고른다
 * @param sources    원천 접속 (이름 → 접속). 작업이 source 로 고른다
 * @param jobs       작업 목록
 * @param jobSets    이름 붙인 작업 묶음 — 작업 목록을 파일 여럿으로 나눌 때. Spring 은 같은 이름의 목록을 합치지 않고
 *                   뒤 파일로 덮어쓰므로(2026-10-23 poll-jobs-hot.yml 이 SAP 114 작업을 지웠다), 파일마다 묶음 이름을 달리 둔다.
 *                   {@link #jobs()} 가 jobs 와 묶음 전부를 합쳐 돌려준다
 * @param fetchSize  원천에서 한 번에 가져오는 행 수
 * @param batchSize  대상에 한 번에 쓰는 행 수 (한 트랜잭션)
 */
@ConfigurationProperties("hotdb.poll")
public record PollProperties(
        String agent,
        Conn target,
        Map<String, Conn> targets,
        Map<String, Conn> sources,
        List<JobSpec> jobs,
        Map<String, List<JobSpec>> jobSets,
        @DefaultValue("60000") long defaultIntervalMs,
        @DefaultValue("1000") int fetchSize,
        @DefaultValue("500") int batchSize,
        @DefaultValue("2") int threads) {

    public record Conn(String url, String user, String password) {}

    /**
     * @param name       작업 이름 (지표 · 상태 키). 비우면 target
     * @param source     sources 의 이름
     * @param from       원천 표 (스키마.표) 또는 괄호로 싼 SELECT 와 별칭 — {@code (SELECT … FROM a JOIN b …) q}.
     *                   HANA · Oracle 은 따옴표 없이 쓰면 대문자로 접힌다
     * @param target     대상 표 (스키마.표)
     * @param to         targets 의 이름. 비우면 기본 대상
     * @param keys       UPSERT 키. 비우면 대상 표의 PK
     * @param mode       incremental (워터마크 이상만) · full (전부 UPSERT) · replace (지우고 다시 — PK 없는 표)
     * @param watermark  incremental 의 워터마크 식 (원천 SQL). 예: {@code upd_date || upd_time}. 문자열 비교로 순서를 정한다
     * @param intervalMs 주기. 비우면 defaultIntervalMs
     * @param scope      적재 추적 범위 (대상에 src_job · polled_at 열이 있을 때 src_job 에 쓰는 값). 비우면 작업 이름.
     *                   증분 작업과 그 하루 1회 전체 재동기화 작업이 같은 scope 를 쓰면 한 범위로 삭제를 본다
     * @param markMissing full 작업이 끝난 뒤, 같은 scope 에서 이번 실행에 안 온 행에 src_deleted_at 을 찍는다
     *                   (레거시 원천에서 지워진 행 — V20 개선안 F4). 대상에 세 추적 열이 있어야 한다
     */
    public record JobSpec(String name, String source, String from, String target, String to, List<String> keys,
                          @DefaultValue("incremental") String mode, String watermark, Long intervalMs,
                          @DefaultValue("true") boolean enabled, String scope, @DefaultValue("false") boolean markMissing) {

        // 생성자는 하나만 둔다 — 둘이면 Spring 설정 바인딩이 어느 쪽인지 몰라 작업이 하나도 안 묶인다 (2026-10-25)

        public String jobName() {
            return name == null || name.isBlank() ? target : name;
        }

        public String trackScope() {
            return scope == null || scope.isBlank() ? jobName() : scope;
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
    public Map<String, Conn> targets() {
        return targets == null ? Map.of() : targets;
    }

    @Override
    public List<JobSpec> jobs() {
        List<JobSpec> all = new java.util.ArrayList<>(jobs == null ? List.of() : jobs);
        if (jobSets != null) {
            jobSets.values().forEach(all::addAll);
        }
        return all;
    }
}
