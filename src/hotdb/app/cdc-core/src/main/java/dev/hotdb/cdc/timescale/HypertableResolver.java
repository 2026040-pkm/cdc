package dev.hotdb.cdc.timescale;

import dev.hotdb.cdc.event.TableId;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 청크 이름 → 하이퍼테이블 이름.
 *
 * <p>하이퍼테이블은 상속 구조라 논리 디코딩에 부모가 아니라
 * {@code _timescaledb_internal._hyper_N_M_chunk} 로 나온다 (2026-09-17 실측 B1).
 * 청크는 인터벌마다 새로 생기므로 처음 본 청크만 카탈로그를 조회하고 캐시한다.
 *
 * <p>카탈로그에 없는 청크(압축 뭉치 {@code *_compressed} 등)는 비어 있는 결과로 캐시한다 —
 * 그 이벤트는 버린다. 연속집계의 materialization 청크는 {@code _timescaledb_internal} 하이퍼테이블로
 * 풀리므로 라우트에 걸리지 않아 역시 버려진다.
 */
public class HypertableResolver {

    private static final Logger log = LoggerFactory.getLogger(HypertableResolver.class);
    private static final Pattern CHUNK = Pattern.compile("_hyper_[0-9]+_[0-9]+_chunk");
    private static final String SQL = """
            SELECT hypertable_schema, hypertable_name
              FROM timescaledb_information.chunks
             WHERE chunk_schema = ? AND chunk_name = ?
            """;

    private final JdbcTemplate jdbc;
    private final MeterRegistry meters;
    private final Map<TableId, Optional<TableId>> cache = new ConcurrentHashMap<>();

    public HypertableResolver(JdbcTemplate jdbc, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.meters = meters;
    }

    /** 청크가 아니면 그대로, 청크면 부모, 풀 수 없으면 empty. */
    public Optional<TableId> resolve(TableId physical) {
        if (!isChunk(physical)) {
            return Optional.of(physical);
        }
        Optional<TableId> hit = cache.get(physical);
        if (hit != null) {
            return hit;
        }
        Optional<TableId> resolved = jdbc.query(SQL,
                rs -> rs.next() ? Optional.of(new TableId(rs.getString(1), rs.getString(2))) : Optional.<TableId>empty(),
                physical.schema(), physical.table());
        cache.put(physical, resolved);
        meters.counter("hotdb.cdc.chunk.resolve", "result", resolved.isPresent() ? "resolved" : "unknown").increment();
        log.info("청크 {} → {}", physical, resolved.map(TableId::toString).orElse("(하이퍼테이블 없음 — 버림)"));
        return resolved;
    }

    public static boolean isChunk(TableId t) {
        return "_timescaledb_internal".equals(t.schema()) && CHUNK.matcher(t.table()).matches();
    }
}
