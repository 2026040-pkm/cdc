package dev.hotdb.cdc.route;

import dev.hotdb.cdc.route.RoutingProperties.LookupSpec;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 참조 표 하나를 통째로 메모리에 둔다 (예: tag_key → 장비 · 권역).
 *
 * <p>이력 행에는 tag_key 만 있어서 어느 권역 장비인지 알려면 tag_catalog · device 를 봐야 한다.
 * 행마다 질의하지 않고 캐시를 쓴다. 갱신은 두 길로 한다 — reload-on 표의 CDC 이벤트, 그리고
 * 처음 보는 키(태그를 막 등록해서 이벤트보다 캐시가 늦은 경우)를 만났을 때. 후자는 초당 한 번으로 묶는다.
 */
public class LookupCache {

    private static final Logger log = LoggerFactory.getLogger(LookupCache.class);
    private static final long MISS_RELOAD_INTERVAL_MS = 1000;

    private final String name;
    private final LookupSpec spec;
    private final JdbcTemplate jdbc;
    private final MeterRegistry meters;
    private volatile Map<String, Map<String, Object>> rows = Map.of();
    private volatile long lastReloadAt;

    public LookupCache(String name, LookupSpec spec, JdbcTemplate jdbc, MeterRegistry meters) {
        this.name = name;
        this.spec = spec;
        this.jdbc = jdbc;
        this.meters = meters;
        meters.gauge("hotdb.cdc.lookup.size", io.micrometer.core.instrument.Tags.of("lookup", name), this, c -> c.rows.size());
    }

    public String name() {
        return name;
    }

    public LookupSpec spec() {
        return spec;
    }

    public synchronized void reload(String reason) {
        Map<String, Map<String, Object>> next = new HashMap<>();
        jdbc.query(spec.sql(), rs -> {
            var md = rs.getMetaData();
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                row.put(md.getColumnLabel(i), rs.getObject(i));
            }
            next.put(Objects.toString(row.get(spec.key())), row);
        });
        rows = next;
        lastReloadAt = System.currentTimeMillis();
        meters.counter("hotdb.cdc.lookup.reload", "lookup", name, "reason", reason).increment();
        log.debug("lookup {} 다시 읽음 ({}행, {})", name, next.size(), reason);
    }

    /** 키로 행을 찾는다. 없으면 한 번 다시 읽어 본다 (초당 1회 한도). */
    public Map<String, Object> get(Object key) {
        if (key == null) {
            return null;
        }
        String k = Objects.toString(key);
        Map<String, Object> hit = rows.get(k);
        if (hit == null && System.currentTimeMillis() - lastReloadAt > MISS_RELOAD_INTERVAL_MS) {
            reload("miss");
            hit = rows.get(k);
        }
        if (hit == null) {
            meters.counter("hotdb.cdc.lookup.miss", "lookup", name).increment();
        }
        return hit;
    }
}
