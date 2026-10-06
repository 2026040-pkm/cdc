package dev.hotdb.cdc.route;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 원천 행 → 대상 표 규칙 ({@code hotdb.lookups · hotdb.routes}).
 *
 * <p>스키마가 바뀌면 여기만 고친다. 기동 시 {@link RouteCompiler} 가 원천 · 대상 표의 실제 컬럼과
 * 대조해서, 없는 컬럼을 참조하면 무엇이 어긋났는지 전부 모아 기동을 거부한다.
 */
@ConfigurationProperties("hotdb")
public record RoutingProperties(Map<String, LookupSpec> lookups, List<RouteSpec> routes) {

    @Override
    public Map<String, LookupSpec> lookups() {
        return lookups == null ? Map.of() : lookups;
    }

    @Override
    public List<RouteSpec> routes() {
        return routes == null ? List.of() : routes;
    }

    /**
     * 이벤트에 붙여 읽을 참조 표 (메모리 캐시).
     *
     * @param key      sql 결과에서 찾는 키 컬럼
     * @param sql      캐시를 채우는 질의
     * @param reloadOn 이 표들에 변경 이벤트가 오면 캐시를 다시 읽는다
     */
    public record LookupSpec(String key, String sql, List<String> reloadOn) {
        @Override
        public List<String> reloadOn() {
            return reloadOn == null ? List.of() : reloadOn;
        }
    }

    /**
     * 라우트 하나 = 원천 표 하나 → 대상 표 하나.
     *
     * @param source      원천 논리 테이블 (하이퍼테이블이면 부모 이름)
     * @param lookups     참조 이름 → 키 식. 예: {@code tag: row.tag_key}
     * @param when        모두 참이어야 반영. {@code tag.zone=ASSEMBLY}, {@code row.event_type=START|COMPLETE},
     *                    {@code row.x!=A}, {@code row.x is null}, {@code row.x not null}
     * @param ops         반영할 op (c · u · d · r)
     * @param mode        upsert · insert · insert-on-change · delete
     * @param keys        upsert 충돌 키 · delete 조건
     * @param newerThan   upsert 에서 이 컬럼이 더 새로울 때만 덮어쓴다 (순서 뒤바뀐 재전송 방어)
     * @param changeOf    insert-on-change: 이 컬럼들이 직전 행과 다를 때만 넣는다
     * @param partitionBy insert-on-change: 직전 행을 찾는 묶음 (예: device_id)
     * @param orderBy     insert-on-change: 직전 행 판정 순서 컬럼
     * @param lagFrom     지연 지표 기준 시각 식 (예: row.received_at)
     * @param columns     대상 컬럼 → 식. {@code 식 | keep|coalesce|overwrite|min|max}, {@code 식 if 조건}
     */
    public record RouteSpec(
            String name,
            String source,
            Map<String, String> lookups,
            List<String> when,
            @DefaultValue({"c", "r", "u"}) List<String> ops,
            String target,
            @DefaultValue("upsert") String mode,
            List<String> keys,
            String newerThan,
            List<String> changeOf,
            List<String> partitionBy,
            String orderBy,
            String lagFrom,
            @DefaultValue("true") boolean enabled,
            LinkedHashMap<String, String> columns) {

        @Override
        public Map<String, String> lookups() {
            return lookups == null ? Map.of() : lookups;
        }

        @Override
        public List<String> when() {
            return when == null ? List.of() : when;
        }

        @Override
        public List<String> keys() {
            return keys == null ? List.of() : keys;
        }

        @Override
        public List<String> changeOf() {
            return changeOf == null ? List.of() : changeOf;
        }

        @Override
        public List<String> partitionBy() {
            return partitionBy == null ? List.of() : partitionBy;
        }

        @Override
        public LinkedHashMap<String, String> columns() {
            return columns == null ? new LinkedHashMap<>() : columns;
        }
    }
}
