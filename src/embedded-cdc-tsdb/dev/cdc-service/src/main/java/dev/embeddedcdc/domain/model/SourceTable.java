package dev.embeddedcdc.domain.model;

import java.util.Optional;

/**
 * 이 파이프라인이 동기화하는 source 테이블.
 *
 * 여기에 없는 테이블의 이벤트는 무시된다. publication 이 두 테이블만 담고 있으므로
 * 평소에는 도달하지 않지만, publication 이 바뀌면 조용히 유실되는 대신 경고가 남도록
 * 이름을 열거형으로 고정해 둔다.
 */
public enum SourceTable {

    CAR("car"),
    COMPUTER("computer"),

    // grade 는 member 의 부모다. 열거 순서는 적용 순서와 무관하다 —
    // 순서는 이벤트의 LSN 이 정하고, BatchApplier 가 그 순서대로 부른다.
    GRADE("grade"),
    MEMBER("member"),

    // lidar 스택(src/timescaledb)의 일반 테이블. 원천이 TimescaleDB 인 lidar DB 로 바뀌면서
    // 들어왔다. 하이퍼테이블 셋(lidar_status · lidar_scan_actual · lidar_scan_artifact)은
    // 넣지 않는다 — 청크가 주기마다 새 테이블로 생기고 압축·보존이 내부 경로로 행을 옮기고
    // 지워서, 행 단위 CDC 가 성립하지 않는다
    // (docs/timescaledb-cdc-impact.html B안). 그 데이터는 lidar 스택의 소비자가 직접 적재한다.

    // 원천에서 이 표들은 rdb 스키마에 산다(01-schema.sql · 04-cdc-lidar.sql). 여기 이름에
    // 스키마를 붙이지 않는 것은 Debezium 이벤트의 source.table 이 스키마 없는 표 이름이기
    // 때문이다 — 스키마는 source.schema 로 따로 온다. 스키마 거르기는 table-include-list 가 한다.
    LIDAR_DEVICE_STATE("lidar_device_state"),
    LIDAR_STATUS_MESSAGE("lidar_status_message"),
    LIDAR_INGEST_REJECT("lidar_ingest_reject"),

    // Kafka 는 장비 id 를 싣지 않고 EES ParameterId(숫자)만 싣는다. 그 숫자를 장비로
    // 되돌리는 등록부 사본이다 — 수신 측에서도 tid 를 읽으려면 같이 와야 한다.
    // 태그를 다시 등록할 때만 바뀌므로 변경량은 사실상 0 이다.
    LIDAR_TAG_CATALOG("lidar_tag_catalog");

    private final String tableName;

    SourceTable(String tableName) {
        this.tableName = tableName;
    }

    public String tableName() {
        return tableName;
    }

    public static Optional<SourceTable> fromName(String name) {
        for (SourceTable table : values()) {
            if (table.tableName.equals(name)) {
                return Optional.of(table);
            }
        }
        return Optional.empty();
    }
}
