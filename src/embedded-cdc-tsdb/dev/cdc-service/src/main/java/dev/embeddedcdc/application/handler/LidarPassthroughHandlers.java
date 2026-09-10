package dev.embeddedcdc.application.handler;

import dev.embeddedcdc.domain.model.SourceTable;
import dev.embeddedcdc.domain.port.out.PassthroughRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * lidar 스택(src/timescaledb) 표 네 개의 핸들러. 컬럼 목록이 곧 설정이다.
 *
 * 컬럼 이름은 src/timescaledb/infra/db/init/01-schema.sql 과 같아야 한다.
 * 원천에 컬럼이 늘면 여기에도 더해야 target 에 옮겨진다 — 빠뜨리면 그 컬럼만 조용히 빠진다.
 * 반대로 여기 있는 컬럼이 원천에서 사라지면 NULL 로 들어간다(키 컬럼이면 예외).
 *
 * lidar_status(하이퍼테이블)는 없다. 이유는 SourceTable 주석 참고.
 */
@Configuration
public class LidarPassthroughHandlers {

    @Bean
    TableSyncHandler lidarDeviceStateSyncHandler(PassthroughRepository repository) {
        return new PassthroughSyncHandler(
                SourceTable.LIDAR_DEVICE_STATE,
                List.of("tid"),
                List.of("tid", "device_role", "site", "zone", "shop", "bay", "status", "error_code",
                        "scan_rate_pts_per_sec", "temperature_c", "connectivity_rssi", "fov_mode",
                        "last_event_at", "last_heartbeat_at", "updated_at"),
                repository);
    }

    @Bean
    TableSyncHandler lidarStatusMessageSyncHandler(PassthroughRepository repository) {
        return new PassthroughSyncHandler(
                SourceTable.LIDAR_STATUS_MESSAGE,
                List.of("kafka_partition", "kafka_offset"),
                List.of("kafka_partition", "kafka_offset", "kafka_ts", "kafka_topic", "uniqueid", "msg_ts",
                        "message_version", "method_id", "data_type", "project_id", "infra_proc_name",
                        "task_area_code", "send_topic", "item_count", "status_count", "actual_count",
                        "artifact_count", "reject_count", "received_at"),
                repository);
    }

    @Bean
    TableSyncHandler lidarIngestRejectSyncHandler(PassthroughRepository repository) {
        return new PassthroughSyncHandler(
                SourceTable.LIDAR_INGEST_REJECT,
                List.of("id"),
                List.of("id", "received_at", "kafka_offset", "payload", "reason"),
                repository);
    }

    /**
     * 키가 (send_topic, param_id) 인 이유는 원천과 같다 — EES ParameterId 는 메시지 안에서만
     * 1부터 매기는 일련번호라 토픽이 다르면 같은 숫자가 다른 태그를 가리킬 수 있다.
     */
    @Bean
    TableSyncHandler lidarTagCatalogSyncHandler(PassthroughRepository repository) {
        return new PassthroughSyncHandler(
                SourceTable.LIDAR_TAG_CATALOG,
                List.of("send_topic", "param_id"),
                List.of("send_topic", "param_id", "tag_id", "device_id", "tid", "channel",
                        "artifact_type", "mqtt_topic", "mqtt_topic_key", "field", "data_type",
                        "updated_at"),
                repository);
    }
}
