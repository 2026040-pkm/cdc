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

    /**
     * 블록별 공정 진행. 원천에서 ingest 가 배치마다 UPSERT 하므로 UPDATE 가 대부분이다.
     * 누계(event_count·complete_count)와 마일스톤(*_completed_at)은 원천이 이미 합친 결과가
     * 실려 온다 — 여기서 다시 더하거나 비교하지 않는다. 그랬다가는 같은 이벤트가 두 번 반영된다.
     *
     * completed_stage_count 는 목록에 없다. 생성 컬럼이라 target 이 스스로 계산하고,
     * 값을 직접 INSERT 하려 하면 PostgreSQL 이 거부한다.
     */
    @Bean
    TableSyncHandler lidarBlockProgressSyncHandler(PassthroughRepository repository) {
        return new PassthroughSyncHandler(
                SourceTable.LIDAR_BLOCK_PROGRESS,
                List.of("hull_no", "block_id"),
                List.of("hull_no", "block_id", "site", "zone", "shop", "bay",
                        "current_stage", "current_event_type", "current_progress_rate",
                        "current_match_confidence", "current_scan_id", "current_tid",
                        "reference_cad_id", "model_version",
                        "arrangement_completed_at", "fitting_completed_at", "welding_completed_at",
                        "inspection_completed_at", "wiring_completed_at", "piping_completed_at",
                        "event_count", "complete_count", "max_progress_rate",
                        "first_event_at", "last_event_at", "updated_at"),
                repository);
    }

    /**
     * 블록별 산출물 대장. 종류 셋이 건수·용량 컬럼으로 누워 있어 행이 블록당 하나다.
     * is_complete_set 은 생성 컬럼이라 위와 같은 이유로 목록에 없다.
     */
    @Bean
    TableSyncHandler lidarBlockArtifactSyncHandler(PassthroughRepository repository) {
        return new PassthroughSyncHandler(
                SourceTable.LIDAR_BLOCK_ARTIFACT,
                List.of("hull_no", "block_id"),
                List.of("hull_no", "block_id",
                        "registered_pcd_count", "registered_pcd_bytes",
                        "transformation_matrix_count",
                        "segmented_pcd_count", "segmented_pcd_bytes",
                        "latest_artifact_type", "latest_scan_id", "latest_segment_id",
                        "latest_storage_uri", "latest_checksum", "latest_file_size_bytes",
                        "produced_by_device_id", "latest_tid", "model_version",
                        "artifact_count", "total_bytes",
                        "first_event_at", "last_event_at", "updated_at"),
                repository);
    }
}
