-- ============================================================================
-- lidar 스택(timescaledb) 표의 수신 측 사본
--
-- 원천이 TimescaleDB 인 lidar DB 로 바뀌면서 들어온 네 표. 컬럼은 원천과 같고
-- source_lsn 만 관리 컬럼으로 붙는다(car 와 같은 순서 역전 방어).
--
-- 하이퍼테이블 셋(lidar_status · lidar_scan_actual · lidar_scan_artifact)은 여기 없다.
-- 청크가 주기마다 새 테이블로 생기고 압축·보존이 내부 경로로 행을 옮기고 지워
-- 행 단위 CDC 가 성립하지 않는다 (docs/timescaledb-cdc-impact.html B안).
-- 그 데이터는 lidar 스택 소비자가 직접 적재한다.
--
-- 컬럼은 timescaledb/infra/db/init/01-schema.sql 과 한 줄씩 맞춘다. 원천에 컬럼이 늘면
-- 여기와 LidarPassthroughHandlers 의 목록에도 더해야 옮겨진다 — 빠뜨리면 그 컬럼만 조용히 빈다.
--
-- 시퀀스·기본값을 두지 않는다. 값은 전부 원천에서 온다.
-- ============================================================================
CREATE TABLE lidar_device_state (
    tid                   TEXT PRIMARY KEY,
    device_role           TEXT,
    site                  TEXT,
    zone                  TEXT,
    shop                  TEXT,
    bay                   TEXT,
    status                TEXT NOT NULL,
    error_code            TEXT,
    scan_rate_pts_per_sec BIGINT,
    temperature_c         DOUBLE PRECISION,
    connectivity_rssi     SMALLINT,
    fov_mode              TEXT,
    last_event_at         TIMESTAMPTZ NOT NULL,
    last_heartbeat_at     TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ NOT NULL,
    source_lsn            BIGINT      NOT NULL DEFAULT 0
);

CREATE TABLE lidar_status_message (
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    kafka_ts         TIMESTAMPTZ NOT NULL,
    kafka_topic      TEXT,
    uniqueid         UUID,
    msg_ts           TIMESTAMPTZ,
    message_version  TEXT,
    method_id        TEXT,
    data_type        TEXT,
    project_id       TEXT,
    infra_proc_name  TEXT,
    task_area_code   TEXT,
    send_topic       TEXT,
    item_count       INTEGER     NOT NULL,
    status_count     INTEGER     NOT NULL,
    actual_count     INTEGER     NOT NULL,
    artifact_count   INTEGER     NOT NULL,
    reject_count     INTEGER     NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL,
    source_lsn       BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (kafka_partition, kafka_offset)
);

CREATE TABLE lidar_ingest_reject (
    id            BIGINT      PRIMARY KEY,
    received_at   TIMESTAMPTZ NOT NULL,
    kafka_offset  BIGINT,
    payload       JSONB       NOT NULL,
    reason        TEXT        NOT NULL,
    source_lsn    BIGINT      NOT NULL DEFAULT 0
);

-- InterSysLink 태그 등록부의 사본. Kafka 의 숫자 tid(EES ParameterId)를 장비로 되돌린다.
-- 이 표가 없으면 lidar_device_state.tid 가 무엇을 가리키는지 수신 측에서 설명할 수 없다.
CREATE TABLE lidar_tag_catalog (
    send_topic     TEXT        NOT NULL,
    param_id       BIGINT      NOT NULL,
    tag_id         TEXT        NOT NULL,
    device_id      TEXT        NOT NULL,
    tid            TEXT        NOT NULL,
    channel        TEXT        NOT NULL,
    artifact_type  TEXT,
    mqtt_topic     TEXT        NOT NULL,
    mqtt_topic_key TEXT,
    field          TEXT,
    data_type      TEXT,
    updated_at     TIMESTAMPTZ NOT NULL,
    source_lsn     BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (send_topic, param_id)
);

CREATE INDEX lidar_tag_catalog_tid_idx ON lidar_tag_catalog (tid);

-- ── 블록 축 상태 표 둘 ──────────────────────────────────────────────────────
-- 원천의 하이퍼테이블(tsdb.lidar_scan_actual · tsdb.lidar_scan_artifact)은 CDC 로 못 옮기지만,
-- 그것을 블록 축(hull_no, block_id)으로 접은 상태 표는 일반 표라 옮겨진다. 수신 측이 status
-- 말고 나머지 두 채널까지 보게 되는 것이 이 둘 덕분이다 — 원문은 원천에 남고 현재 상태만 온다.
--
-- 두 표는 축만 같고 모양이 다르다. 실적은 "어디까지 갔나(공정 마일스톤)" 이고
-- 산출물은 "무엇이 몇 개·얼마나 나왔나(종류별 대장)" 다.
--
-- 컬럼은 timescaledb/infra/db/init/01-schema.sql 과 한 줄씩 맞춘다.
-- 생성 컬럼(completed_stage_count · is_complete_set)은 여기서도 같은 식으로 다시 선언한다.
-- CDC 로 옮기지 않는다 — 양쪽이 같은 정의를 들고 각자 계산하므로 어긋날 수 없고,
-- 생성 컬럼은 값을 직접 INSERT 할 수 없어 옮기려 해도 못 옮긴다.
CREATE TABLE lidar_block_progress (
    hull_no                  TEXT NOT NULL,
    block_id                 TEXT NOT NULL,
    site                     TEXT,
    zone                     TEXT,
    shop                     TEXT,
    bay                      TEXT,
    current_stage            TEXT,
    current_event_type       TEXT,
    current_progress_rate    DOUBLE PRECISION,
    current_match_confidence DOUBLE PRECISION,
    current_scan_id          TEXT,
    current_tid              TEXT,
    reference_cad_id         TEXT,
    model_version            TEXT,
    arrangement_completed_at TIMESTAMPTZ,
    fitting_completed_at     TIMESTAMPTZ,
    welding_completed_at     TIMESTAMPTZ,
    inspection_completed_at  TIMESTAMPTZ,
    wiring_completed_at      TIMESTAMPTZ,
    piping_completed_at      TIMESTAMPTZ,
    completed_stage_count    SMALLINT GENERATED ALWAYS AS (
        num_nonnulls(arrangement_completed_at, fitting_completed_at, welding_completed_at,
                     inspection_completed_at, wiring_completed_at, piping_completed_at)
    ) STORED,
    event_count              BIGINT      NOT NULL,
    complete_count           BIGINT      NOT NULL,
    max_progress_rate        DOUBLE PRECISION,
    first_event_at           TIMESTAMPTZ NOT NULL,
    last_event_at            TIMESTAMPTZ NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL,
    source_lsn               BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (hull_no, block_id)
);

CREATE TABLE lidar_block_artifact (
    hull_no                     TEXT NOT NULL,
    block_id                    TEXT NOT NULL,
    registered_pcd_count        BIGINT NOT NULL,
    registered_pcd_bytes        BIGINT NOT NULL,
    transformation_matrix_count BIGINT NOT NULL,
    segmented_pcd_count         BIGINT NOT NULL,
    segmented_pcd_bytes         BIGINT NOT NULL,
    is_complete_set             BOOLEAN GENERATED ALWAYS AS (
        registered_pcd_count > 0 AND transformation_matrix_count > 0 AND segmented_pcd_count > 0
    ) STORED,
    latest_artifact_type        TEXT,
    latest_scan_id              TEXT,
    latest_segment_id           TEXT,
    latest_storage_uri          TEXT,
    latest_checksum             TEXT,
    latest_file_size_bytes      BIGINT,
    produced_by_device_id       TEXT,
    latest_tid                  TEXT,
    model_version               TEXT,
    artifact_count              BIGINT      NOT NULL,
    total_bytes                 BIGINT      NOT NULL,
    first_event_at              TIMESTAMPTZ NOT NULL,
    last_event_at               TIMESTAMPTZ NOT NULL,
    updated_at                  TIMESTAMPTZ NOT NULL,
    source_lsn                  BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (hull_no, block_id)
);
