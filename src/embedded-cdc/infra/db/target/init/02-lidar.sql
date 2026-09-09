-- ============================================================================
-- lidar 스택(src/timescaledb) 표의 수신 측 사본
--
-- 원천이 TimescaleDB 인 lidar DB 로 바뀌면서 들어온 세 표. 컬럼은 원천과 같고
-- source_lsn 만 관리 컬럼으로 붙는다(car 와 같은 순서 역전 방어).
--
-- lidar_status(하이퍼테이블)는 여기 없다. 청크가 주기마다 새 테이블로 생기고 압축·보존이
-- 내부 경로로 행을 옮기고 지워 행 단위 CDC 가 성립하지 않는다
-- (docs/timescaledb-cdc-impact.html B안). 그 데이터는 lidar 스택 소비자가 직접 적재한다.
--
-- 시퀀스·기본값을 두지 않는다. 값은 전부 원천에서 온다.
-- ============================================================================
CREATE TABLE lidar_device_state (
    tid                       TEXT PRIMARY KEY,
    status                    TEXT NOT NULL,
    error_code                TEXT,
    scan_rate_pts_per_sec     INTEGER,
    point_cloud_quality_score DOUBLE PRECISION,
    temperature_c             DOUBLE PRECISION,
    connectivity_rssi         SMALLINT,
    last_event_at             TIMESTAMPTZ NOT NULL,
    last_heartbeat_at         TIMESTAMPTZ,
    updated_at                TIMESTAMPTZ NOT NULL,
    source_lsn                BIGINT      NOT NULL DEFAULT 0
);

CREATE TABLE lidar_status_message (
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    kafka_ts         TIMESTAMPTZ NOT NULL,
    uniqueid         UUID,
    msg_ts           TIMESTAMPTZ,
    message_version  TEXT,
    method_id        TEXT,
    item_count       INTEGER     NOT NULL,
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
