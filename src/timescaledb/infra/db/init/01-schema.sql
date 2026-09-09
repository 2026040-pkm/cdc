-- ─────────────────────────────────────────────────────────────────────────────
-- ot-lidar-status 적재 스키마
--
-- Kafka 레코드 하나 = 장비(tid) 약 200대의 상태 배열이다.
--   [{"content":{"timestamp":<ns epoch>,"tid":"58","value":"<JSON 문자열>","pm_mode":false}}, ...]
-- content.value 를 풀면 아래 열 개 필드가 나온다.
--   status · error_code · scan_rate_pts_per_sec · point_cloud_quality_score · temperature_c
--   · connectivity_rssi · last_heartbeat_at · occurred_at · ingested_at · idempotency_key
--
-- 표는 넷이다.
--   lidar_status         항목 하나 = 행 하나. 하이퍼테이블. 이 스택의 본체.
--   lidar_status_message Kafka 레코드 하나 = 행 하나. 헤더·오프셋·항목 수. 추적용.
--   lidar_device_state   장비별 최신 상태 한 행. 대시보드와 exporter 가 읽는다.
--   lidar_ingest_reject  파싱 실패 항목 격리. 원문과 사유를 남긴다.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── 본체 ────────────────────────────────────────────────────────────────────
-- time 은 메시지의 occurred_at(장비 발생 시각) 이다. Kafka 도착 시각이 아니다.
-- 버퍼 방출로 늦게 온 항목도 제 시각 자리에 들어가야 집계가 맞는다.
CREATE TABLE lidar_status (
    time                      TIMESTAMPTZ      NOT NULL,   -- value.occurred_at
    tid                       TEXT             NOT NULL,   -- content.tid (장비 식별자)
    status                    TEXT             NOT NULL,   -- ONLINE · IDLE · WARNING · ERROR
    error_code                TEXT,                        -- '' 는 NULL 로 넣는다
    scan_rate_pts_per_sec     INTEGER,
    point_cloud_quality_score DOUBLE PRECISION,
    temperature_c             DOUBLE PRECISION,
    connectivity_rssi         SMALLINT,
    last_heartbeat_at         TIMESTAMPTZ,
    ingested_at               TIMESTAMPTZ,                 -- 발신 측이 찍은 값. DB 적재 시각이 아니다
    content_ts                TIMESTAMPTZ,                 -- content.timestamp (ns epoch → tz)
    pm_mode                   BOOLEAN,
    idempotency_key           TEXT             NOT NULL,   -- 발신 측 멱등 키
    kafka_offset              BIGINT,                      -- 어느 레코드에서 왔는지 (lidar_status_message 와 맞춘다)
    received_at               TIMESTAMPTZ      NOT NULL DEFAULT now()  -- DB 적재 시각
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    -- 관측치(2026-09-09) : 레코드당 ~210 행, 레코드 간격 1~3초 → 70~210 행/초 · 하루 최대 ~1,800만 행.
    -- 행 ~110B 로 잡으면 하루 최대 ~2GB, 6시간 청크 ~500MB. VM 메모리 2GiB 의 25%(512MB) 안쪽.
    tsdb.chunk_interval = '6 hours',
    tsdb.segmentby = 'tid',        -- 압축 시 장비별로 묶는다 (조회도 장비 단위가 대부분)
    tsdb.orderby   = 'time DESC'
);

-- 멱등 적재의 근거. Kafka 는 at-least-once 라 재전달이 온다.
-- 하이퍼테이블의 유니크 인덱스는 파티션 컬럼(time)을 반드시 포함해야 한다.
CREATE UNIQUE INDEX lidar_status_uq ON lidar_status (idempotency_key, time);
-- 장비별 최근 조회. 압축 segmentby 와 같은 축이다.
CREATE INDEX lidar_status_tid_time_idx ON lidar_status (tid, time DESC);

-- ── Kafka 레코드 단위 추적 ──────────────────────────────────────────────────
-- 행이 어느 레코드에서 왔는지, 레코드 하나에 항목이 몇 개였는지 남긴다.
-- 3초에 한 행이라 일반 테이블로 둔다.
CREATE TABLE lidar_status_message (
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    kafka_ts         TIMESTAMPTZ NOT NULL,   -- 레코드 CreateTime
    uniqueid         UUID,                   -- 헤더 uniqueid
    msg_ts           TIMESTAMPTZ,            -- 헤더 msg_timestamp (ns epoch)
    message_version  TEXT,                   -- 헤더 message_version
    method_id        TEXT,                   -- 헤더 method_id
    item_count       INTEGER     NOT NULL,
    reject_count     INTEGER     NOT NULL DEFAULT 0,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_partition, kafka_offset)
);

-- ── 장비별 최신 상태 ────────────────────────────────────────────────────────
-- "지금 ERROR 인 장비가 몇 대인가" 를 하이퍼테이블 전체를 훑지 않고 답한다.
-- ingest 가 배치마다 UPSERT 하되, 더 오래된 이벤트가 최신 값을 덮지 못하게
-- last_event_at 비교를 건다 (늦게 도착한 재전달 방어).
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
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── 격리 ────────────────────────────────────────────────────────────────────
-- 항목 하나가 깨졌다고 레코드 전체를 버리지 않는다. 깨진 것만 여기로 보내고
-- 나머지는 적재한다. 운영자가 들여다보는 표라 일반 테이블이다.
CREATE TABLE lidar_ingest_reject (
    id            BIGSERIAL   PRIMARY KEY,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    kafka_offset  BIGINT,
    payload       JSONB       NOT NULL,
    reason        TEXT        NOT NULL
);
