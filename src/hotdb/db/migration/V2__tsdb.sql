-- [tsdb] 필드 데이터. 필드 카탈로그(docs/hotdb/BRD.md §03) 기준.
--   device · tag_catalog : 일반 테이블. 이력이 tag_key 로 참조
--   *_history            : 채널별 이력 하이퍼테이블 3개 — 권역 서비스의 CDC 원천
--
-- 이력은 append-only 라 REPLICA IDENTITY 는 DEFAULT 그대로 둔다 (FULL 은 WAL 만 불린다).
-- 컬럼을 바꾸면 zone-service 의 라우트(application.yml)가 기동 시 대조해 어긋남을 알려 준다.

CREATE TABLE tsdb.device (
  site        text    NOT NULL,
  device_id   text    NOT NULL,
  device_role text    NOT NULL,
  zone        text    NOT NULL,   -- ASSEMBLY · OUTFITTING · PAINTING · MACHINING — 권역 서비스가 거르는 기준
  shop        text,
  bay         text,
  active      boolean NOT NULL DEFAULT true,
  PRIMARY KEY (site, device_id)
);

-- 80자대 TagId 를 이력 행마다 반복하지 않도록 tag_key 로 참조한다.
CREATE TABLE tsdb.tag_catalog (
  tag_key        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  edge_group_id  text         NOT NULL,
  tag_id         varchar(100) NOT NULL,   -- {deviceId}.{topic '/'→'_'}.raw_payload
  mqtt_topic     text         NOT NULL,
  source_id      text         NOT NULL,   -- MQTT 의 id (산출물은 종류 접미사 포함)
  site           text         NOT NULL,
  device_id      text         NOT NULL,
  channel        text         NOT NULL CHECK (channel IN ('status','actual','artifact')),
  artifact_type  text,
  active         boolean      NOT NULL DEFAULT true,
  UNIQUE (edge_group_id, tag_id),
  FOREIGN KEY (site, device_id) REFERENCES tsdb.device (site, device_id),
  CHECK ((channel = 'artifact') = (artifact_type IS NOT NULL)),
  CHECK (artifact_type IN ('REGISTERED_PCD','TRANSFORMATION_MATRIX','SEGMENTED_PCD'))
);

-- status: 350 태그 × 1초 ≈ 3,024만 행/일
CREATE TABLE tsdb.status_history (
  event_time            timestamptz NOT NULL,   -- occurred_at
  tag_key               bigint      NOT NULL REFERENCES tsdb.tag_catalog (tag_key),
  status                text        NOT NULL CHECK (status IN ('ONLINE','CALIBRATING','ERROR','OFFLINE')),
  error_code            text,
  last_heartbeat_at     timestamptz,
  scan_rate_pts_per_sec integer     CHECK (scan_rate_pts_per_sec >= 0),
  temperature_c         real,
  connectivity_rssi     smallint,
  fov_mode              text,
  source_time_text      text        NOT NULL,   -- .NET 7자리 원문. PG 는 마이크로초까지만 보존
  source_ingested_at    timestamptz,
  received_at           timestamptz NOT NULL,   -- Provider 수신 시각 — 지연 측정 기준점
  PRIMARY KEY (tag_key, event_time)
) WITH (tsdb.hypertable, tsdb.partition_column = 'event_time',
        tsdb.chunk_interval = '6 hours', tsdb.segmentby = 'tag_key',
        tsdb.orderby = 'event_time DESC');

-- actual: 350 태그 × 1분 ≈ 50만 행/일. stage 없음 (2026-09-22 재설계)
CREATE TABLE tsdb.actual_history (
  event_time          timestamptz NOT NULL,
  tag_key             bigint      NOT NULL REFERENCES tsdb.tag_catalog (tag_key),
  scan_id             uuid        NOT NULL,
  event_type          text        NOT NULL CHECK (event_type IN ('START','PROGRESS','COMPLETE')),
  hull_no             text        NOT NULL,
  block_id            text        NOT NULL,
  scanned_at          timestamptz,
  block_progress_rate real        NOT NULL CHECK (block_progress_rate BETWEEN 0 AND 100),
  match_confidence    real        CHECK (match_confidence BETWEEN 0 AND 1),
  reference_cad_id    text,
  model_version       text,
  source_time_text    text        NOT NULL,
  source_ingested_at  timestamptz,
  received_at         timestamptz NOT NULL,
  PRIMARY KEY (tag_key, event_time, scan_id, event_type)
) WITH (tsdb.hypertable, tsdb.partition_column = 'event_time',
        tsdb.chunk_interval = '1 day', tsdb.segmentby = 'tag_key',
        tsdb.orderby = 'event_time DESC');
CREATE INDEX actual_history_scan_idx ON tsdb.actual_history (scan_id);

-- artifact: 350대 × 12건 × 1분 ≈ 605만 행/일. 파일 본체가 아니라 위치만
CREATE TABLE tsdb.artifact_history (
  event_time            timestamptz NOT NULL,
  tag_key               bigint      NOT NULL REFERENCES tsdb.tag_catalog (tag_key),
  scan_id               uuid        NOT NULL,
  artifact_type         text        NOT NULL CHECK (artifact_type IN
                          ('REGISTERED_PCD','TRANSFORMATION_MATRIX','SEGMENTED_PCD')),
  segment_key           text        NOT NULL DEFAULT '',   -- segment_id null → ''
  hull_no               text        NOT NULL,
  block_id              text        NOT NULL,
  storage_uri           text,
  file_size_bytes       bigint      CHECK (file_size_bytes >= 0),
  checksum              text,
  transformation_matrix real[],
  produced_by_device_id text,
  model_version         text,
  source_time_text      text        NOT NULL,
  source_ingested_at    timestamptz,
  received_at           timestamptz NOT NULL,
  PRIMARY KEY (tag_key, event_time, scan_id, artifact_type, segment_key),
  CHECK ((artifact_type = 'SEGMENTED_PCD') = (segment_key <> '')),
  CHECK (CASE WHEN artifact_type = 'TRANSFORMATION_MATRIX'
              THEN cardinality(transformation_matrix) = 16 AND storage_uri IS NULL
              ELSE transformation_matrix IS NULL AND storage_uri <> '' END)
) WITH (tsdb.hypertable, tsdb.partition_column = 'event_time',
        tsdb.chunk_interval = '1 day', tsdb.segmentby = 'tag_key',
        tsdb.orderby = 'event_time DESC');
CREATE INDEX artifact_history_scan_idx ON tsdb.artifact_history (scan_id);

-- 압축은 CDC 소비자가 따라잡을 여유를 두고 늦게 건다.
-- 압축은 원본 청크 행을 이벤트 없이 비우므로(2026-09-17 실측 B2), 아직 안 읽은 행이
-- 압축되면 그 행은 CDC 로 영영 안 나온다. 소비자 최대 허용 정지 시간보다 길게 잡는다.
-- 2.29 는 segmentby 를 준 하이퍼테이블에 기본 압축 정책을 자동으로 붙인다 — 그걸 지우고 다시 건다.
SELECT remove_compression_policy('tsdb.status_history',   if_exists => true);
SELECT remove_compression_policy('tsdb.actual_history',   if_exists => true);
SELECT remove_compression_policy('tsdb.artifact_history', if_exists => true);
SELECT add_compression_policy('tsdb.status_history',   compress_after => INTERVAL '7 days');
SELECT add_compression_policy('tsdb.actual_history',   compress_after => INTERVAL '7 days');
SELECT add_compression_policy('tsdb.artifact_history', compress_after => INTERVAL '7 days');
-- 보존 90일 (BRD Q6). drop_chunks 는 CDC 에 안 보이지만 RDB 는 이력 미러가 아니라 파생값이라 무방하다.
SELECT add_retention_policy('tsdb.status_history',   drop_after => INTERVAL '90 days');
SELECT add_retention_policy('tsdb.actual_history',   drop_after => INTERVAL '90 days');
SELECT add_retention_policy('tsdb.artifact_history', drop_after => INTERVAL '90 days');

GRANT UPDATE ON tsdb.device, tsdb.tag_catalog TO hotdb_provider;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA tsdb TO hotdb_provider;
