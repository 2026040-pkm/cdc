-- [TSDB 영역] HotDBProvider 가 필드 데이터(LiDAR 태그)를 풀어 넣는 곳.
--   device · tag_catalog : 일반 테이블. 이력 표가 tag_key 로 참조
--   *_history            : 채널별 수신 이력 하이퍼테이블 3개 (압축은 TimescaleDB 기본 정책)
-- CDC 대상이 아니다 — 청크명 · 압축 · drop_chunks 가 논리 디코딩에 제대로 안 보인다.
--
-- 태그 한 건 = tagMode=raw 의 JSON 문자열 하나. 필드마다 태그가 갈라지지 않으므로
-- Provider 가 JSON 을 풀어 채널별 타입 컬럼에 넣는다. 산출물은 태그 하나로 12행이 온다.

CREATE TABLE tsdb.device (
  site        text    NOT NULL,
  device_id   text    NOT NULL,
  device_role text    NOT NULL,
  zone        text,
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
  mqtt_topic     text         NOT NULL,   -- TagId 에서 역변환이 안 되므로 원래 토픽 보관
  source_id      text         NOT NULL,   -- 등록 CSV 의 deviceId (산출물은 종류 접미사 포함)
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

-- status: 350 태그 × 1초 ≈ 3,024만 행/일.
-- 위치(zone·shop·bay)·장비 역할은 1초마다 바뀌지 않으므로 행마다 싣지 않고 tsdb.device 에 둔다.
-- 같은 태그·같은 발생 시각이 다시 오면 PK 충돌 → Provider 가 내용 비교 후 같으면 버리고 다르면 격리.
CREATE TABLE tsdb.status_history (
  event_time            timestamptz NOT NULL,   -- raw_payload.occurred_at
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
  received_at           timestamptz NOT NULL,
  PRIMARY KEY (tag_key, event_time)
) WITH (tsdb.hypertable, tsdb.partition_column = 'event_time',
        tsdb.chunk_interval = '6 hours', tsdb.segmentby = 'tag_key',
        tsdb.orderby = 'event_time DESC');

-- actual: 350 태그 × 1분 ≈ 50만 행/일. stage 없음(2026-09-22 재설계).
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
CREATE INDEX actual_history_block_idx ON tsdb.actual_history (hull_no, block_id, event_time DESC);
CREATE INDEX actual_history_scan_idx  ON tsdb.actual_history (scan_id);

-- artifact: 350대 × 12건 × 1분 ≈ 605만 행/일. 파일 본체가 아니라 위치만.
-- 세그먼트 10개가 같은 TagId · 같은 발생 시각으로 오므로 scan_id · 종류 · 세그먼트까지 PK 에 넣는다.
-- segment_id null 은 '' 로 정규화 — NULL 은 PK 에 못 들어간다.
CREATE TABLE tsdb.artifact_history (
  event_time            timestamptz NOT NULL,
  tag_key               bigint      NOT NULL REFERENCES tsdb.tag_catalog (tag_key),
  scan_id               uuid        NOT NULL,
  artifact_type         text        NOT NULL CHECK (artifact_type IN
                          ('REGISTERED_PCD','TRANSFORMATION_MATRIX','SEGMENTED_PCD')),
  segment_key           text        NOT NULL DEFAULT '',
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
