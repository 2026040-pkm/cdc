-- ─────────────────────────────────────────────────────────────────────────────
-- InterSysLink(OT) LiDAR 필드 데이터 적재 스키마
--
-- Kafka 레코드 하나 = 태그 여러 개의 값 배열이다 (EES Kafka Provider · data_type=value · V1.0).
--   [{"content":{"timestamp":<ns epoch>,"tid":"<숫자>","value":"<JSON 문자열>","pm_mode":false}}, ...]
--
-- 채널은 **Kafka 토픽**이 정한다. EES 메시지 하나가 토픽 하나이고, 채널마다 메시지를 따로
-- 만들어 두었기 때문이다 (EES_MESSAGE.msgHeaderFormat.topic.send_topic).
--
--   ot.lidar.status    ← ot/device/{zone}/lidar/status              1건/1초·대   → lidar_status
--   ot.lidar.actual    ← ot/sensor/{stage}/actual                   1건/1분·대   → lidar_scan_actual
--   ot.lidar.artifact  ← ot/pipeline/{zone}/{shop}/{bay}/artifact  12건/1분·대   → lidar_scan_artifact
--
-- content.tid 는 TagId 가 아니라 **EES ParameterId(숫자)** 다. Provider 가 태그를 실을 때
-- 문자열 TagId 를 버리고 이 번호만 쓴다(ValueMessageFormatterV1_0). 그리고 raw_payload
-- 어디에도 장비 id 필드가 없다 — 즉 Kafka 만 읽어서는 어느 장비인지 알 수 없다.
-- 그 번호를 장비로 되돌리는 것이 lidar_tag_catalog 다 (아래 · scripts/export-tag-catalog.py).
--
-- tagMode=raw 라 content.value 는 raw_payload 객체 통째의 JSON 문자열이다. 채널마다 키가 다르다.
--
-- 표는 일곱이다.
--   lidar_status         상태 항목 하나 = 행 하나. 하이퍼테이블.
--   lidar_scan_actual    실적 항목 하나 = 행 하나. 하이퍼테이블. scan_id 로 산출물과 이어진다.
--   lidar_scan_artifact  산출물 메타 하나 = 행 하나. 하이퍼테이블. 파일 본체는 오지 않는다(경로만).
--   lidar_status_message Kafka 레코드 하나 = 행 하나. 헤더·오프셋·항목 수. 추적용.
--   lidar_device_state   장비별 최신 상태 한 행. 대시보드와 exporter 가 읽는다.
--   lidar_ingest_reject  파싱 실패 항목 격리. 원문과 사유를 남긴다.
--   lidar_tag_catalog    숫자 tid → 장비 id·TagId·MQTT 토픽. InterSysLink 등록부의 사본.
--
-- 컬럼 이름은 raw_payload 의 키를 그대로 쓴다. 이름을 바꾸면 필드 정의서와 대조가 안 된다.
-- 예외는 넷뿐이다 — time(=occurred_at) · tid(=장비 id) · tag_id(=문자열 TagId) ·
-- param_id(=content.tid 숫자 그대로).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── 0) 태그 등록부 ──────────────────────────────────────────────────────────
-- InterSysLink 의 EES_TAG × TAG_CATALOG 를 그대로 옮겨 둔 사본이다.
-- 소비자가 기동할 때 통째로 읽어 (send_topic, param_id) → 장비 id 로 푼다.
--
-- 이 표가 비어 있어도 적재는 멈추지 않는다. 다만 tid 가 '#2257' 같은 미해석 값으로 남고
-- 장비 축 대시보드가 숫자만 보여 준다. 그때는 scripts/export-tag-catalog.py 를 다시 돌린 뒤
-- down.sh -v 로 볼륨을 지우거나, 이 표에 직접 넣고 소비자를 재시작한다.
--
-- 키가 (send_topic, param_id) 인 이유: ParameterId 는 워크플로우·PType 안에서만 1부터
-- 매기는 일련번호라 메시지가 다르면 같은 숫자가 다른 태그를 가리킬 수 있다.
CREATE TABLE lidar_tag_catalog (
    send_topic     TEXT        NOT NULL,   -- Kafka 토픽 (= EES send_topic · 헤더 request)
    param_id       BIGINT      NOT NULL,   -- content.tid
    tag_id         TEXT        NOT NULL,   -- 문자열 TagId ({장비}.{토픽}.{필드})
    device_id      TEXT        NOT NULL,   -- TagId 의 장비 조각 (산출물은 -SEGMENTED_PCD 등이 붙어 있다)
    tid            TEXT        NOT NULL,   -- 장비 축 — device_id 에서 산출물 접미사를 뗀 값
    channel        TEXT        NOT NULL,   -- status · actual · artifact
    artifact_type  TEXT,                   -- 산출물 태그면 REGISTERED_PCD 등
    mqtt_topic     TEXT        NOT NULL,   -- ot/device/assembly/lidar/status
    mqtt_topic_key TEXT,                   -- '/' 를 '_' 로 접은 것 — 옛 계약의 TagId 가운데 조각
    field          TEXT,                   -- raw_payload
    data_type      TEXT,                   -- STRING
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (send_topic, param_id)
);

CREATE INDEX lidar_tag_catalog_tid_idx ON lidar_tag_catalog (tid);

-- ── 1) 장비 상태 ────────────────────────────────────────────────────────────
-- time 은 raw_payload.occurred_at(장비 발생 시각) 이다. Kafka 도착 시각이 아니다.
-- 버퍼 방출로 늦게 온 항목도 제 시각 자리에 들어가야 집계가 맞는다.
CREATE TABLE lidar_status (
    time                      TIMESTAMPTZ      NOT NULL,   -- raw_payload.occurred_at
    tid                       TEXT             NOT NULL,   -- 장비 id (예 LDR-GJ-A1B3-07). 카탈로그가 없으면 '#<param_id>'
    tag_id                    TEXT,                        -- 문자열 TagId (카탈로그에서 푼 값)
    param_id                  BIGINT,                      -- content.tid — 실려 온 숫자 그대로
    mqtt_topic                TEXT,                        -- 카탈로그의 토픽 조각 (ot_device_assembly_lidar_status)
    device_role               TEXT,                        -- LIDAR
    site                      TEXT,                        -- geoje
    zone                      TEXT,                        -- ASSEMBLY · OUTFITTING
    shop                      TEXT,                        -- assembly1 · outfitting1
    bay                       TEXT,                        -- bay1 ~ bay7
    status                    TEXT             NOT NULL,   -- ONLINE · CALIBRATING · ERROR · OFFLINE
    error_code                TEXT,                        -- ERROR 일 때만 채워진다. 빈 문자열은 NULL 로 넣는다
    scan_rate_pts_per_sec     BIGINT,                      -- OFFLINE 이면 0
    temperature_c             DOUBLE PRECISION,
    connectivity_rssi         SMALLINT,
    fov_mode                  TEXT,                        -- wide · narrow
    last_heartbeat_at         TIMESTAMPTZ,                 -- OFFLINE 이면 멈춘다 — 죽은 장비의 신호
    ingested_at               TIMESTAMPTZ,                 -- 발신 측이 찍은 값. DB 적재 시각이 아니다
    content_ts                TIMESTAMPTZ,                 -- content.timestamp (ns epoch → tz)
    pm_mode                   BOOLEAN,
    idempotency_key           TEXT             NOT NULL,   -- 발신 측 멱등 키 ({장비}:{yyyyMMddTHHmmss})
    kafka_offset              BIGINT,                      -- 어느 레코드에서 왔는지 (lidar_status_message 와 맞춘다)
    received_at               TIMESTAMPTZ      NOT NULL DEFAULT now()  -- DB 적재 시각
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    -- 발행기 기본값: 장비 350대 × 1초 = 350행/초 · 하루 ~3,000만 행.
    -- 행 ~140B 로 잡으면 하루 ~4GB, 6시간 청크 ~1GB.
    tsdb.chunk_interval = '6 hours',
    tsdb.segmentby = 'tid',        -- 압축 시 장비별로 묶는다 (조회도 장비 단위가 대부분)
    tsdb.orderby   = 'time DESC'
);

-- 멱등 적재의 근거. MQTT QoS 1 · Kafka at-least-once 라 재전달이 온다.
-- 하이퍼테이블의 유니크 인덱스는 파티션 컬럼(time)을 반드시 포함해야 한다.
CREATE UNIQUE INDEX lidar_status_uq ON lidar_status (idempotency_key, time);
-- 장비별 최근 조회. 압축 segmentby 와 같은 축이다.
CREATE INDEX lidar_status_tid_time_idx ON lidar_status (tid, time DESC);

-- ── 2) 실적 결과 ────────────────────────────────────────────────────────────
-- 스캔 한 번이 실적 1건 + 산출물 12건을 만든다. scan_id 가 둘을 잇는 유일한 조인 키다.
CREATE TABLE lidar_scan_actual (
    time                 TIMESTAMPTZ      NOT NULL,   -- raw_payload.occurred_at
    tid                  TEXT             NOT NULL,   -- 장비 id. 카탈로그가 없으면 '#<param_id>'
    tag_id               TEXT,
    param_id             BIGINT,                      -- content.tid — 실려 온 숫자 그대로
    mqtt_topic           TEXT,                        -- ot_sensor_welding_actual 등
    site                 TEXT,
    zone                 TEXT,
    shop                 TEXT,
    bay                  TEXT,
    stage                TEXT,                        -- ARRANGEMENT·FITTING·WELDING·INSPECTION·WIRING·PIPING
    record_type          TEXT,                        -- ACTUAL
    input_method         TEXT,                        -- AUTO
    source_system        TEXT,                        -- AI Inference Service · LiDAR Edge Service
    hull_no              TEXT,                        -- 호선
    block_id             TEXT,                        -- 블록
    scan_id              TEXT             NOT NULL,   -- 산출물과 이어지는 키
    scanned_at           TIMESTAMPTZ,
    pan_tilt             TEXT,                        -- 장비 구성 — 팬틸트
    edge_pc              TEXT,                        -- 엣지 PC
    inference_ws         TEXT,                        -- 추론 워크스테이션
    vision_ocr           TEXT,                        -- OCR 이 개입한 스캔에만 채워진다
    event_type           TEXT,                        -- START · PROGRESS · COMPLETE
    block_progress_rate  DOUBLE PRECISION,            -- 되돌아가지 않는다. COMPLETE 뒤 0 부터 다시
    reference_cad_id     TEXT,
    match_confidence     DOUBLE PRECISION,            -- 정합 신뢰도 0~1
    model_version        TEXT,
    ingested_at          TIMESTAMPTZ,
    content_ts           TIMESTAMPTZ,
    pm_mode              BOOLEAN,
    idempotency_key      TEXT             NOT NULL,   -- {scan_id 앞 8자}:{stage}:{event_type}
    kafka_offset         BIGINT,
    received_at          TIMESTAMPTZ      NOT NULL DEFAULT now()
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    -- 350대 × 1분 = 5.8행/초. 하루 50만 행이라 청크를 크게 잡는다.
    tsdb.chunk_interval = '1 day',
    tsdb.segmentby = 'tid',
    tsdb.orderby   = 'time DESC'
);

CREATE UNIQUE INDEX lidar_scan_actual_uq ON lidar_scan_actual (idempotency_key, time);
CREATE INDEX lidar_scan_actual_tid_time_idx ON lidar_scan_actual (tid, time DESC);
-- 실적에서 산출물로 건너갈 때 쓰는 축.
CREATE INDEX lidar_scan_actual_scan_idx ON lidar_scan_actual (scan_id, time DESC);

-- ── 3) 산출물 메타 ──────────────────────────────────────────────────────────
-- 점군 파일 자체는 오지 않는다. storage_uri · file_size_bytes · checksum 만 온다.
-- 변환행렬만 숫자 16개뿐이라 예외적으로 메시지에 직접 실려 온다.
CREATE TABLE lidar_scan_artifact (
    time                  TIMESTAMPTZ      NOT NULL,   -- raw_payload.occurred_at
    tid                   TEXT             NOT NULL,   -- 장비 id (-REGISTERED_PCD 등 접미사는 뗀다). 없으면 '#<param_id>'
    tag_id                TEXT,
    param_id              BIGINT,                      -- content.tid — 실려 온 숫자 그대로
    mqtt_topic            TEXT,                        -- ot_pipeline_assembly_assembly1_bay1_artifact
    scan_id               TEXT             NOT NULL,
    artifact_type         TEXT             NOT NULL,   -- REGISTERED_PCD · TRANSFORMATION_MATRIX · SEGMENTED_PCD
    hull_no               TEXT,
    block_id              TEXT,
    segment_id            TEXT,                        -- SEGMENTED_PCD 에만 (SEG-001 …)
    storage_uri           TEXT,                        -- file://… — 본체는 DB 밖에 있다
    file_size_bytes       BIGINT,
    checksum              TEXT,                        -- sha256:…
    -- 4x4 = 숫자 16개. 배열 대신 jsonb 로 둔다 — 원문 그대로 보관하고 행 길이를 예측 가능하게 둔다.
    transformation_matrix JSONB,
    produced_by_device_id TEXT,                        -- 추론 워크스테이션
    model_version         TEXT,
    ingested_at           TIMESTAMPTZ,
    content_ts            TIMESTAMPTZ,
    pm_mode               BOOLEAN,
    -- 이 채널만 발신 측 멱등 키가 없다. 소비자가 {scan_id}:{artifact_type}:{segment_id} 로 만든다.
    idempotency_key       TEXT             NOT NULL,
    kafka_offset          BIGINT,
    received_at           TIMESTAMPTZ      NOT NULL DEFAULT now()
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    -- 350대 × 12건 × 1분 = 70행/초 · 하루 600만 행.
    tsdb.chunk_interval = '6 hours',
    tsdb.segmentby = 'tid',
    tsdb.orderby   = 'time DESC'
);

CREATE UNIQUE INDEX lidar_scan_artifact_uq ON lidar_scan_artifact (idempotency_key, time);
CREATE INDEX lidar_scan_artifact_scan_idx ON lidar_scan_artifact (scan_id, time DESC);
CREATE INDEX lidar_scan_artifact_type_time_idx ON lidar_scan_artifact (artifact_type, time DESC);

-- ── Kafka 레코드 단위 추적 ──────────────────────────────────────────────────
-- 행이 어느 레코드에서 왔는지, 레코드 하나에 항목이 몇 개였는지 남긴다.
-- 헤더는 EES V1.0 의 11개 계약(EesHeaderBuilderV1_0)에서 뜻이 있는 것만 받는다.
-- request 헤더가 곧 발신 측이 설정한 send_topic 이다.
CREATE TABLE lidar_status_message (
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    kafka_ts         TIMESTAMPTZ NOT NULL,   -- 레코드 CreateTime
    kafka_topic      TEXT,                   -- 실제로 소비한 Kafka 토픽
    uniqueid         UUID,                   -- 헤더 uniqueid (레코드마다 새로 생긴다)
    msg_ts           TIMESTAMPTZ,            -- 헤더 msg_timestamp (ns epoch)
    message_version  TEXT,                   -- 헤더 message_version (1.0)
    method_id        TEXT,                   -- 헤더 method_id
    data_type        TEXT,                   -- 헤더 data_type (value · cmd · alert · unknown)
    project_id       TEXT,                   -- 헤더 project_id (= 레코드 키)
    infra_proc_name  TEXT,                   -- 헤더 infra_proc_name
    task_area_code   TEXT,                   -- 헤더 task_area_code
    send_topic       TEXT,                   -- 헤더 request
    item_count       INTEGER     NOT NULL,
    status_count     INTEGER     NOT NULL DEFAULT 0,   -- 채널별 항목 수 — 한 레코드에 세 채널이 섞여 온다
    actual_count     INTEGER     NOT NULL DEFAULT 0,
    artifact_count   INTEGER     NOT NULL DEFAULT 0,
    reject_count     INTEGER     NOT NULL DEFAULT 0,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_partition, kafka_offset)
);

-- ── 장비별 최신 상태 ────────────────────────────────────────────────────────
-- "지금 ERROR 인 장비가 몇 대인가" 를 하이퍼테이블 전체를 훑지 않고 답한다.
-- ingest 가 배치마다 UPSERT 하되, 더 오래된 이벤트가 최신 값을 덮지 못하게
-- last_event_at 비교를 건다 (늦게 도착한 재전달 방어).
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
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
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
