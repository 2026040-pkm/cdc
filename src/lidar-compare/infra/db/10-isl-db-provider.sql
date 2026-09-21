-- ─────────────────────────────────────────────────────────────────────────────
-- C 경로(isl-db) 적재 함수 — ISL Db.Provider 가 배치마다 한 번 부른다
--
--   SELECT kind, name, n FROM isl.apply_tag_batch($1::jsonb)
--
-- Db.Provider 는 lidar 를 모른다. Engine 이 NATS(intersyslink.workflow.{wf}.tags)로 보낸 태그 변경을
-- 받은 그대로 JSON 배열로 넘길 뿐이다.
--
--   [{"tagId": "LDR-GJ-A1B4-29.ot_device_assembly_lidar_status.raw_payload",
--     "valueKind": "String", "value": "{...raw_payload...}", "changedAt": "2026-09-17T04:54:40.42+00:00"}, ...]
--
-- 이 파일이 그 태그를 A·B 경로와 같은 표 · 같은 행으로 펼친다. 규칙은 src/timescaledb/dev/ingest/ingest.py
-- (parse_mqtt_message · build_row · *_row · state_rows_of · progress_rows · artifact_rows · write_batch)와 같다.
-- 한쪽을 고치면 다른 쪽도 고친다 — 세 경로 비교는 "같은 행을 쓰는 비용" 을 재야 하기 때문이다.
--
-- 한 문장(데이터 변경 CTE)으로 쓴다. 파싱은 한 번만 하고, 표 3개 INSERT · 상태 표 3개 UPSERT ·
-- 격리 · 배치 원장이 한 트랜잭션 스냅숏에서 같이 돈다. 돌려주는 행이 Provider 지표가 된다.
--
--   kind='channel'  name=status|actual|artifact  n=파싱된 항목 수      → lidar_ingest_items_total{channel}
--   kind='table'    name=lidar_status|…          n=실제로 들어간 행 수  → lidar_ingest_table_rows_total{table}
--   kind='reject'   name=사유(':' 앞)            n=격리 수             → lidar_ingest_rejects_total{reason}
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS isl;

-- ingest.py num() — 숫자·문자열만 통과시킨다. 캐스팅은 컬럼 타입이 한다.
CREATE OR REPLACE FUNCTION isl.num(v jsonb) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN jsonb_typeof(v) IN ('number', 'string') THEN v #>> '{}' END
$$;

CREATE OR REPLACE FUNCTION isl.apply_tag_batch(p_items jsonb)
RETURNS TABLE (kind text, name text, n bigint)
LANGUAGE sql VOLATILE AS $fn$
WITH
-- MQTT 경로와 같이 DB 배치 하나를 원장 행 하나로 적는다. 행의 kafka_offset 도 이 id(µs epoch)다.
batch AS (
    SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint AS id
),
src AS (
    SELECT e.ord, e.item,
           e.item ->> 'tagId' AS tag_id,
           e.item ->> 'value' AS value_text,
           NULLIF(e.item ->> 'changedAt', '')::timestamptz AS changed_at
      FROM jsonb_array_elements(p_items) WITH ORDINALITY AS e(item, ord)
),
-- TagId = {장비 id}.{토픽 '/'→'_'}.{필드}. 장비 id 와 토픽에는 '.' 이 없어 늘 세 조각이다 (split_tag_id).
parsed AS (
    SELECT s.*,
           cardinality(string_to_array(s.tag_id, '.')) AS parts,
           split_part(s.tag_id, '.', 1) AS device_id,
           split_part(s.tag_id, '.', 2) AS topic_key,
           split_part(s.tag_id, '.', 3) AS field,
           -- 토픽의 마지막 마디가 채널이다 (channel_of_mqtt_topic)
           lower(regexp_replace(split_part(s.tag_id, '.', 2), '^.*_', '')) AS channel,
           CASE WHEN s.value_text IS NOT NULL AND pg_input_is_valid(s.value_text, 'jsonb')
                THEN s.value_text::jsonb END AS v
      FROM src s
),
judged AS (
    SELECT p.*,
           CASE
               WHEN p.tag_id IS NULL OR p.parts <> 3 OR p.device_id = '' OR p.topic_key = '' OR p.field = ''
                   THEN 'tagId is not {device}.{topic}.{field}: ' || coalesce(p.tag_id, '')
               WHEN p.field <> 'raw_payload' THEN 'unsupported tag field: ' || p.field
               WHEN p.channel NOT IN ('status', 'actual', 'artifact') THEN 'unknown channel for tag: ' || p.tag_id
               WHEN p.v IS NULL THEN 'value is not JSON'
               WHEN jsonb_typeof(p.v) <> 'object' THEN 'value is not an object'
               -- require() 순서: status(status, occurred_at, idempotency_key)
               --                actual(occurred_at, scan_id, idempotency_key) · artifact(occurred_at, scan_id, artifact_type)
               WHEN p.channel = 'status' AND coalesce(p.v ->> 'status', '') = '' THEN 'missing status'
               WHEN coalesce(p.v ->> 'occurred_at', '') = '' THEN 'missing occurred_at'
               WHEN p.channel = 'status' AND coalesce(p.v ->> 'idempotency_key', '') = '' THEN 'missing idempotency_key'
               WHEN p.channel IN ('actual', 'artifact') AND coalesce(p.v ->> 'scan_id', '') = '' THEN 'missing scan_id'
               WHEN p.channel = 'actual' AND coalesce(p.v ->> 'idempotency_key', '') = '' THEN 'missing idempotency_key'
               WHEN p.channel = 'artifact' AND coalesce(p.v ->> 'artifact_type', '') = '' THEN 'missing artifact_type'
           END AS reject_reason
      FROM parsed p
),
ok AS (
    SELECT j.ord, j.tag_id, j.topic_key, j.channel, j.v, j.changed_at, b.id AS batch_id,
           (j.v ->> 'occurred_at')::timestamptz AS at,
           -- 산출물은 종류마다 다른 id 로 온다 (LDR-…-01-SEGMENTED_PCD). 장비 축은 접미사를 뗀 것 (device_of)
           CASE WHEN j.channel = 'artifact'
                THEN regexp_replace(j.device_id, '-(REGISTERED_PCD|TRANSFORMATION_MATRIX|SEGMENTED_PCD)$', '')
                ELSE j.device_id END AS tid
      FROM judged j, batch b
     WHERE j.reject_reason IS NULL
),

-- ── 하이퍼테이블 3개 ─────────────────────────────────────────────────────────
ins_status AS (
    INSERT INTO tsdb.lidar_status (
        time, tid, tag_id, param_id, mqtt_topic, device_role, site, zone, shop, bay, status, error_code,
        scan_rate_pts_per_sec, temperature_c, connectivity_rssi, fov_mode, last_heartbeat_at,
        ingested_at, content_ts, pm_mode, idempotency_key, kafka_offset)
    SELECT at, tid, tag_id, NULL, topic_key, v ->> 'device_role', v ->> 'site', v ->> 'zone', v ->> 'shop', v ->> 'bay',
           v ->> 'status', NULLIF(v ->> 'error_code', ''),
           isl.num(v -> 'scan_rate_pts_per_sec')::bigint, isl.num(v -> 'temperature_c')::double precision,
           isl.num(v -> 'connectivity_rssi')::smallint, v ->> 'fov_mode', NULLIF(v ->> 'last_heartbeat_at', '')::timestamptz,
           NULLIF(v ->> 'ingested_at', '')::timestamptz, changed_at, NULL, v ->> 'idempotency_key', batch_id
      FROM ok WHERE channel = 'status'
    ON CONFLICT (idempotency_key, time) DO NOTHING
    RETURNING 1
),
ins_actual AS (
    INSERT INTO tsdb.lidar_scan_actual (
        time, tid, tag_id, param_id, mqtt_topic, site, zone, shop, bay, stage, record_type, input_method,
        source_system, hull_no, block_id, scan_id, scanned_at, pan_tilt, edge_pc, inference_ws,
        vision_ocr, event_type, block_progress_rate, reference_cad_id, match_confidence,
        model_version, ingested_at, content_ts, pm_mode, idempotency_key, kafka_offset)
    SELECT at, tid, tag_id, NULL, topic_key, v ->> 'site', v ->> 'zone', v ->> 'shop', v ->> 'bay', v ->> 'stage',
           v ->> 'record_type', v ->> 'input_method', v ->> 'source_system', v ->> 'hull_no', v ->> 'block_id',
           v ->> 'scan_id', NULLIF(v ->> 'scanned_at', '')::timestamptz, v ->> 'pan_tilt', v ->> 'edge_pc',
           v ->> 'inference_ws', NULLIF(v ->> 'vision_ocr', ''), v ->> 'event_type',
           isl.num(v -> 'block_progress_rate')::double precision, v ->> 'reference_cad_id',
           isl.num(v -> 'match_confidence')::double precision, v ->> 'model_version',
           NULLIF(v ->> 'ingested_at', '')::timestamptz, changed_at, NULL, v ->> 'idempotency_key', batch_id
      FROM ok WHERE channel = 'actual'
    ON CONFLICT (idempotency_key, time) DO NOTHING
    RETURNING 1
),
ins_artifact AS (
    INSERT INTO tsdb.lidar_scan_artifact (
        time, tid, tag_id, param_id, mqtt_topic, scan_id, artifact_type, hull_no, block_id, segment_id,
        storage_uri, file_size_bytes, checksum, transformation_matrix, produced_by_device_id,
        model_version, ingested_at, content_ts, pm_mode, idempotency_key, kafka_offset)
    SELECT at, tid, tag_id, NULL, topic_key, v ->> 'scan_id', v ->> 'artifact_type', v ->> 'hull_no', v ->> 'block_id',
           NULLIF(v ->> 'segment_id', ''), NULLIF(v ->> 'storage_uri', ''), isl.num(v -> 'file_size_bytes')::bigint,
           NULLIF(v ->> 'checksum', ''), NULLIF(v -> 'transformation_matrix', 'null'::jsonb), v ->> 'produced_by_device_id',
           v ->> 'model_version', NULLIF(v ->> 'ingested_at', '')::timestamptz, changed_at, NULL,
           -- 이 채널만 발신 측 멱등 키가 없다 — {scan_id}:{artifact_type}:{segment_id|-}
           (v ->> 'scan_id') || ':' || (v ->> 'artifact_type') || ':' || coalesce(NULLIF(v ->> 'segment_id', ''), '-'),
           batch_id
      FROM ok WHERE channel = 'artifact'
    ON CONFLICT (idempotency_key, time) DO NOTHING
    RETURNING 1
),

-- ── 장비 상태 (state_rows_of — 장비별 최신, 같은 시각이면 나중 항목) ───────────
st AS (
    SELECT DISTINCT ON (tid) * FROM ok WHERE channel = 'status' ORDER BY tid, at DESC, ord DESC
),
up_state AS (
    INSERT INTO rdb.lidar_device_state (
        tid, device_role, site, zone, shop, bay, status, error_code, scan_rate_pts_per_sec,
        temperature_c, connectivity_rssi, fov_mode, last_event_at, last_heartbeat_at, updated_at)
    SELECT tid, v ->> 'device_role', v ->> 'site', v ->> 'zone', v ->> 'shop', v ->> 'bay', v ->> 'status',
           NULLIF(v ->> 'error_code', ''), isl.num(v -> 'scan_rate_pts_per_sec')::bigint,
           isl.num(v -> 'temperature_c')::double precision, isl.num(v -> 'connectivity_rssi')::smallint,
           v ->> 'fov_mode', at, NULLIF(v ->> 'last_heartbeat_at', '')::timestamptz, now()
      FROM st
    ON CONFLICT (tid) DO UPDATE SET
        device_role = EXCLUDED.device_role,
        site = EXCLUDED.site,
        zone = EXCLUDED.zone,
        shop = EXCLUDED.shop,
        bay = EXCLUDED.bay,
        status = EXCLUDED.status,
        error_code = EXCLUDED.error_code,
        scan_rate_pts_per_sec = EXCLUDED.scan_rate_pts_per_sec,
        temperature_c = EXCLUDED.temperature_c,
        connectivity_rssi = EXCLUDED.connectivity_rssi,
        fov_mode = EXCLUDED.fov_mode,
        last_event_at = EXCLUDED.last_event_at,
        last_heartbeat_at = EXCLUDED.last_heartbeat_at,
        updated_at = now()
    WHERE EXCLUDED.last_event_at > lidar_device_state.last_event_at
    RETURNING 1
),

-- ── 블록 공정 진행 (progress_rows) ─────────────────────────────────────────
-- 같은 블록이 한 배치에 여러 번 오면 ON CONFLICT 가 "cannot affect row a second time" 으로 죽는다. 먼저 합친다.
act AS (
    SELECT * FROM ok WHERE channel = 'actual' AND v ->> 'hull_no' IS NOT NULL AND v ->> 'block_id' IS NOT NULL
),
act_latest AS (
    SELECT DISTINCT ON (v ->> 'hull_no', v ->> 'block_id') *
      FROM act ORDER BY v ->> 'hull_no', v ->> 'block_id', at DESC, ord DESC
),
act_agg AS (
    SELECT v ->> 'hull_no' AS hull_no, v ->> 'block_id' AS block_id,
           count(*) AS event_count,
           count(*) FILTER (WHERE v ->> 'event_type' = 'COMPLETE') AS complete_count,
           max(isl.num(v -> 'block_progress_rate')::double precision) AS max_progress_rate,
           max(at) FILTER (WHERE v ->> 'event_type' = 'COMPLETE' AND upper(v ->> 'stage') = 'ARRANGEMENT') AS arrangement_completed_at,
           max(at) FILTER (WHERE v ->> 'event_type' = 'COMPLETE' AND upper(v ->> 'stage') = 'FITTING')     AS fitting_completed_at,
           max(at) FILTER (WHERE v ->> 'event_type' = 'COMPLETE' AND upper(v ->> 'stage') = 'INSPECTION')  AS inspection_completed_at,
           max(at) FILTER (WHERE v ->> 'event_type' = 'COMPLETE' AND upper(v ->> 'stage') = 'PIPING')      AS piping_completed_at,
           max(at) FILTER (WHERE v ->> 'event_type' = 'COMPLETE' AND upper(v ->> 'stage') = 'WELDING')     AS welding_completed_at,
           max(at) FILTER (WHERE v ->> 'event_type' = 'COMPLETE' AND upper(v ->> 'stage') = 'WIRING')      AS wiring_completed_at,
           min(at) AS first_event_at, max(at) AS last_event_at
      FROM act GROUP BY 1, 2
),
up_progress AS (
    INSERT INTO rdb.lidar_block_progress (
        hull_no, block_id, site, zone, shop, bay, current_stage, current_event_type, current_progress_rate,
        current_match_confidence, current_scan_id, current_tid, reference_cad_id, model_version,
        event_count, complete_count, max_progress_rate,
        arrangement_completed_at, fitting_completed_at, inspection_completed_at, piping_completed_at,
        welding_completed_at, wiring_completed_at, first_event_at, last_event_at, updated_at)
    SELECT g.hull_no, g.block_id, l.v ->> 'site', l.v ->> 'zone', l.v ->> 'shop', l.v ->> 'bay',
           l.v ->> 'stage', l.v ->> 'event_type', isl.num(l.v -> 'block_progress_rate')::double precision,
           isl.num(l.v -> 'match_confidence')::double precision, l.v ->> 'scan_id', l.tid,
           l.v ->> 'reference_cad_id', l.v ->> 'model_version',
           g.event_count, g.complete_count, g.max_progress_rate,
           g.arrangement_completed_at, g.fitting_completed_at, g.inspection_completed_at, g.piping_completed_at,
           g.welding_completed_at, g.wiring_completed_at, g.first_event_at, g.last_event_at, now()
      FROM act_agg g
      JOIN act_latest l ON l.v ->> 'hull_no' = g.hull_no AND l.v ->> 'block_id' = g.block_id
    ON CONFLICT (hull_no, block_id) DO UPDATE SET
        site = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.site ELSE lidar_block_progress.site END,
        zone = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.zone ELSE lidar_block_progress.zone END,
        shop = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.shop ELSE lidar_block_progress.shop END,
        bay = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.bay ELSE lidar_block_progress.bay END,
        current_stage = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.current_stage ELSE lidar_block_progress.current_stage END,
        current_event_type = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.current_event_type ELSE lidar_block_progress.current_event_type END,
        current_progress_rate = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.current_progress_rate ELSE lidar_block_progress.current_progress_rate END,
        current_match_confidence = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.current_match_confidence ELSE lidar_block_progress.current_match_confidence END,
        current_scan_id = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.current_scan_id ELSE lidar_block_progress.current_scan_id END,
        current_tid = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.current_tid ELSE lidar_block_progress.current_tid END,
        reference_cad_id = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.reference_cad_id ELSE lidar_block_progress.reference_cad_id END,
        model_version = CASE WHEN EXCLUDED.last_event_at > lidar_block_progress.last_event_at THEN EXCLUDED.model_version ELSE lidar_block_progress.model_version END,
        event_count = lidar_block_progress.event_count + EXCLUDED.event_count,
        complete_count = lidar_block_progress.complete_count + EXCLUDED.complete_count,
        max_progress_rate = GREATEST(lidar_block_progress.max_progress_rate, EXCLUDED.max_progress_rate),
        arrangement_completed_at = GREATEST(lidar_block_progress.arrangement_completed_at, EXCLUDED.arrangement_completed_at),
        fitting_completed_at = GREATEST(lidar_block_progress.fitting_completed_at, EXCLUDED.fitting_completed_at),
        inspection_completed_at = GREATEST(lidar_block_progress.inspection_completed_at, EXCLUDED.inspection_completed_at),
        piping_completed_at = GREATEST(lidar_block_progress.piping_completed_at, EXCLUDED.piping_completed_at),
        welding_completed_at = GREATEST(lidar_block_progress.welding_completed_at, EXCLUDED.welding_completed_at),
        wiring_completed_at = GREATEST(lidar_block_progress.wiring_completed_at, EXCLUDED.wiring_completed_at),
        first_event_at = LEAST(lidar_block_progress.first_event_at, EXCLUDED.first_event_at),
        last_event_at = GREATEST(lidar_block_progress.last_event_at, EXCLUDED.last_event_at),
        updated_at = now()
    RETURNING 1
),

-- ── 블록 산출물 대장 (artifact_rows) ───────────────────────────────────────
art AS (
    SELECT *, v ->> 'artifact_type' AS artifact_type, coalesce(isl.num(v -> 'file_size_bytes')::bigint, 0) AS bytes
      FROM ok WHERE channel = 'artifact' AND v ->> 'hull_no' IS NOT NULL AND v ->> 'block_id' IS NOT NULL
),
art_latest AS (
    SELECT DISTINCT ON (v ->> 'hull_no', v ->> 'block_id') *
      FROM art ORDER BY v ->> 'hull_no', v ->> 'block_id', at DESC, ord DESC
),
art_agg AS (
    SELECT v ->> 'hull_no' AS hull_no, v ->> 'block_id' AS block_id,
           count(*) FILTER (WHERE artifact_type = 'REGISTERED_PCD') AS registered_pcd_count,
           coalesce(sum(bytes) FILTER (WHERE artifact_type = 'REGISTERED_PCD'), 0) AS registered_pcd_bytes,
           count(*) FILTER (WHERE artifact_type = 'TRANSFORMATION_MATRIX') AS transformation_matrix_count,
           count(*) FILTER (WHERE artifact_type = 'SEGMENTED_PCD') AS segmented_pcd_count,
           coalesce(sum(bytes) FILTER (WHERE artifact_type = 'SEGMENTED_PCD'), 0) AS segmented_pcd_bytes,
           count(*) AS artifact_count,
           sum(bytes) AS total_bytes,
           min(at) AS first_event_at, max(at) AS last_event_at
      FROM art GROUP BY 1, 2
),
up_artifact AS (
    INSERT INTO rdb.lidar_block_artifact (
        hull_no, block_id, latest_artifact_type, latest_scan_id, latest_segment_id, latest_storage_uri,
        latest_checksum, latest_file_size_bytes, produced_by_device_id, latest_tid, model_version,
        registered_pcd_count, registered_pcd_bytes, transformation_matrix_count, segmented_pcd_count,
        segmented_pcd_bytes, artifact_count, total_bytes, first_event_at, last_event_at, updated_at)
    SELECT g.hull_no, g.block_id, l.artifact_type, l.v ->> 'scan_id', NULLIF(l.v ->> 'segment_id', ''),
           NULLIF(l.v ->> 'storage_uri', ''), NULLIF(l.v ->> 'checksum', ''), isl.num(l.v -> 'file_size_bytes')::bigint,
           l.v ->> 'produced_by_device_id', l.tid, l.v ->> 'model_version',
           g.registered_pcd_count, g.registered_pcd_bytes, g.transformation_matrix_count, g.segmented_pcd_count,
           g.segmented_pcd_bytes, g.artifact_count, g.total_bytes, g.first_event_at, g.last_event_at, now()
      FROM art_agg g
      JOIN art_latest l ON l.v ->> 'hull_no' = g.hull_no AND l.v ->> 'block_id' = g.block_id
    ON CONFLICT (hull_no, block_id) DO UPDATE SET
        latest_artifact_type = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.latest_artifact_type ELSE lidar_block_artifact.latest_artifact_type END,
        latest_scan_id = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.latest_scan_id ELSE lidar_block_artifact.latest_scan_id END,
        latest_segment_id = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.latest_segment_id ELSE lidar_block_artifact.latest_segment_id END,
        latest_storage_uri = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.latest_storage_uri ELSE lidar_block_artifact.latest_storage_uri END,
        latest_checksum = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.latest_checksum ELSE lidar_block_artifact.latest_checksum END,
        latest_file_size_bytes = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.latest_file_size_bytes ELSE lidar_block_artifact.latest_file_size_bytes END,
        produced_by_device_id = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.produced_by_device_id ELSE lidar_block_artifact.produced_by_device_id END,
        latest_tid = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.latest_tid ELSE lidar_block_artifact.latest_tid END,
        model_version = CASE WHEN EXCLUDED.last_event_at > lidar_block_artifact.last_event_at THEN EXCLUDED.model_version ELSE lidar_block_artifact.model_version END,
        registered_pcd_count = lidar_block_artifact.registered_pcd_count + EXCLUDED.registered_pcd_count,
        registered_pcd_bytes = lidar_block_artifact.registered_pcd_bytes + EXCLUDED.registered_pcd_bytes,
        transformation_matrix_count = lidar_block_artifact.transformation_matrix_count + EXCLUDED.transformation_matrix_count,
        segmented_pcd_count = lidar_block_artifact.segmented_pcd_count + EXCLUDED.segmented_pcd_count,
        segmented_pcd_bytes = lidar_block_artifact.segmented_pcd_bytes + EXCLUDED.segmented_pcd_bytes,
        artifact_count = lidar_block_artifact.artifact_count + EXCLUDED.artifact_count,
        total_bytes = lidar_block_artifact.total_bytes + EXCLUDED.total_bytes,
        first_event_at = LEAST(lidar_block_artifact.first_event_at, EXCLUDED.first_event_at),
        last_event_at = GREATEST(lidar_block_artifact.last_event_at, EXCLUDED.last_event_at),
        updated_at = now()
    RETURNING 1
),

-- ── 격리 · 원장 ────────────────────────────────────────────────────────────
ins_reject AS (
    INSERT INTO rdb.lidar_ingest_reject (kafka_offset, payload, reason)
    SELECT b.id, j.item, j.reject_reason FROM judged j, batch b WHERE j.reject_reason IS NOT NULL
    RETURNING 1
),
ins_message AS (
    INSERT INTO rdb.lidar_status_message (
        kafka_partition, kafka_offset, kafka_ts, kafka_topic, uniqueid, msg_ts, message_version,
        method_id, data_type, project_id, infra_proc_name, task_area_code, send_topic,
        item_count, status_count, actual_count, artifact_count, reject_count)
    SELECT 0, b.id, now(), 'isl-db', gen_random_uuid(), NULL, 'isl-nats', NULL, 'raw', 'isl.db.provider', NULL, NULL, NULL,
           (SELECT count(*) FROM src),
           (SELECT count(*) FROM ok WHERE channel = 'status'),
           (SELECT count(*) FROM ok WHERE channel = 'actual'),
           (SELECT count(*) FROM ok WHERE channel = 'artifact'),
           (SELECT count(*) FROM judged WHERE reject_reason IS NOT NULL)
      FROM batch b
    ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
    RETURNING 1
)
SELECT 'channel', channel, count(*) FROM ok GROUP BY channel
UNION ALL SELECT 'table', 'lidar_status', (SELECT count(*) FROM ins_status)
UNION ALL SELECT 'table', 'lidar_scan_actual', (SELECT count(*) FROM ins_actual)
UNION ALL SELECT 'table', 'lidar_scan_artifact', (SELECT count(*) FROM ins_artifact)
UNION ALL SELECT 'reject', split_part(reject_reason, ':', 1), count(*) FROM judged WHERE reject_reason IS NOT NULL GROUP BY 2
$fn$;
