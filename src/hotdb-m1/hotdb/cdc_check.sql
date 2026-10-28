-- CDC 동작 확인 — ./hotdb.sh cdc-check
-- 논리 슬롯 2개(pgoutput)를 만들고, 실제 쓰는 계정으로 tsdb 하이퍼테이블 · hot.p_progress 에 시험 행을 넣은 뒤
-- 각 publication 으로 어떤 표의 변경이 나오는지 본다. 끝나면 슬롯과 시험 행을 지운다 (다른 데이터는 건드리지 않음).
--   기대: hot_cdc_pub  → p_progress
--         tsdb_cdc_pub → _hyper_N_M_chunk (tsdb 하이퍼테이블 청크) · device · tag_catalog
\set ON_ERROR_STOP on
\set QUIET on
\pset footer off

SELECT 'slot' AS step, pg_create_logical_replication_slot('cdc_check_hot', 'pgoutput') IS NOT NULL AS ok
UNION ALL
SELECT 'slot', pg_create_logical_replication_slot('cdc_check_tsdb', 'pgoutput') IS NOT NULL;

-- tsdb 쓰기는 Provider 계정, p_progress 쓰기는 판별 모듈 계정으로 (권한도 함께 확인)
SET ROLE hotdb_provider;
INSERT INTO tsdb.device (site, device_id, device_role, zone) VALUES ('CDC-CHECK', 'CDC-CHECK-1', 'LIDAR', 'mch');
INSERT INTO tsdb.tag_catalog (edge_group_id, tag_id, mqtt_topic, source_id, site, device_id, channel)
VALUES ('CDC-CHECK', 'CDC-CHECK-1', 'cdc/check', 'cdc-check', 'CDC-CHECK', 'CDC-CHECK-1', 'status');
INSERT INTO tsdb.status_history (event_time, tag_key, status, source_time_text, received_at)
SELECT now(), tag_key, 'ONLINE', 'cdc-check', now() FROM tsdb.tag_catalog WHERE edge_group_id = 'CDC-CHECK';
RESET ROLE;
INSERT INTO hot.l_work_unit (unit_id, level) VALUES ('CDC-CHECK', 'DWG');
SET ROLE svc_mch;
INSERT INTO hot.p_progress (anchor_id, as_of, state) VALUES ('CDC-CHECK', current_date, 'inProgress');
RESET ROLE;

\echo '== publication 별로 나온 변경'
WITH c AS (
  SELECT 'hot_cdc_pub' AS pub, data FROM pg_logical_slot_peek_binary_changes('cdc_check_hot', NULL, NULL, 'proto_version', '1', 'publication_names', 'hot_cdc_pub')
  UNION ALL
  SELECT 'tsdb_cdc_pub', data FROM pg_logical_slot_peek_binary_changes('cdc_check_tsdb', NULL, NULL, 'proto_version', '1', 'publication_names', 'tsdb_cdc_pub')
)
SELECT pub, string_agg(DISTINCT m[1], ', ') AS tables
FROM c, regexp_matches(encode(c.data, 'escape'), '(p_progress|l_work_unit|_hyper_[0-9]+_[0-9]+_chunk|status_history|tag_catalog|device)', 'g') m
GROUP BY pub ORDER BY pub;

-- 정리
SELECT pg_drop_replication_slot('cdc_check_hot'), pg_drop_replication_slot('cdc_check_tsdb') \g /dev/null
DELETE FROM hot.p_progress WHERE anchor_id = 'CDC-CHECK';
DELETE FROM hot.l_work_unit WHERE unit_id = 'CDC-CHECK';
DELETE FROM tsdb.status_history WHERE tag_key IN (SELECT tag_key FROM tsdb.tag_catalog WHERE edge_group_id = 'CDC-CHECK');
DELETE FROM tsdb.tag_catalog WHERE edge_group_id = 'CDC-CHECK';
DELETE FROM tsdb.device WHERE site = 'CDC-CHECK';
\echo '시험 슬롯 · 행을 지웠습니다.'
