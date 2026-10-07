-- [tsdb 압축 7일 · 보존 90일] V12(보존 7일 · 압축 안 함)를 되돌린다 (2026-10-07 사용자 결정).
-- V2 와 같은 값 — 압축은 7일 뒤(CDC 소비자가 읽기 전에 압축되지 않게), 청크 삭제는 90일 뒤.
SELECT remove_retention_policy('tsdb.status_history',   if_exists => true);
SELECT remove_retention_policy('tsdb.actual_history',   if_exists => true);
SELECT remove_retention_policy('tsdb.artifact_history', if_exists => true);
SELECT remove_compression_policy('tsdb.status_history',   if_exists => true);
SELECT remove_compression_policy('tsdb.actual_history',   if_exists => true);
SELECT remove_compression_policy('tsdb.artifact_history', if_exists => true);

SELECT add_compression_policy('tsdb.status_history',   compress_after => INTERVAL '7 days');
SELECT add_compression_policy('tsdb.actual_history',   compress_after => INTERVAL '7 days');
SELECT add_compression_policy('tsdb.artifact_history', compress_after => INTERVAL '7 days');
SELECT add_retention_policy('tsdb.status_history',   drop_after => INTERVAL '90 days');
SELECT add_retention_policy('tsdb.actual_history',   drop_after => INTERVAL '90 days');
SELECT add_retention_policy('tsdb.artifact_history', drop_after => INTERVAL '90 days');
