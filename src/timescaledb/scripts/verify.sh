#!/usr/bin/env bash
# 적재가 실제로 되고 있는지 한 번에 본다.
#   1) Kafka 토픽 끝 오프셋 vs DB 가 받은 마지막 오프셋  → 소비 지연
#   2) 하이퍼테이블 행 수 · 청크 · 장비 상태 분포 · 신선도 · 격리 건수
#   3) 소비자 지표 요약 (/metrics)
set -euo pipefail
engine=docker
command -v docker >/dev/null 2>&1 || engine=podman
# -i 가 없으면 heredoc 이 컨테이너 안 psql 에 전달되지 않는다.
psql() { $engine exec -i tsdb-lidar-pg psql -U postgres -d lidar -X -q "$@"; }

echo "── Kafka 오프셋 ──────────────────────────────────"
kafka_ctr="$($engine ps --format '{{.Names}}' | grep -E '^kafka-[a-z0-9]+$' | head -1 || true)"
if [ -n "$kafka_ctr" ]; then
  $engine exec "$kafka_ctr" kafka-get-offsets --bootstrap-server localhost:9092 --topic ot-lidar-status 2>/dev/null \
    | sed 's/^/  topic end   : /'
else
  echo "  (Kafka 컨테이너를 못 찾았다)"
fi
psql -tAc "SELECT '  db last     : ' || COALESCE(max(kafka_offset)::text, '-') || '  (' || count(*) || ' records)' FROM lidar_status_message"

echo "── TimescaleDB ───────────────────────────────────"
psql <<'SQL'
SELECT 'lidar_status rows (approx)' AS item, approximate_row_count('lidar_status')::text AS value
UNION ALL SELECT 'chunks', count(*)::text FROM timescaledb_information.chunks WHERE hypertable_name = 'lidar_status'
UNION ALL SELECT 'hypertable size', pg_size_pretty(hypertable_size('lidar_status'))
UNION ALL SELECT 'newest event', max(last_event_at)::text FROM lidar_device_state
UNION ALL SELECT 'freshness', (now() - max(last_event_at))::text FROM lidar_device_state
UNION ALL SELECT 'devices', count(*)::text FROM lidar_device_state
UNION ALL SELECT 'rejects', count(*)::text FROM lidar_ingest_reject;
SELECT status, count(*) AS devices FROM lidar_device_state GROUP BY status ORDER BY status;
SELECT error_code, count(*) AS devices FROM lidar_device_state WHERE error_code IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;
SELECT j.job_id, j.proc_name, j.hypertable_name, s.last_run_status, s.total_runs, s.total_failures, s.next_start
FROM timescaledb_information.jobs j JOIN timescaledb_information.job_stats s USING (job_id)
WHERE j.job_id >= 1000 ORDER BY j.job_id;
SQL

echo "── lidar-ingest /metrics ─────────────────────────"
curl -s http://localhost:59080/metrics | grep -E '^lidar_ingest_(messages_total|rows_inserted_total|rows_duplicate_total|batches_total|db_errors_total|kafka_lag)' | sed 's/^/  /'
