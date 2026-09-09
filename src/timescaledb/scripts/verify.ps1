# 적재 확인 (PowerShell 판). 동작은 verify.sh 와 같다.
$ErrorActionPreference = "Continue"
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

Write-Host "── Kafka 오프셋 ──────────────────────────────────"
$kafkaCtr = & $engine ps --format '{{.Names}}' | Where-Object { $_ -match '^kafka-[a-z0-9]+$' } | Select-Object -First 1
if ($kafkaCtr) {
  & $engine exec $kafkaCtr kafka-get-offsets --bootstrap-server localhost:9092 --topic ot-lidar-status | ForEach-Object { "  topic end   : $_" }
} else {
  Write-Host "  (Kafka 컨테이너를 못 찾았다)"
}
& $engine exec tsdb-lidar-pg psql -U postgres -d lidar -X -q -tAc "SELECT '  db last     : ' || COALESCE(max(kafka_offset)::text, '-') || '  (' || count(*) || ' records)' FROM lidar_status_message"

Write-Host "── TimescaleDB ───────────────────────────────────"
$sql = @'
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
'@
& $engine exec tsdb-lidar-pg psql -U postgres -d lidar -X -q -c $sql

Write-Host "── lidar-ingest /metrics ─────────────────────────"
try {
  (Invoke-WebRequest -UseBasicParsing http://localhost:59080/metrics).Content -split "`n" |
    Where-Object { $_ -match '^lidar_ingest_(messages_total|rows_inserted_total|rows_duplicate_total|batches_total|db_errors_total|kafka_lag)' } |
    ForEach-Object { "  $_" }
} catch { Write-Host "  (metrics 응답 없음: $_)" }
