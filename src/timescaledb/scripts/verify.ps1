# 적재 확인 (PowerShell 판). 동작은 verify.sh 와 같다.
$ErrorActionPreference = "Continue"
Set-Location (Join-Path $PSScriptRoot "..")
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

# 구독 토픽은 kafka.env 가 정한다. 쉼표로 여럿일 수 있다.
$topics = "ot.lidar.status,ot.lidar.actual,ot.lidar.artifact"
if (Test-Path kafka.env) {
  $line = Select-String -Path kafka.env -Pattern '^KAFKA_TOPIC=' | Select-Object -First 1
  if ($line) { $topics = ($line.Line -split '=', 2)[1] }
}

Write-Host "── Kafka 오프셋 ──────────────────────────────────"
$kafkaCtr = & $engine ps --format '{{.Names}}' | Where-Object { $_ -match '^kafka-[a-z0-9]+$' } | Select-Object -First 1
if ($kafkaCtr) {
  foreach ($topic in ($topics -split ',')) {
    & $engine exec $kafkaCtr kafka-get-offsets --bootstrap-server localhost:9092 --topic $topic.Trim() | ForEach-Object { "  topic end   : $_" }
  }
} else {
  Write-Host "  (Kafka 컨테이너를 못 찾았다)"
}
& $engine exec tsdb-lidar-pg psql -U postgres -d lidar -X -q -tAc "SELECT '  db last     : ' || COALESCE(max(kafka_offset)::text, '-') || '  (' || count(*) || ' records)' FROM lidar_status_message"

Write-Host "── TimescaleDB ───────────────────────────────────"
$sql = @'
SELECT 'lidar_status rows (approx)'        AS item, approximate_row_count('lidar_status')::text        AS value
UNION ALL SELECT 'lidar_scan_actual rows',   approximate_row_count('lidar_scan_actual')::text
UNION ALL SELECT 'lidar_scan_artifact rows', approximate_row_count('lidar_scan_artifact')::text
UNION ALL SELECT 'chunks (3 hypertables)',
       (SELECT count(*)::text FROM timescaledb_information.chunks
         WHERE hypertable_name IN ('lidar_status', 'lidar_scan_actual', 'lidar_scan_artifact'))
UNION ALL SELECT 'hypertable size',
       pg_size_pretty(hypertable_size('lidar_status') + hypertable_size('lidar_scan_actual')
                      + hypertable_size('lidar_scan_artifact'))
UNION ALL SELECT 'newest event', (SELECT max(last_event_at)::text FROM lidar_device_state)
UNION ALL SELECT 'freshness',    (SELECT (now() - max(last_event_at))::text FROM lidar_device_state)
UNION ALL SELECT 'devices',      (SELECT count(*)::text FROM lidar_device_state)
UNION ALL SELECT 'rejects',      (SELECT count(*)::text FROM lidar_ingest_reject)
UNION ALL SELECT 'tag catalog',  (SELECT count(*)::text FROM lidar_tag_catalog)
-- 카탈로그가 못 푼 장비는 tid 가 '#숫자' 로 남는다. 0 이 아니면 등록부를 다시 내보내야 한다.
UNION ALL SELECT 'unresolved devices',
       (SELECT count(*)::text FROM lidar_device_state WHERE tid LIKE '#%');

-- 토픽 하나 = 채널 하나다. 어느 토픽이 실제로 실려 오는지, 채널 수가 토픽과 맞는지.
SELECT send_topic, count(*) AS records, sum(item_count) AS items,
       sum(status_count) AS status, sum(actual_count) AS actual,
       sum(artifact_count) AS artifact, sum(reject_count) AS rejects
FROM lidar_status_message GROUP BY send_topic ORDER BY send_topic;

SELECT status, count(*) AS devices FROM lidar_device_state GROUP BY status ORDER BY status;
SELECT error_code, count(*) AS devices FROM lidar_device_state WHERE error_code IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;

SELECT artifact_type, count(*) AS rows_5m, count(DISTINCT scan_id) AS scans,
       pg_size_pretty(sum(file_size_bytes)) AS bytes
FROM lidar_scan_artifact WHERE time > now() - INTERVAL '5 minutes'
GROUP BY 1 ORDER BY 1;

SELECT j.job_id, j.proc_name, j.hypertable_name, s.last_run_status, s.total_runs, s.total_failures, s.next_start
FROM timescaledb_information.jobs j JOIN timescaledb_information.job_stats s USING (job_id)
WHERE j.job_id >= 1000 ORDER BY j.job_id;
'@
& $engine exec tsdb-lidar-pg psql -U postgres -d lidar -X -q -c $sql

Write-Host "── lidar-ingest /metrics ─────────────────────────"
try {
  (Invoke-WebRequest -UseBasicParsing http://localhost:59080/metrics).Content -split "`n" |
    Where-Object { $_ -match '^lidar_ingest_(messages_total|rows_inserted_total|rows_duplicate_total|table_rows_total|batches_total|db_errors_total|kafka_lag|tag_catalog_rows|tag_unresolved_total|tag_channel_mismatch_total)' } |
    ForEach-Object { "  $_" }
} catch { Write-Host "  (metrics 응답 없음: $_)" }
