# 세 hot DB 를 같은 질문으로 나란히 찍는다.
#
#   .\verify.ps1              # 직전 5분(마지막 1분은 아직 차는 중이라 뺀다)
#   .\verify.ps1 -Minutes 15
#
# 창은 이벤트 시각(time = raw_payload.occurred_at) 기준이다. 두 DB 가 같은 발행기 입력을 받으므로
# 같은 창의 행 수가 같아야 한다. 차이는 경로가 버리거나(합쳐지거나) 늦은 것이다. (A · B · C)
param([int]$Minutes = 5)
$ErrorActionPreference = "Continue"
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

$from = "date_trunc('minute', now()) - interval '$($Minutes + 1) minutes'"
$to = "date_trunc('minute', now()) - interval '1 minute'"
$sql = @"
WITH w AS (SELECT $from AS f, $to AS t)
SELECT 'status'   AS ch, count(*) AS rows, count(DISTINCT tid) AS devices,
       count(*) FILTER (WHERE tid LIKE '#%') AS unresolved,
       round(avg(extract(epoch FROM received_at - time))::numeric, 3) AS avg_lag_s,
       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - time)))::numeric, 3) AS p95_lag_s
  FROM tsdb.lidar_status, w WHERE time >= w.f AND time < w.t
UNION ALL
SELECT 'actual', count(*), count(DISTINCT tid), count(*) FILTER (WHERE tid LIKE '#%'),
       round(avg(extract(epoch FROM received_at - time))::numeric, 3),
       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - time)))::numeric, 3)
  FROM tsdb.lidar_scan_actual, w WHERE time >= w.f AND time < w.t
UNION ALL
SELECT 'artifact', count(*), count(DISTINCT tid), count(*) FILTER (WHERE tid LIKE '#%'),
       round(avg(extract(epoch FROM received_at - time))::numeric, 3),
       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - time)))::numeric, 3)
  FROM tsdb.lidar_scan_artifact, w WHERE time >= w.f AND time < w.t;
"@

$expected = @{ status = 350 * 60 * $Minutes; actual = 350 * $Minutes; artifact = 350 * 12 * $Minutes }
$result = @{}
foreach ($p in @(@{ name = "A isl-kafka"; ctr = "tsdb-lidar-pg" }, @{ name = "B mqtt-direct"; ctr = "cmp-direct-pg" }, @{ name = "C isl-db"; ctr = "cmp-provider-pg" })) {
  $lines = & $engine exec $p.ctr psql -U postgres -d lidar -At -F "|" -c $sql 2>$null
  foreach ($line in $lines) {
    $c = $line -split "\|"
    if ($c.Count -lt 6) { continue }
    $result["$($p.name)|$($c[0])"] = $c
  }
}

Write-Host ("창: 직전 {0}분 (이벤트 시각 기준, 마지막 1분 제외). 기대값은 발행기 기본값(350대) 기준" -f $Minutes)
Write-Host ""
Write-Host ("{0,-9} {1,-13} {2,9} {3,8} {4,8} {5,6} {6,10} {7,10}" -f "채널", "경로", "행", "기대", "도달률", "장비", "지연평균", "지연p95")
foreach ($ch in "status", "actual", "artifact") {
  foreach ($name in "A isl-kafka", "B mqtt-direct", "C isl-db") {
    $c = $result["$name|$ch"]
    if (-not $c) { Write-Host ("{0,-9} {1,-13} (조회 실패)" -f $ch, $name); continue }
    $rate = if ($expected[$ch] -gt 0) { "{0:P1}" -f ([double]$c[1] / $expected[$ch]) } else { "-" }
    $unres = if ([int]$c[3] -gt 0) { " (#미해석 $($c[3]))" } else { "" }
    Write-Host ("{0,-9} {1,-13} {2,9} {3,8} {4,8} {5,6} {6,9}s {7,9}s{8}" -f $ch, $name, $c[1], $expected[$ch], $rate, $c[2], $c[4], $c[5], $unres)
  }
}

Write-Host ""
Write-Host "소비자 지표 (Prometheus, 1분 평균)"
foreach ($q in @(
    @{ t = "적재 행/초"; e = 'cmp:rows_inserted:rate1m' },
    @{ t = "e2e p95(s)"; e = 'cmp:e2e_seconds:p95' },
    @{ t = "소스 대기"; e = 'max by (pipeline) (lidar_ingest_source_lag)' },
    @{ t = "격리/분"; e = 'sum by (pipeline) (rate(lidar_ingest_rejects_total[5m])) * 60' })) {
  try {
    $r = Invoke-RestMethod -Uri ("http://localhost:63090/api/v1/query?query=" + [uri]::EscapeDataString($q.e)) -TimeoutSec 5
    $vals = ($r.data.result | ForEach-Object { "{0}={1:N2}" -f $_.metric.pipeline, [double]$_.value[1] }) -join "  "
    Write-Host ("  {0,-10} {1}" -f $q.t, $vals)
  } catch { Write-Host ("  {0,-10} (Prometheus 조회 실패)" -f $q.t) }
}
