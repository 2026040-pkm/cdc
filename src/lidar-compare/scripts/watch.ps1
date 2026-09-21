# 세 hot DB 에 행이 쌓이는 속도를 주기적으로 찍는다. Ctrl+C 로 멈춘다.
#
#   .\watch.ps1                 # 10초마다
#   .\watch.ps1 -Interval 30    # 30초마다
#
# 창은 적재 시각(received_at = 소비자 트랜잭션의 now()) 기준이다 — "DB 에 언제 들어왔나".
# 트랜잭션 시작 시각이 찍히고 커밋은 그 뒤라, 막 커밋 중인 배치를 놓치지 않게 -Lag 초만큼 뒤를 본다.
# verify.ps1 은 이벤트 시각(time) 기준이라 "발행한 것 중 몇 건이 왔나" 를 본다. 둘은 질문이 다르다.
param([int]$Interval = 10, [int]$Lag = 5, [int]$Count = 0)  # -Count 0 = 멈출 때까지
$ErrorActionPreference = "Continue"
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

# 발행기 기본값(350대 · 상태 1초 · 스캔 1분에 실적 1 + 산출물 12) 기준 초당 기대 행
$expected = @{ status = 350.0; actual = 350 / 60.0; artifact = 350 * 12 / 60.0 }
$expectedSum = $expected.status + $expected.actual + $expected.artifact
$paths = @(@{ name = "A isl-kafka"; ctr = "tsdb-lidar-pg" }, @{ name = "B mqtt-direct"; ctr = "cmp-direct-pg" }, @{ name = "C isl-db"; ctr = "cmp-provider-pg" })

function Invoke-Sql($ctr, $sql) {
  $out = & $engine exec $ctr psql -U postgres -d lidar -At -F "|" -c $sql 2>$null
  if ($LASTEXITCODE -ne 0 -or -not $out) { return $null }
  return @($out)[0] -split "\|"
}

# 시작점: 경로마다 DB 시계로 커서를 잡고 전체 행 수를 한 번 센다
foreach ($p in $paths) {
  $c = Invoke-Sql $p.ctr @"
SELECT now() - interval '$Lag s',
       (SELECT count(*) FROM tsdb.lidar_status) + (SELECT count(*) FROM tsdb.lidar_scan_actual) + (SELECT count(*) FROM tsdb.lidar_scan_artifact)
"@
  if (-not $c) { Write-Host "$($p.name): $($p.ctr) 에 붙지 못했다"; exit 1 }
  $p.cursor = $c[0]; $p.base = [long]$c[1]; $p.since = 0L; $p.started = Get-Date
}

Write-Host ("{0}초마다 · 적재 시각 기준(-{1}초) · 기대 {2:N1} 행/초 (status {3:N0} + actual {4:N1} + artifact {5:N0})" -f `
  $Interval, $Lag, $expectedSum, $expected.status, $expected.actual, $expected.artifact)

for ($n = 1; $Count -le 0 -or $n -le $Count; $n++) {
  Start-Sleep -Seconds $Interval
  $sim = & $engine logs --tail 1 cmp-lidar-sim 2>$null | Select-Object -Last 1
  Write-Host ""
  Write-Host ("[{0:HH:mm:ss}] 발행기  {1}" -f (Get-Date), ($(if ($sim) { ($sim -replace '^\[[^\]]*\]\s*', '') } else { "(로그 없음)" })))
  Write-Host ("  {0,-13} {1,9} {2,8} {3,10} {4,9} {5,7} {6,12} {7,13} {8,9}" -f "경로", "status/s", "actual/s", "artifact/s", "합계/s", "기대비", "시작후 누적", "전체 행", "DB 크기")
  foreach ($p in $paths) {
    # time 조건은 청크를 거르려는 것이다. 이벤트가 1시간 넘게 늦게 들어오면 빠진다.
    $c = Invoke-Sql $p.ctr @"
WITH w AS (SELECT '$($p.cursor)'::timestamptz AS f, now() - interval '$Lag s' AS t)
SELECT w.t, extract(epoch FROM w.t - w.f),
  (SELECT count(*) FROM tsdb.lidar_status        s WHERE s.time > w.f - interval '1 hour' AND s.received_at > w.f AND s.received_at <= w.t),
  (SELECT count(*) FROM tsdb.lidar_scan_actual   s WHERE s.time > w.f - interval '1 hour' AND s.received_at > w.f AND s.received_at <= w.t),
  (SELECT count(*) FROM tsdb.lidar_scan_artifact s WHERE s.time > w.f - interval '1 hour' AND s.received_at > w.f AND s.received_at <= w.t),
  pg_size_pretty(pg_database_size('lidar'))
FROM w
"@
    if (-not $c) { Write-Host ("  {0,-13} (조회 실패)" -f $p.name); continue }
    $secs = [double]$c[1]
    $st = [long]$c[2]; $ac = [long]$c[3]; $ar = [long]$c[4]; $sum = $st + $ac + $ar
    $p.cursor = $c[0]; $p.since += $sum
    Write-Host ("  {0,-13} {1,9:N1} {2,8:N2} {3,10:N1} {4,9:N1} {5,7:P1} {6,12:N0} {7,13:N0} {8,9}" -f `
      $p.name, ($st / $secs), ($ac / $secs), ($ar / $secs), ($sum / $secs), (($sum / $secs) / $expectedSum), $p.since, ($p.base + $p.since), $c[5])
  }
}
