# 비교 스택 기동 — A(isl-kafka) · B(mqtt-direct) · C(isl-db) 세 경로를 한 번에 올린다.
#
# 전제: A 경로를 보려면 ISL 이 떠 있어야 한다 — Aspire(Kafka 컨테이너) + scripts/isl-services.ps1.
#       B 경로는 ISL 과 무관하다(EMQX 를 직접 구독).
#
# 하는 일
#   1) A 경로 스택(src/timescaledb) 기동 + Kafka 컨테이너를 tsdb-net 에 붙인다 (그 스택의 up.ps1)
#   2) 이 스택 기동 (EMQX · 발행기 · B 소비자 · B DB · C DB · 비교 모니터링)
#      C 경로의 쓰는 쪽(ISL Db.Provider)은 호스트 프로세스라 scripts/isl-services.ps1 이 띄운다
#   3) ISL 호스트 프로세스 지표 수집기(isl-process-metrics.ps1)를 숨은 창으로 띄운다
# Windows PowerShell 5.1 은 네이티브 명령의 stderr(podman compose 안내·빌드 진행 표시)를 오류로 바꿔
# Stop 이면 거기서 멈춘다. 실패 판정은 $LASTEXITCODE 로 한다.
$ErrorActionPreference = "Continue"
$env:PODMAN_COMPOSE_WARNING_LOGS = "false"
$root = Split-Path -Parent $PSScriptRoot
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

# ── 0) Aspire 컨테이너 확인 ───────────────────────────────────────────────
$kafkaCtr = & $engine ps --format '{{.Names}}' | Where-Object { $_ -match '^kafka-[a-z0-9]+$' } | Select-Object -First 1
if (-not $kafkaCtr) {
  Write-Warning "Aspire 의 Kafka 컨테이너가 없다. A 경로(isl-kafka)는 붙을 곳이 없다 — Aspire 를 띄운 뒤 다시 실행하라. B 경로는 그대로 돈다."
}

# ── 1) A 경로 ────────────────────────────────────────────────────────────
& (Join-Path $root "..\timescaledb\scripts\up.ps1")

# ── 2) 이 스택 ───────────────────────────────────────────────────────────
& $engine compose -f (Join-Path $root "docker-compose.yml") up -d --build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# ── 3) ISL 호스트 프로세스 지표 ─────────────────────────────────────────
$pidFile = Join-Path $root "isl-metrics\isl-process-metrics.pid"
$running = $false
if (Test-Path $pidFile) {
  $old = Get-Content $pidFile -ErrorAction SilentlyContinue
  if ($old -and (Get-Process -Id $old -ErrorAction SilentlyContinue)) { $running = $true }
}
if (-not $running) {
  Start-Process -FilePath "powershell" -WindowStyle Hidden -ArgumentList @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "isl-process-metrics.ps1"))
  Write-Host "ISL 프로세스 지표 수집기 기동"
}

Write-Host ""
Write-Host "── 기동 완료 ─────────────────────────────────────"
Write-Host "  비교 Grafana   : http://localhost:63380  (admin/admin)"
Write-Host "  비교 Prometheus: http://localhost:63090"
Write-Host "  EMQX           : localhost:1884 · 대시보드 http://localhost:18083 (admin/public)"
Write-Host "  B hot DB       : localhost:63432 (lidar / postgres:postgres)"
Write-Host "  C hot DB       : localhost:63433 (lidar / postgres:postgres)"
Write-Host "  A hot DB       : localhost:59432 (lidar / postgres:postgres)"
Write-Host "  B 소비자 지표  : http://localhost:63080/metrics"
Write-Host "  C Provider 지표: isl-metrics\isl-db-provider.prom"
Write-Host "  A 소비자 지표  : http://localhost:59080/metrics"
