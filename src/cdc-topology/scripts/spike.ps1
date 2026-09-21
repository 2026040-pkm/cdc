# 스파이크 DB 를 띄우고 spike/*.sql 을 돌려 results/ 에 남긴다.
#
#   .\spike.ps1        # 기동 → 실행 → 저장 → 정지 (데이터는 tmpfs 라 남지 않는다)
#   .\spike.ps1 -Keep  # 컨테이너를 남긴다. psql -h localhost -p 64432 -U postgres spike
#
# Windows PowerShell 5.1 은 네이티브 명령의 stderr 를 오류로 바꾼다. 판정은 $LASTEXITCODE 로 한다.
param([switch]$Keep)
$ErrorActionPreference = "Continue"
$env:PODMAN_COMPOSE_WARNING_LOGS = "false"
$root = Split-Path -Parent $PSScriptRoot
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }
$compose = Join-Path $root "docker-compose.yml"
$results = Join-Path $root "results"
New-Item -ItemType Directory -Force $results | Out-Null

& $engine compose -f $compose up -d 2>&1 | Out-Null
for ($i = 0; $i -lt 60; $i++) {
  if ((& $engine inspect topo-spike-pg --format '{{.State.Health.Status}}') -eq "healthy") { break }
  Start-Sleep -Seconds 2
}

$stamp = Get-Date -Format "yyyyMMdd-HHmm"
foreach ($sql in Get-ChildItem (Join-Path $root "spike") -Filter "*.sql") {
  $out = Join-Path $results "$($sql.BaseName)-$stamp.txt"
  # 매번 빈 DB 에서 시작한다 — 이전 실행의 슬롯·표가 남아 있으면 이벤트 수가 섞인다
  & $engine exec topo-spike-pg psql -U postgres -c "DROP DATABASE IF EXISTS spike_run" -c "CREATE DATABASE spike_run" 2>&1 | Out-Null
  & $engine exec topo-spike-pg psql -U postgres -d spike_run -c "CREATE EXTENSION IF NOT EXISTS timescaledb" 2>&1 | Out-Null
  $text = Get-Content -Raw -Encoding UTF8 $sql.FullName
  $text | & $engine exec -i topo-spike-pg psql -U postgres -d spike_run -X 2>&1 | Out-File -Encoding utf8 $out
  Write-Host "$($sql.Name) → $out (exit $LASTEXITCODE)"
  # 슬롯이 남으면 WAL 을 붙잡는다. 스크립트 중간에 실패했을 때를 위해 지운다
  & $engine exec topo-spike-pg psql -U postgres -d spike_run -Atc "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots" 2>&1 | Out-Null
}

if (-not $Keep) { & $engine compose -f $compose down 2>&1 | Out-Null }
