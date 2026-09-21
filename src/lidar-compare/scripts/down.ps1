# 비교 스택 정지. A 경로(src/timescaledb)와 ISL(Aspire)은 건드리지 않는다.
#   -Volumes : B DB · Prometheus · Grafana · EMQX 볼륨까지 지운다
#   -All     : A 경로 스택도 같이 내린다 (그 스택의 down.ps1)
param([switch]$Volumes, [switch]$All)
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

$pidFile = Join-Path $root "isl-metrics\isl-process-metrics.pid"
if (Test-Path $pidFile) {
  $old = Get-Content $pidFile -ErrorAction SilentlyContinue
  if ($old) { Stop-Process -Id $old -Force -ErrorAction SilentlyContinue }
  Remove-Item $pidFile -Force
}

$args2 = @("compose", "-f", (Join-Path $root "docker-compose.yml"), "down")
if ($Volumes) { $args2 += "-v" }
& $engine @args2

if ($All) {
  $aDown = Join-Path $root "..\timescaledb\scripts\down.ps1"
  if ($Volumes) { & $aDown -v } else { & $aDown }
}
