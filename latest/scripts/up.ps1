# latest 스택 전체 기동 — 순서가 있다.
#
#   1) timescaledb        원천. 네트워크 tsdb-net 을 만들고 Kafka 를 거기 붙인다
#   2) embedded-cdc-tsdb  그 tsdb-net 에 external 로 붙는다. 1 이 없으면 기동 자체가 막힌다
#
# 두 스택을 compose 하나로 합치지 않은 이유는 ..\README.md 를 볼 것.
#
# Windows PowerShell 5.1 은 네이티브 명령의 stderr(podman compose 안내·빌드 진행 표시)를
# 오류로 바꿔 Stop 이면 거기서 멈춘다. 실패 판정은 $LASTEXITCODE 로 한다.
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot

Write-Host "── 1/2 · timescaledb (원천) ──────────────────────"
& "$root\timescaledb\scripts\up.ps1"
if ($LASTEXITCODE -ne 0) { Write-Error "timescaledb 기동 실패 ($LASTEXITCODE). 여기서 멈춘다."; exit $LASTEXITCODE }

Write-Host ""
Write-Host "── 2/2 · embedded-cdc-tsdb (CDC) ─────────────────"
& "$root\embedded-cdc-tsdb\scripts\up.ps1"
if ($LASTEXITCODE -ne 0) { Write-Error "embedded-cdc-tsdb 기동 실패 ($LASTEXITCODE)."; exit $LASTEXITCODE }
