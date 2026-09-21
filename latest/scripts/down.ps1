# latest 스택 전체 정지 — 기동의 역순이다.
# 수신(CDC)을 먼저 내려야 원천 네트워크(tsdb-net)를 지울 수 있다.
#
#   .\down.ps1       데이터 유지
#   .\down.ps1 -v    볼륨까지 삭제 (init SQL 재실행. CDC 슬롯·오프셋도 같이 정리된다)
#
# Windows PowerShell 5.1 의 stderr 처리 때문에 Stop 을 쓰지 않는다 (up.ps1 주석 참고).
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot

Write-Host "── 1/2 · embedded-cdc-tsdb ───────────────────────"
& "$root\embedded-cdc-tsdb\scripts\down.ps1" @args

Write-Host ""
Write-Host "── 2/2 · timescaledb ─────────────────────────────"
& "$root\timescaledb\scripts\down.ps1" @args
