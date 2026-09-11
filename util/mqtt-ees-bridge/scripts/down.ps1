# 스택 정지 (PowerShell 판). -Volumes 면 볼륨(Kafka 로그·자동 태그 상태)까지 지운다.
param([switch]$Volumes)

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot

$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

# tsdb-net 에 붙여 뒀으면 먼저 뗀다 - 안 그러면 네트워크가 남아 다음 기동에서 꼬인다.
& $engine network disconnect tsdb-net ees-kafka 2>$null | Out-Null
if ($?) { Write-Host "ees-kafka 를 tsdb-net 에서 뗐다" }

if ($Volumes) {
  & $engine compose -f (Join-Path $root "docker-compose.yml") down -v
  Write-Host "볼륨까지 삭제했다 (Kafka 로그·자동 태그 번호가 사라진다)"
} else {
  & $engine compose -f (Join-Path $root "docker-compose.yml") down
}
