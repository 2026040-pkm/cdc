# 정지 (PowerShell 판). 기본은 데이터 유지, `.\down.ps1 -v` 면 볼륨까지 지운다.
#
# up 이 Kafka 컨테이너를 tsdb-net 에 붙여 두었으므로 먼저 떼어 낸다. 안 떼면
# compose 가 네트워크를 지우지 못해 "network is being used" 로 끝난다.
# Windows PowerShell 5.1 은 네이티브 명령의 stderr(podman compose 안내·빌드 진행 표시)를 오류로 바꿔
# Stop 이면 거기서 멈춘다. 실패 판정은 $LASTEXITCODE 로 한다.
$ErrorActionPreference = "Continue"
$env:PODMAN_COMPOSE_WARNING_LOGS = "false"
$root = Split-Path -Parent $PSScriptRoot
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

& $engine network inspect tsdb-net *> $null
if ($LASTEXITCODE -eq 0) {
  $members = (& $engine network inspect tsdb-net --format '{{range .Containers}}{{.Name}} {{end}}') -split '\s+' | Where-Object { $_ }
  foreach ($ctr in $members) {
    if ($ctr -notlike 'tsdb-*') {
      & $engine network disconnect tsdb-net $ctr
      Write-Host "tsdb-net 에서 분리: $ctr"
    }
  }
}

& $engine compose -f (Join-Path $root "docker-compose.yml") down @args
