# 스택 기동 (PowerShell 판). 동작은 up.sh 와 같다.
#
#   scripts\up.ps1          # 기동만 한다
#   scripts\up.ps1 -Tsdb    # 수신 측(src/timescaledb)이 이 Kafka 를 읽도록 tsdb-net 에 붙인다
#
# -Tsdb 는 tsdb-net 에 이미 붙어 있는 Aspire Kafka(kafka-xxxxxxxx)를 떼고 이쪽을 alias
# kafka 로 붙인다. 같은 네트워크에 alias 가 둘이면 소비자가 어느 쪽에 붙을지 알 수 없다.
param([switch]$Tsdb)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

& $engine compose -f (Join-Path $root "docker-compose.yml") up -d --build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($Tsdb) {
  $members = & $engine network inspect tsdb-net --format '{{range .Containers}}{{.Name}} {{end}}' 2>$null
  if (-not $members) {
    Write-Warning "tsdb-net 이 없다. src/timescaledb 스택을 먼저 올려라."
  } else {
    $names = $members -split '\s+' | Where-Object { $_ }
    foreach ($ctr in $names) {
      if ($ctr -match '^kafka-[a-z0-9]+$') {
        & $engine network disconnect tsdb-net $ctr
        Write-Host "Aspire Kafka $ctr 를 tsdb-net 에서 뗐다"
      }
    }
    if ($names -contains "ees-kafka") {
      Write-Host "ees-kafka 는 이미 tsdb-net 에 있다"
    } else {
      & $engine network connect tsdb-net ees-kafka --alias kafka
      Write-Host "ees-kafka -> tsdb-net (alias kafka)"
    }
    & $engine restart tsdb-lidar-ingest 2>$null | Out-Null
    if ($?) { Write-Host "tsdb-lidar-ingest 재기동 (새 브로커로 붙는다)" }
  }
}

$mqttHost = if ($env:MQTT_HOST) { $env:MQTT_HOST } else { "host.docker.internal" }
$mqttPort = if ($env:MQTT_PORT) { $env:MQTT_PORT } else { "1884" }

Write-Host ""
Write-Host "-- 기동 완료 -------------------------------------"
Write-Host "  Kafka       : localhost:61092   (컨테이너끼리는 kafka:9093)"
Write-Host "  Kafka UI    : http://localhost:61080"
Write-Host "  브리지 지표 : http://localhost:61090/metrics"
Write-Host "  MQTT 원천   : ${mqttHost}:${mqttPort} (EMQX)"
Write-Host ""
Write-Host "  발행기를 돌려야 데이터가 흐른다 - util\mqtt-lidar-sim\1-publish.cmd"
