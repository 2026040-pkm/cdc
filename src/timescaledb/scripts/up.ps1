# 전체 스택 기동 (PowerShell 판). 동작은 up.sh 와 같다.
#
# Kafka 는 이 스택 밖(Aspire 세션)에 있다. 소비자를 Aspire 네트워크에 넣으면 Aspire 가
# 5초 안에 떼어 내므로, 반대로 Kafka 컨테이너를 이쪽 네트워크(tsdb-net)에 alias "kafka" 로
# 붙인다. 브로커가 광고하는 내부 주소가 kafka:9093 이라 alias 이름은 반드시 kafka 여야 한다.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

# kafka.env 의 KAFKA_CONTAINER (비어 있으면 자동 탐지)
$kafkaCtr = $env:KAFKA_CONTAINER
if (-not $kafkaCtr) {
  $line = Get-Content (Join-Path $root "kafka.env") | Where-Object { $_ -match '^KAFKA_CONTAINER=' } | Select-Object -First 1
  if ($line) { $kafkaCtr = ($line -replace '^KAFKA_CONTAINER=', '').Trim() }
}
if (-not $kafkaCtr) {
  $kafkaCtr = & $engine ps --format '{{.Names}}' | Where-Object { $_ -match '^kafka-[a-z0-9]+$' } | Select-Object -First 1
}

# 1) 기동
& $engine compose -f (Join-Path $root "docker-compose.yml") up -d --build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# 2) Kafka 컨테이너를 tsdb-net 에 붙인다 (이미 붙어 있으면 건너뛴다)
if ($kafkaCtr) {
  $members = & $engine network inspect tsdb-net --format '{{range .Containers}}{{.Name}} {{end}}'
  if ($members -split '\s+' -contains $kafkaCtr) {
    Write-Host "Kafka 컨테이너 $kafkaCtr 는 이미 tsdb-net 에 있다"
  } else {
    & $engine network connect tsdb-net $kafkaCtr --alias kafka
    Write-Host "Kafka 컨테이너 $kafkaCtr → tsdb-net (alias kafka)"
  }
} else {
  Write-Warning "떠 있는 Kafka 컨테이너(kafka-xxxxxxxx)를 못 찾았다. Aspire 를 띄운 뒤 다시 실행하거나 '$engine network connect tsdb-net <컨테이너> --alias kafka' 를 손으로 실행하라."
}

Write-Host ""
Write-Host "── 기동 완료 ─────────────────────────────────────"
Write-Host "  TimescaleDB  : localhost:59432 (lidar / postgres:postgres)"
Write-Host "  lidar-ingest : http://localhost:59080/metrics"
Write-Host "  Prometheus   : http://localhost:59090"
Write-Host "  Grafana      : http://localhost:59380  (admin/admin)"
Write-Host "  Kafka        : $kafkaCtr (kafka:9093 on tsdb-net)"
