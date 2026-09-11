# 흐르고 있는지 확인한다 (PowerShell 판). 동작은 verify.sh 와 같다.
$ErrorActionPreference = "Continue"

$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

$topics = @("ot.lidar.status", "ot.lidar.actual", "ot.lidar.artifact")

Write-Host "-- Kafka 오프셋 ----------------------------------"
foreach ($t in $topics) {
  & $engine exec ees-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9093 --topic $t |
    ForEach-Object { "  $_" }
}

Write-Host "-- 브리지 지표 -----------------------------------"
try {
  $metrics = (Invoke-WebRequest -UseBasicParsing http://localhost:61090/metrics).Content -split "`n"
  $metrics | Where-Object { $_ -match '^ees_bridge_(mqtt_messages_total|mqtt_drops_total|records_total|items_total|delivered_total|delivery_errors_total|tag_catalog_rows|tag_auto_rows|tag_unregistered_total|mqtt_connected)' } |
    ForEach-Object { "  $_" }
} catch {
  Write-Host "  (브리지 지표를 못 읽었다: $_)"
}

Write-Host "-- 계약 검사 (수신 측 규칙 그대로) ----------------"
& $engine exec ees-bridge python check_contract.py

Write-Host "-- 레코드 한 건 (ot.lidar.status) -----------------"
$one = & $engine exec ees-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic ot.lidar.status --max-messages 1 --timeout-ms 10000 --property print.headers=true
if ($one) {
  $text = ($one -join " ")
  "  " + $text.Substring(0, [Math]::Min(1200, $text.Length))
}

Write-Host "-- 브리지가 직접 번호를 매긴 태그 -----------------"
& $engine exec ees-bridge python -c "import json,pathlib;p=pathlib.Path('/state/auto-tags.json');t=json.loads(p.read_text(encoding='utf-8'))['tags'] if p.exists() else None;print('  none') if t is None else print('  %d tags - /state/auto-tags.sql' % len(t))"
