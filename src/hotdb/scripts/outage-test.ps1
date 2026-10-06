<#
.SYNOPSIS
  권역 서비스를 멈췄다 다시 띄워, 멈춘 동안 쌓인 변경을 빠짐없이 따라잡는지 본다.

  멈춘 동안: 슬롯이 WAL 을 붙잡아 hotdb_slot_retained_bytes 가 오른다 (Grafana 슬롯 패널)
  다시 띄우면: 슬롯 위치부터 이어 받아 최신값이 현재 시각으로 돌아온다

.EXAMPLE
  .\scripts\outage-test.ps1 -Zone asm -Seconds 60
#>
param([string]$Zone = "asm", [int]$Seconds = 60)
$ErrorActionPreference = "Continue"

function Sql([string]$q) { podman exec hotdb-pg psql -U postgres -d hotdb -Atc $q 2>&1 | ForEach-Object { "$_" } }
$slotQ = "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots WHERE slot_name = 'zone_$Zone'"
$staleQ = "SELECT count(*) FROM svc.device_status_current WHERE module = '$Zone' AND device_id LIKE 'LDR-%' AND src_event_time < now() - interval '10 seconds'"

Write-Host "[$(Get-Date -f T)] hotdb-zone-$Zone 멈춤 ($Seconds 초)"
podman stop "hotdb-zone-$Zone" | Out-Null
Start-Sleep -Seconds $Seconds
Write-Host "[$(Get-Date -f T)] 슬롯 미확인 WAL: $(Sql $slotQ) · 10초 넘게 묵은 장비: $(Sql $staleQ)"

podman start "hotdb-zone-$Zone" | Out-Null
$t0 = Get-Date
do {
    Start-Sleep -Seconds 2
    $stale = [int](Sql $staleQ)
} while ($stale -gt 0 -and ((Get-Date) - $t0).TotalSeconds -lt 180)

$took = [int]((Get-Date) - $t0).TotalSeconds
Write-Host "[$(Get-Date -f T)] 재기동 후 $took 초 · 묵은 장비 $stale · 슬롯 미확인 WAL $(Sql $slotQ)"
Write-Host "dead letter: $(Sql 'SELECT count(*) FROM ops.cdc_dead_letter')"
# 따라잡는 동안 배치가 커진다 — 여기서 반영 실패가 나면 재시도 대기로 다시 밀린다
$failed = @(podman logs --since "$([int]((Get-Date) - $t0).TotalSeconds + 5)s" "hotdb-zone-$Zone" 2>&1 |
    Where-Object { "$_" -match "배치 반영 실패" }).Count
Write-Host "따라잡는 동안 배치 반영 실패: $failed"
if ($stale -gt 0 -or $failed -gt 0) { exit 1 }
