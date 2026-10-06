<#

.SYNOPSIS

  HotDB 스택을 내린다.



.EXAMPLE

  .\scripts\down.ps1          # 컨테이너만 내림 (데이터 · 슬롯 · 오프셋 유지)

  .\scripts\down.ps1 -Volumes # 볼륨까지 삭제 — DB · 슬롯 · 오프셋 · Prometheus 이력 전부 초기화

#>

param([switch]$Volumes)

$root = Split-Path -Parent $PSScriptRoot

Set-Location $root

$files = @("-f", "compose.db.yml", "-f", "compose.legacy.yml", "-f", "compose.zone.yml", "-f", "compose.rfc.yml", "-f", "compose.agent.yml",

           "-f", "compose.legacy-sim.yml", "-f", "compose.sim.yml", "-f", "compose.monitoring.yml", "-f", "compose.local.yml")

$extra = @()

if ($Volumes) { $extra = @("-v") }

$ErrorActionPreference = "Continue"

podman compose @files down @extra 2>&1 | ForEach-Object { "$_" }

