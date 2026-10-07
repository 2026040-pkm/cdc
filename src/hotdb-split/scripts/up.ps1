<#

.SYNOPSIS

  HotDB 스택을 이 PC 한 대에 전부 띄운다 (DB · 마이그레이션 · 판별 모듈 4 · RFC Service · DB Agent · 레거시 대역 · 발행기 · 모니터링).



.EXAMPLE

  .\scripts\up.ps1                 # 빌드 + 기동

  .\scripts\up.ps1 -SkipBuild      # jar 빌드 없이 기동

  .\scripts\up.ps1 -AllZones       # 발행기가 도장 · 가공 장비(대역 30대씩)도 낸다 — 필드 정의서 전까지의 가정값

  .\scripts\up.ps1 -Fast           # 발행기 actual 주기 5초 (스캔 · 실적이 빨리 쌓인다)

#>

param(

    [switch]$SkipBuild,

    [switch]$AllZones,

    [switch]$Fast


)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

Set-Location $root



function Invoke-Native([string]$what, [scriptblock]$cmd) {

    # 네이티브 명령의 stderr 를 PowerShell 오류로 바꾸지 않는다 (5.1 은 Stop 에서 죽는다)

    $prev = $ErrorActionPreference

    $ErrorActionPreference = "Continue"

    & $cmd 2>&1 | ForEach-Object { "$_" }

    $code = $LASTEXITCODE

    $ErrorActionPreference = $prev

    if ($code -ne 0) { throw "$what 실패 (exit $code)" }

}



# ── 1. JDK ───────────────────────────────────────────────────────────────

if (-not $env:JAVA_HOME) {

    $jdk = Get-ChildItem "$env:USERPROFILE\.jdks" -Directory -ErrorAction SilentlyContinue |

        Where-Object { $_.Name -match '21' } | Select-Object -First 1

    if ($jdk) { $env:JAVA_HOME = $jdk.FullName }

}

Write-Host "JAVA_HOME = $env:JAVA_HOME"




# ── 2. jar 빌드 ──────────────────────────────────────────────────────────

if (-not $SkipBuild) {

    Write-Host "gradle bootJar ..."

    Push-Location "$root\app"

    try { Invoke-Native "gradle bootJar" { .\gradlew.bat bootJar --no-daemon -q } } finally { Pop-Location }

}



# ── 3. Prometheus 수집 대상 — 한 대에 전부 띄울 때의 주소 (서버 to 서버는 up-services.sh 가 서버 주소로 다시 쓴다)
$tg = "$root\monitoring\prometheus\targets"
@{
    "legacy-db.json" = '[ { "targets": ["legacy-exporter:9187"], "labels": { "server": "local" } } ]'
    "node.json"      = '[ { "targets": ["node-exporter:9100"], "labels": { "server": "local" } } ]'
    "podman.json"    = '[ { "targets": ["podman-exporter:9882"], "labels": { "server": "local" } } ]'
}.GetEnumerator() | ForEach-Object { [IO.File]::WriteAllText("$tg\$($_.Key)", $_.Value + "`n") }

# ── 4. compose ───────────────────────────────────────────────────────────

$files = @("-f", "compose.db.yml", "-f", "compose.legacy.yml", "-f", "compose.zone.yml", "-f", "compose.rfc.yml", "-f", "compose.agent.yml",

           "-f", "compose.legacy-sim.yml", "-f", "compose.sim.yml", "-f", "compose.monitoring.yml", "-f", "compose.local.yml")

if ($AllZones) { $env:SIM_PAINTING = "30"; $env:SIM_MACHINING = "30" }

if ($Fast) { $env:SIM_ACTUAL_INTERVAL_MS = "5000" }



if (-not $SkipBuild) { Invoke-Native "compose build" { podman compose @files build } }



# 레거시 대역(SAP · Oracle)을 먼저 띄우고 원천 표를 채운다 — RFC Service · DB Agent 가 첫 주기부터 읽게

Write-Host "레거시 대역 기동 (Oracle 은 처음에 1~2분) ..."

Invoke-Native "legacy-sim up" { podman compose @files up -d --wait sap-sim oracle-sim }

$prev = $ErrorActionPreference; $ErrorActionPreference = "Continue"

$has = podman exec hotdb-sap-sim psql -U sapsim -d sapsim -Atc "SELECT to_regclass('erpsrc.item') IS NOT NULL" 2>$null

$ErrorActionPreference = $prev

if ("$has".Trim() -ne "t") {

    Invoke-Native "legacy-sim setup" { python "$root\scripts\legacy-sim.py" setup }

}



Invoke-Native "compose up" { podman compose @files up -d }



Write-Host ""

Write-Host "Grafana     http://localhost:59400   (대시보드 HotDB / HotDB CDC)"

Write-Host "Prometheus  http://localhost:59490"

Write-Host "HotDB       localhost:59433  db=hotdb  (postgres/postgres)"

Write-Host "판별 모듈   http://localhost:59481 (asm) · 59482 (oft) · 59483 (pnt) · 59484 (mch)  /actuator/health"

Write-Host "RFC Service http://localhost:59485/actuator/health   (SAP 폴링 → erp · 실적 CDC → SAP Z 표)"

Write-Host "DB Agent    http://localhost:59486/actuator/health   (Oracle 폴링 → mes · lgs)"

Write-Host "레거시 대역 SAP localhost:59434 (sapsim/sapsim) · Oracle localhost:59521/FREEPDB1 (legacy_reader/legacy_reader · 표 소유자 MES · LGS · GEO)"

Write-Host "발행기      http://localhost:59480/sim   (배속: POST /sim/speed?value=5)"

Write-Host ""

Write-Host "테스트:     .\scripts\test.ps1"

