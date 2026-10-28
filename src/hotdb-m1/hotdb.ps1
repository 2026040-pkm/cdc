# Hot DB 절차 래퍼 (Windows PowerShell 5.1) — hotdb.sh 와 같은 명령
#   .\hotdb.ps1 up       기동 + 확인. 볼륨이 비어 있으면 initdb 가 스키마를 만든다
#   .\hotdb.ps1 clean    데이터만 비움 — 스키마 · 사전 시드 유지
#   .\hotdb.ps1 reset    볼륨까지 지우고 처음부터 다시 구축 (데이터 전부 삭제) — 스키마를 바꾼 뒤에도 이것
#   .\hotdb.ps1 verify   구축 상태 · CDC 설정 · 가공 절점 사전 · 적재 건수 출력
#   .\hotdb.ps1 cdc-check  시험 슬롯으로 tsdb · hot.p_progress 변경이 publication 으로 나오는지 확인
#   .\hotdb.ps1 psql     접속
#   .\hotdb.ps1 down     컨테이너 정지 (데이터 유지)
# 엔진은 docker 우선, 없으면 podman. $env:HOTDB_ENGINE 로 강제할 수 있다.
param([string]$Command = '')
Set-Location $PSScriptRoot
$env:PODMAN_COMPOSE_WARNING_LOGS = 'false'   # podman 외부 compose 안내문(stderr) 끄기

$Engine = $env:HOTDB_ENGINE
if (-not $Engine) {
    if (Get-Command docker -ErrorAction SilentlyContinue) { $Engine = 'docker' }
    elseif (Get-Command podman -ErrorAction SilentlyContinue) { $Engine = 'podman' }
    elseif (Test-Path "$env:LOCALAPPDATA\Programs\Podman\podman.exe") { $Engine = "$env:LOCALAPPDATA\Programs\Podman\podman.exe" }
    else { Write-Host 'docker 또는 podman 을 찾지 못했습니다.'; exit 1 }
}

function Compose { & $Engine compose @args 2>&1 | ForEach-Object { "$_" }; if ($LASTEXITCODE -ne 0) { throw "compose $args 실패 (exit $LASTEXITCODE)" } }
function PsqlFile([string]$Path) {
    # 관리자는 컨테이너의 POSTGRES_USER 를 그대로 쓴다 (.env HOTDB_ADMIN_USER)
    Get-Content $Path -Raw -Encoding UTF8 | & $Engine compose exec -T hotdb sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
    if ($LASTEXITCODE -ne 0) { throw "psql $Path 실패 (exit $LASTEXITCODE)" }
}

function Do-Verify  { PsqlFile hotdb\verify.sql }
function Do-Cdc     { PsqlFile hotdb\cdc_check.sql }
function Do-Up      { Compose up -d --wait hotdb; Do-Verify }
function Do-Reset   { Compose down -v --remove-orphans; Do-Up }
function Do-Clean   { PsqlFile hotdb\clean.sql; Write-Host '데이터를 비웠습니다 (스키마 · 사전 시드 유지).' }

try {
    switch ($Command) {
        'up'      { Do-Up }
        'clean'   { Do-Clean }
        'reset'   { Do-Reset }
        'verify'  { Do-Verify }
        'cdc-check' { Do-Cdc }
        'psql'    { & $Engine compose exec hotdb sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' }
        'down'    { Compose down }
        default   { Get-Content $PSCommandPath -Encoding UTF8 | Select-Object -Skip 1 -First 8; exit 1 }
    }
} catch {
    Write-Host $_.Exception.Message
    exit 1
}
