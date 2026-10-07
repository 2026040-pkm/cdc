<#
.SYNOPSIS
  테스트. 기본은 단위 테스트 + 통합 테스트(기동 중인 스택에 붙음) + 대시보드 빈 패널 검사.

.EXAMPLE
  .\scripts\test.ps1            # 전부 (먼저 .\scripts\up.ps1)
  .\scripts\test.ps1 -UnitOnly  # DB 없이 단위 테스트만
#>
param([switch]$UnitOnly)
$root = Split-Path -Parent $PSScriptRoot
if (-not $env:JAVA_HOME) {
    $jdk = Get-ChildItem "$env:USERPROFILE\.jdks" -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match '21' } | Select-Object -First 1
    if ($jdk) { $env:JAVA_HOME = $jdk.FullName }
}
$ErrorActionPreference = "Continue"
Push-Location "$root\app"
try {
    $gargs = @("test", "--no-daemon")
    if (-not $UnitOnly) { $gargs += "-Dhotdb.it=true" }
    .\gradlew.bat @gargs 2>&1 | Where-Object { "$_" -match "PASSED|FAILED|SKIPPED|BUILD|Exception|expected" } | ForEach-Object { "$_" }
    $code = $LASTEXITCODE
} finally { Pop-Location }

if (-not $UnitOnly) {
    Write-Host ""
    Write-Host "대시보드 쿼리 검사"
    python "$root\scripts\check-dashboard.py" 2>&1 | ForEach-Object { "$_" }
}
Write-Host ""
Write-Host "리포트: $root\app\zone-service\build\reports\tests\test\index.html"
exit $code
