# ISL 호스트 프로세스 CPU·메모리 → Prometheus textfile (isl-metrics/isl.prom)
#
# ISL(Engine · Mqtt.Agent · EES.Kafka.Provider · Db.Provider)은 Aspire 가 호스트에서 .NET 프로세스로 띄운다.
# 컨테이너가 아니라 podman-exporter 에 안 잡힌다. 5초마다 Get-Process 를 찍어 파일로 쓰고,
# 컨테이너 node-exporter(textfile collector)가 그 폴더를 읽는다 — 호스트에 설치할 것이 없다.
#
# 같은 이름의 프로세스(Provider 3개 · Engine 2개)는 process 레이블 아래 pid 로 갈린다.
# 대시보드는 process 로 합쳐 본다.
#
# up.ps1 이 숨은 창으로 띄운다. 멈추려면: Get-Content isl-metrics\isl-process-metrics.pid | Stop-Process
param([int]$IntervalSeconds = 5)

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
$dir = Join-Path $root "isl-metrics"
New-Item -ItemType Directory -Force $dir | Out-Null
Set-Content -Path (Join-Path $dir "isl-process-metrics.pid") -Value $PID -Encoding ascii

$names = @("Engine", "Mqtt.Agent", "EES.Kafka.Provider", "Db.Provider")
$target = Join-Path $dir "isl.prom"
$tmp = Join-Path $dir "isl.prom.tmp"
$utf8 = New-Object System.Text.UTF8Encoding($false)

while ($true) {
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.AppendLine("# HELP isl_process_cpu_seconds_total total CPU time of an ISL host process")
    [void]$sb.AppendLine("# TYPE isl_process_cpu_seconds_total counter")
    $procs = @(Get-Process -Name $names -ErrorAction SilentlyContinue)
    foreach ($p in $procs) {
        $cpu = if ($null -ne $p.CPU) { $p.CPU } else { 0 }
        [void]$sb.AppendLine(("isl_process_cpu_seconds_total{{process=""{0}"",pid=""{1}""}} {2}" -f $p.ProcessName, $p.Id, [string]::Format([Globalization.CultureInfo]::InvariantCulture, "{0}", $cpu)))
    }
    [void]$sb.AppendLine("# HELP isl_process_working_set_bytes working set of an ISL host process")
    [void]$sb.AppendLine("# TYPE isl_process_working_set_bytes gauge")
    foreach ($p in $procs) {
        [void]$sb.AppendLine(("isl_process_working_set_bytes{{process=""{0}"",pid=""{1}""}} {2}" -f $p.ProcessName, $p.Id, $p.WorkingSet64))
    }
    [void]$sb.AppendLine("# HELP isl_process_up number of running processes per ISL component")
    [void]$sb.AppendLine("# TYPE isl_process_up gauge")
    foreach ($n in $names) {
        $count = @($procs | Where-Object { $_.ProcessName -eq $n }).Count
        [void]$sb.AppendLine(("isl_process_up{{process=""{0}""}} {1}" -f $n, $count))
    }
    # textfile collector 는 .prom 만 읽는다. 쓰다 만 파일을 읽지 않도록 tmp 에 쓰고 바꿔 끼운다.
    [IO.File]::WriteAllText($tmp, $sb.ToString().Replace("`r`n", "`n"), $utf8)
    Move-Item -Force $tmp $target
    Start-Sleep -Seconds $IntervalSeconds
}
