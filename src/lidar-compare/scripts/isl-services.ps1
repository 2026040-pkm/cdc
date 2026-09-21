# ISL 서비스(Engine ×2 · EES Kafka Provider ×3 · Db.Provider · Mqtt.Agent) 를 Aspire 와 같은 환경변수로 직접 띄운다.
#
# 왜 Aspire 에 맡기지 않는가
#   Aspire 가 NATS 컨테이너와 Engine 을 거의 동시에 띄우는데, Engine 은 기동 중 NATS 연결이 한 번
#   끊기면(`NATSConnectionException: Connect read error` · 10053) 재시도 없이 종료한다(JetStreamProvisioner).
#   Aspire 의 WaitFor 는 자기 프록시 포트로 건강 검사를 통과시키지만 Engine 이 쓰는 4222 포워딩은
#   그보다 늦게 열리는 일이 있다. 2026-09-17 에 두 번 연속 Engine 두 개가 모두 이렇게 죽었다.
#   Mqtt.Agent 는 Engine 이 안 뜨면 부트스트랩 재시도(8회)를 소진하고 멈춘다.
#
#   Nats__Url 은 localhost 가 아니라 127.0.0.1 로 준다. podman 은 포트를 127.0.0.1 에만 바인드하는데
#   NATS.Client(v1)가 localhost 를 ::1 부터 시도하다 연결 제한 시간(2초)을 넘겨 `timeout` 으로 죽는다.
#
#   그래서 Aspire 는 컨테이너(NATS · Kafka · Kafka UI)와 대시보드를 맡기고, 데이터 경로에 필요한
#   .NET 서비스만 NATS 가 응답하는 것을 확인한 뒤 여기서 순서대로 띄운다. 나머지 에이전트(OPC UA ·
#   Modbus · DB · Simulator · WinCC OA)는 이 비교와 무관해 건드리지 않는다.
#
#   .\isl-services.ps1           # 멈춰 있는 것만 띄운다 (이미 떠 있으면 그대로 둔다)
#   .\isl-services.ps1 -Restart  # 데이터 경로 서비스를 전부 내리고 다시 띄운다
#   .\isl-services.ps1 -Stop     # 내리기만 한다
param([switch]$Restart, [switch]$Stop)
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
$isl = "D:\git\intersyslink-v4\src"
$logs = Join-Path $root "logs"
New-Item -ItemType Directory -Force $logs | Out-Null
$engine = "docker"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { $engine = "podman" }

function Get-IslProcess([string]$exe, [string]$marker) {
  Get-CimInstance Win32_Process -Filter "Name='$exe'" | Where-Object { -not $marker -or ($_.CommandLine -match [regex]::Escape($marker)) }
}

$common = @{
  "DOTNET_ENVIRONMENT"     = "Development"
  "ASPNETCORE_ENVIRONMENT" = "Development"
}
$edgeCommon = @{
  "Edge__Engines__0__GrpcAddress"         = "http://localhost:5003"
  "Edge__Engines__1__GrpcAddress"         = "http://localhost:5004"
  "Edge__HeartbeatIntervalSeconds"        = "1"
  "EdgeRetry__MaxRetryPerCycle"           = "10"
  "EdgeRetry__MaxFailureWindowMinutes"    = "30"
  "EdgeRetry__MaxFailureCycles"           = "3"
  "EdgeRetry__DelayBetweenCyclesSeconds"  = "10"
}

# 이름 → (실행 파일, 작업 폴더, 추가 환경변수, 로그 이름, 프로세스 구분용 환경 표식)
# 같은 exe 를 여러 번 띄우므로 프로세스 구분은 pid 파일로 한다.
function Get-Services([hashtable]$natsPass) {
  $engineDir = "$isl\Engine\Engine\bin\Debug\net10.0"
  $providerDir = "$isl\Edge\Providers\EES\Kafka\EES.Kafka.Provider\bin\Debug\net10.0"
  $agentDir = "$isl\Edge\Agents\Mqtt\Mqtt.Agent\bin\Debug\net10.0"
  $svc = [ordered]@{}
  foreach ($n in @(
      @{ id = "primary"; peer = "secondary"; http = 5008; grpc = 5003; peerHttp = 5009; peerGrpc = 5004; prio = "200"; nats = 4222; natsNode = "nats-primary" },
      @{ id = "secondary"; peer = "primary"; http = 5009; grpc = 5004; peerHttp = 5008; peerGrpc = 5003; prio = "100"; nats = 4223; natsNode = "nats-secondary" })) {
    $svc["engine-$($n.id)"] = @{
      exe = "$engineDir\Engine.exe"; dir = $engineDir
      env = @{
        "Kestrel__Endpoints__Http__Url"       = "http://localhost:$($n.http)"
        "Kestrel__Endpoints__Http__Protocols" = "Http1"
        "Kestrel__Endpoints__Grpc__Url"       = "http://localhost:$($n.grpc)"
        "Kestrel__Endpoints__Grpc__Protocols" = "Http2"
        "EngineHa__NodeId"                    = $n.id
        "EngineHa__ClusterId"                 = "cluster-1"
        "EngineHa__PeerNodeId"                = $n.peer
        "EngineHa__PeerGrpcAddress"           = "http://localhost:$($n.peerGrpc)"
        "EngineHa__PeerHttpAddress"           = "http://localhost:$($n.peerHttp)"
        "EngineHa__Priority"                  = $n.prio
        "EngineHa__PeerHeartbeatIntervalSeconds" = "1"
        "EngineHa__PeerStaleTimeoutSeconds"   = "5"
        "ConnectionStrings__Default"          = "Data Source=isl-$($n.id).db"
        "Nats__Url"                           = "nats://nats:$($natsPass[$n.natsNode])@127.0.0.1:$($n.nats)"
        "Nats__ExternalUrl"                   = ""
        "Nats__SubjectPrefix"                 = "intersyslink.workflow"
      }
    }
  }
  foreach ($g in "edge.ees.kafka.p3.wwt", "a1", "a2") {
    $svc["provider-$g"] = @{
      exe = "$providerDir\EES.Kafka.Provider.exe"; dir = $providerDir
      env = $edgeCommon + @{ "Edge__EdgeGroupId" = $g; "Edge__EdgeId" = "$g.node-1" }
    }
  }
  # C 경로. Engine 이 워크플로우 태그를 NATS 로 뿌리면 일반 구독이라 Kafka Provider 와 사본을 따로 받는다.
  # 지표는 isl-metrics/ 에 textfile 로 쓴다 — isl-process-exporter 가 읽는다.
  $dbProviderDir = "$isl\Edge\Providers\Db\Db.Provider\bin\Debug\net10.0"
  $svc["db-provider"] = @{
    exe = "$dbProviderDir\Db.Provider.exe"; dir = $dbProviderDir
    env = $edgeCommon + @{
      "Edge__EdgeGroupId"           = "edge.db.p3.lidar"
      "Edge__EdgeId"                = "edge.db.p3.lidar.node-1"
      "DbProvider__ConnectionString" = "Host=127.0.0.1;Port=63433;Database=lidar;Username=postgres;Password=postgres;Application Name=isl-db-provider"
      "Metrics__TextfilePath"       = (Join-Path $root "isl-metrics\isl-db-provider.prom")
    }
  }
  $svc["mqtt-agent"] = @{
    exe = "$agentDir\Mqtt.Agent.exe"; dir = $agentDir
    env = $edgeCommon + @{
      "Edge__EdgeGroupId" = "edge.mqtt.p3.ot"; "Edge__EdgeId" = "edge.mqtt.p3.ot.node-1"; "Edge__NodeNo" = "1"
    }
  }
  return $svc
}

function Stop-DataPath {
  foreach ($exe in "Mqtt.Agent.exe", "EES.Kafka.Provider.exe", "Db.Provider.exe", "Engine.exe") {
    Get-IslProcess $exe | ForEach-Object {
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
      Write-Host "정지: $exe pid=$($_.ProcessId)"
    }
  }
  Get-ChildItem $logs -Filter "isl-*.pid" -ErrorAction SilentlyContinue | Remove-Item -Force
}

if ($Stop -or $Restart) { Stop-DataPath }
if ($Stop) { return }

# ── NATS 준비 확인 ────────────────────────────────────────────────────────
# NATS 노드마다 Aspire 가 비밀번호를 따로 만든다. 컨테이너 인자(--pass)에서 각각 읽는다.
$natsPass = @{}
$ctrNames = & $engine ps --format '{{.Names}}'
foreach ($node in "nats-primary", "nats-secondary") {
  $ctr = $ctrNames | Where-Object { $_ -match "^$node-[a-z0-9]+$" } | Select-Object -First 1
  if (-not $ctr) { Write-Error "Aspire NATS 컨테이너($node)가 없다. Aspire 를 먼저 띄워라."; exit 1 }
  $natsArgs = [string](& $engine inspect $ctr --format '{{json .Args}}')
  $natsPass[$node] = if ($natsArgs -match '"--pass"\s*,\s*"([^"]+)"') { $Matches[1] } else { "" }
}

function Test-Nats([int]$port) {
  try {
    $c = New-Object System.Net.Sockets.TcpClient; $c.ReceiveTimeout = 3000; $c.Connect("127.0.0.1", $port)
    $buf = New-Object byte[] 8; $n = $c.GetStream().Read($buf, 0, 8); $c.Close()
    return ([Text.Encoding]::ASCII.GetString($buf, 0, $n) -like "INFO*")
  } catch { return $false }
}
foreach ($port in 4222, 4223) {
  $ok = $false
  for ($i = 0; $i -lt 60 -and -not $ok; $i++) { $ok = Test-Nats $port; if (-not $ok) { Start-Sleep -Seconds 2 } }
  if (-not $ok) { Write-Error "NATS 127.0.0.1:$port 가 응답하지 않는다"; exit 1 }
}
Write-Host "NATS 4222·4223 응답 확인"

$services = Get-Services $natsPass

function Start-Service1([string]$name, $s) {
  $pidFile = Join-Path $logs "isl-$name.pid"
  if (Test-Path $pidFile) {
    $old = Get-Content $pidFile
    if ($old -and (Get-Process -Id $old -ErrorAction SilentlyContinue)) { Write-Host "이미 실행 중: $name pid=$old"; return }
  }
  # Start-Process 는 환경변수를 따로 넘기지 못한다. 현재 프로세스 환경에 잠깐 얹고 띄운 뒤 되돌린다.
  $saved = @{}
  # Development 는 Engine 에만 준다. Edge(Mqtt.Agent)는 Development 에서 켜지는 DI 스코프 검증에 걸려
  # 기동하지 못한다(MqttConfigurationResponderWorker 가 scoped 핸들러를 받는다) — Aspire 도 Edge 는 기본 환경으로 띄운다.
  $all = if ($name -like "engine-*") { $common + $s.env } else { $s.env }
  foreach ($k in $all.Keys) { $saved[$k] = [Environment]::GetEnvironmentVariable($k); [Environment]::SetEnvironmentVariable($k, $all[$k]) }
  try {
    $p = Start-Process -FilePath $s.exe -WorkingDirectory $s.dir -WindowStyle Hidden -PassThru `
      -RedirectStandardOutput (Join-Path $logs "isl-$name.out.log") -RedirectStandardError (Join-Path $logs "isl-$name.err.log")
  } finally {
    foreach ($k in $saved.Keys) { [Environment]::SetEnvironmentVariable($k, $saved[$k]) }
  }
  Set-Content $pidFile $p.Id
  Write-Host "기동: $name pid=$($p.Id)"
}

function Wait-Port([int]$port, [int]$seconds) {
  for ($i = 0; $i -lt $seconds; $i++) {
    if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) { return $true }
    Start-Sleep -Seconds 1
  }
  return $false
}

# Aspire 가 띄웠다가 죽은(또는 부트스트랩을 포기한) 인스턴스가 남아 있으면 겹친다. pid 파일로
# 관리하지 않는 같은 exe 는 먼저 내린다.
$managed = @(Get-ChildItem $logs -Filter "isl-*.pid" -ErrorAction SilentlyContinue | ForEach-Object { [int](Get-Content $_.FullName) })
foreach ($exe in "Mqtt.Agent.exe", "EES.Kafka.Provider.exe", "Db.Provider.exe", "Engine.exe") {
  Get-IslProcess $exe | Where-Object { $managed -notcontains $_.ProcessId } | ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    Write-Host "관리 밖 인스턴스 정지: $exe pid=$($_.ProcessId)"
  }
}

Start-Service1 "engine-primary" $services["engine-primary"]
Start-Service1 "engine-secondary" $services["engine-secondary"]
foreach ($port in 5003, 5004) {
  if (Wait-Port $port 90) { Write-Host "Engine gRPC :$port 대기 확인" } else { Write-Warning "Engine gRPC :$port 가 90초 안에 열리지 않았다 — logs\isl-engine-*.err.log 확인" }
}
foreach ($name in "provider-edge.ees.kafka.p3.wwt", "provider-a1", "provider-a2", "db-provider", "mqtt-agent") {
  Start-Service1 $name $services[$name]
}
