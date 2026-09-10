#:package MQTTnet@5.2.0.1603

using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

// ──────────────────────────────────────────────────────────────
// 조립·선행의장 LiDAR 필드 데이터 발행기 (350대 / 3채널)
//
// Mqtt.Agent 의 MqttPayloadParser 가 받는 계약 그대로 보낸다:
//   { "id": "<식별자>", "raw_payload": { <평탄화된 필드들> } }
// 세 채널 모두 tagMode=raw 로 구독하는 것을 전제한다 —
// raw_payload 통째가 {id}.{topic}.raw_payload 태그 하나의 값이 된다.
//
//   ① 장비 상태   ot/device/{zone}/lidar/status           1건 / 1초  (--status-interval)
//   ② 실적 결과   ot/sensor/{stage}/actual                1건 / 1분  (--interval)
//   ③ 산출물 메타 ot/pipeline/{zone}/{shop}/{bay}/artifact 12건 / 1분 (--interval)
//                 (정합 PCD 1 + 변환행렬 1 + 세그먼트 10)
//
// 상태와 스캔은 박자가 달라 장비마다 루프가 둘이다.
//
// 실행:  dotnet run tools/mqtt-lidar-sim/lidar-sim.cs -- --host localhost --port 1884
// 태그:  dotnet run tools/mqtt-lidar-sim/lidar-sim.cs -- --export-tags docs/mqtt-tags
// ──────────────────────────────────────────────────────────────

var opt = Options.Parse(args);
if (opt is null)
{
    return 1;
}

Console.OutputEncoding = Encoding.UTF8;

var fleet = Fleet.Build(opt);

if (opt.ExportTagsDir is { } exportDir)
{
    return TagCatalog.Export(fleet, opt, exportDir);
}

if (opt.DryRun)
{
    var sample = fleet[0];
    var now = DateTimeOffset.Now;
    var scan = new ScanContext(sample, now, opt.SegmentCount);
    foreach (var m in scan.Messages().Prepend(ScanContext.StatusMessage(sample, now)))
    {
        Console.WriteLine($"topic  : {m.Topic}");
        Console.WriteLine($"payload: {Encoding.UTF8.GetString(m.Payload)}");
        Console.WriteLine();
    }

    return 0;
}

var perScan = 2 + opt.SegmentCount + 1;
var statusRate = fleet.Length * 1000.0 / opt.StatusIntervalMs;
var scanRate = fleet.Length * perScan * 1000.0 / opt.IntervalMs;
Console.WriteLine($"broker      : {opt.Host}:{opt.Port} ({opt.ProtocolVersion}, qos {opt.Qos})");
Console.WriteLine($"devices     : {fleet.Length}대  (조립 {fleet.Count(d => d.Zone == Zones.Assembly)} · 의장 {fleet.Count(d => d.Zone == Zones.Outfitting)})");
Console.WriteLine($"              {fleet[0].Id} … {fleet[^1].Id}");
Console.WriteLine($"status      : {opt.StatusIntervalMs / 1000.0:F0}초/대 × 1건  →  약 {statusRate:F1} msg/s");
Console.WriteLine($"scan        : {opt.IntervalMs / 1000.0:F0}초/대 × {perScan}건  →  약 {scanRate:F1} msg/s");
Console.WriteLine($"합계        : 약 {statusRate + scanRate:F1} msg/s");
Console.WriteLine($"topics      : {TagCatalog.Topics(fleet, opt).Count}개");
Console.WriteLine();

using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine("\n중지 중…");
    shutdown.Cancel();
};

var factory = new MqttClientFactory();
using var client = factory.CreateMqttClient();

var clientOptions = new MqttClientOptionsBuilder()
    .WithTcpServer(opt.Host, opt.Port)
    .WithClientId(opt.ClientId)
    // Mqtt.Agent(MqttNetSession.BuildOptions)가 3.1.1로 고정돼 있다. 발행 쪽도 같은 버전으로 붙어야
    // 에이전트가 실제로 보는 것과 같은 조건이 된다. MQTTnet 기본값(v5)으로 붙으면 브로커에 따라
    // CONNACK 0x01(UnacceptableProtocolVersion)로 즉시 끊긴다.
    .WithProtocolVersion(opt.ProtocolVersion)
    .WithCleanSession()
    .WithKeepAlivePeriod(TimeSpan.FromSeconds(30))
    .WithTimeout(TimeSpan.FromSeconds(10));

if (opt.Username is not null)
{
    clientOptions = clientOptions.WithCredentials(opt.Username, opt.Password ?? string.Empty);
}

client.DisconnectedAsync += async e =>
{
    if (shutdown.IsCancellationRequested)
    {
        return;
    }

    Console.WriteLine($"[{Stamp()}] 연결 끊김 ({e.Reason}) — 3초 후 재연결");
    try
    {
        await Task.Delay(TimeSpan.FromSeconds(3), shutdown.Token);
        await client.ConnectAsync(clientOptions.Build(), shutdown.Token);
        Console.WriteLine($"[{Stamp()}] 재연결 완료");
    }
    catch (OperationCanceledException)
    {
        // 종료 중이면 재연결하지 않는다.
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[{Stamp()}] 재연결 실패: {ex.Message}");
    }
};

await client.ConnectAsync(clientOptions.Build(), shutdown.Token);
Console.WriteLine($"[{Stamp()}] 연결됨. Ctrl+C 로 중지.\n");

var published = 0L;
var failed = 0L;
var scans = 0L;

// 장비마다 독립 루프를 돌리고 시작 시점을 주기 안에 고르게 흩어 놓는다.
// 350대가 같은 순간에 몰려 publish 하면 실제 라인이 아니라 부하 스파이크를 재현하게 된다.
//
// 상태와 스캔은 박자가 달라 루프를 나눈다 - 하트비트는 초 단위로 계속 뛰고,
// 스캔은 분 단위로 한 번에 13건을 쏟아낸다.
var workers = fleet
    .SelectMany(device => new[] { StatusLoop(device), ScanLoop(device) })
    .ToArray();

var reporter = Report(shutdown.Token);

await Task.WhenAll(workers);
await reporter;

if (client.IsConnected)
{
    await client.DisconnectAsync();
}

Console.WriteLine($"총 발행 {Volatile.Read(ref published):N0}건 (스캔 {Volatile.Read(ref scans):N0}회), 실패 {Volatile.Read(ref failed):N0}건.");
return 0;

async Task StatusLoop(Device device)
{
    var offset = opt.StatusIntervalMs * (double)device.Index / fleet.Length;
    try
    {
        await Task.Delay(TimeSpan.FromMilliseconds(offset), shutdown.Token);

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(opt.StatusIntervalMs));
        do
        {
            device.AdvanceHealth();
            await Publish(ScanContext.StatusMessage(device, DateTimeOffset.Now));
        }
        while (await timer.WaitForNextTickAsync(shutdown.Token));
    }
    catch (OperationCanceledException)
    {
        // 정상 종료.
    }
}

async Task ScanLoop(Device device)
{
    var offset = opt.IntervalMs * (double)device.Index / fleet.Length;
    try
    {
        await Task.Delay(TimeSpan.FromMilliseconds(offset), shutdown.Token);

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(opt.IntervalMs));
        do
        {
            device.AdvanceScan();
            var scan = new ScanContext(device, DateTimeOffset.Now, opt.SegmentCount);
            Interlocked.Increment(ref scans);

            foreach (var m in scan.Messages())
            {
                await Publish(m);

                // 한 스캔의 13건은 파이프라인을 타고 순서대로 나오는 것이라 동시에 터지지 않는다.
                if (opt.StaggerMs > 0)
                {
                    await Task.Delay(opt.StaggerMs, shutdown.Token);
                }
            }
        }
        while (await timer.WaitForNextTickAsync(shutdown.Token));
    }
    catch (OperationCanceledException)
    {
        // 정상 종료.
    }
}

async Task Publish(OutMessage m)
{
    var message = new MqttApplicationMessageBuilder()
        .WithTopic(m.Topic)
        .WithPayload(m.Payload)
        .WithQualityOfServiceLevel((MqttQualityOfServiceLevel)opt.Qos)
        .WithRetainFlag(opt.Retain)
        .Build();

    try
    {
        await client.PublishAsync(message, shutdown.Token);
        Interlocked.Increment(ref published);
    }
    catch (OperationCanceledException)
    {
        // 종료 중이다.
    }
    catch (Exception ex)
    {
        if (Interlocked.Increment(ref failed) % 100 == 1)
        {
            Console.WriteLine($"[{Stamp()}] publish 실패({m.Topic}): {ex.Message}");
        }
    }
}

async Task Report(CancellationToken ct)
{
    var clock = Stopwatch.StartNew();
    var last = 0L;
    var lastAt = TimeSpan.Zero;

    try
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10));
        while (await timer.WaitForNextTickAsync(ct))
        {
            var now = Volatile.Read(ref published);
            var elapsed = clock.Elapsed;
            var rate = (now - last) / (elapsed - lastAt).TotalSeconds;
            last = now;
            lastAt = elapsed;

            var alarms = fleet.Count(d => d.Status is "ERROR" or "CALIBRATING");
            var offline = fleet.Count(d => d.Status == "OFFLINE");
            Console.WriteLine(
                $"[{Stamp()}] 발행 {now,9:N0}건  {rate,6:F1} msg/s  실패 {Volatile.Read(ref failed),4:N0}  " +
                $"보정 {alarms,3}대  정지 {offline,3}대");
        }
    }
    catch (OperationCanceledException)
    {
        // 정상 종료.
    }
}

static string Stamp() => DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture);

static class Json
{
    /// <summary>
    /// 기본 인코더는 타임스탬프 오프셋의 '+' 까지 유니코드로 escape 한다(<c>+09:00</c> → <c>u002B09:00</c>).
    /// 파싱에는 지장이 없지만 브로커 대시보드에서 눈으로 확인할 페이로드라 원문 그대로 쓴다.
    /// </summary>
    public static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };
}

static class Zones
{
    public const string Assembly = "ASSEMBLY";
    public const string Outfitting = "OUTFITTING";

    /// <summary>조립 4종 · 의장 2종. 필드 정의서에 이름이 확정되면 이 배열만 바꾸면 된다.</summary>
    public static string[] StagesOf(string zone) => zone == Assembly
        ? ["ARRANGEMENT", "FITTING", "WELDING", "INSPECTION"]
        : ["WIRING", "PIPING"];

    public static string SlugOf(string zone) => zone == Assembly ? "assembly" : "outfitting";
}

/// <summary>한 스캔이 만들어내는 메시지 하나.</summary>
readonly record struct OutMessage(string Topic, byte[] Payload);

static class Fleet
{
    /// <summary>
    /// 조립 assembly1 · 의장 outfitting1 을 각각 bay 로 쪼개 장비를 고르게 배치한다.
    /// 장비 ID 는 <c>LDR-{site}-{shop}{bay}-{seq}</c> — 산출물 채널에서 여기에
    /// <c>-TRANSFORMATION_MATRIX</c> 가 붙고 토픽까지 합쳐 TagId 가 되므로 짧게 유지한다.
    /// </summary>
    public static Device[] Build(Options o)
    {
        var devices = new List<Device>(o.AssemblyDevices + o.OutfittingDevices);

        Add(Zones.Assembly, "assembly1", "A1", o.AssemblyDevices);
        Add(Zones.Outfitting, "outfitting1", "O1", o.OutfittingDevices);

        for (var i = 0; i < devices.Count; i++)
        {
            devices[i].Index = i;
        }

        return [.. devices];

        void Add(string zone, string shop, string shopCode, int count)
        {
            if (count <= 0)
            {
                return;
            }

            var perBay = (int)Math.Ceiling(count / (double)o.Bays);
            var seq = 0;

            for (var bay = 1; bay <= o.Bays && seq < count; bay++)
            {
                for (var n = 1; n <= perBay && seq < count; n++, seq++)
                {
                    devices.Add(new Device(
                        id: $"LDR-{o.SiteCode}-{shopCode}B{bay}-{n:D2}",
                        zone: zone,
                        shop: shop,
                        bay: $"bay{bay}",
                        site: o.Site,
                        panTilt: $"PTZ-{o.SiteCode}-{shopCode}B{bay}-{n:D2}",
                        edgePc: $"EDG-{o.SiteCode}-{shopCode}-{bay:D2}",
                        inferenceWs: $"INF-{o.SiteCode}-{shopCode}-01"));
                }
            }
        }
    }
}

/// <summary>한 대의 LiDAR 가 들고 있는 값. 매 주기마다 조금씩만 움직인다.</summary>
sealed class Device
{
    // 장비별로 다른 시드를 줘야 350대가 한 몸처럼 똑같이 오르내리지 않는다.
    private readonly Random _rng;
    private readonly string[] _stages;

    private static readonly string[] ErrorCodes =
    [
        "E-LDR-0101", // 광학창 오염
        "E-LDR-0203", // 모터 회전수 이상
        "E-LDR-0311", // 포인트클라우드 결측
        "E-LDR-0402", // 내부 과열
        "E-NET-0007", // 통신 지연
    ];

    private static readonly string[] EventTypes = ["START", "PROGRESS", "COMPLETE"];
    private static readonly string[] FovModes = ["wide", "narrow"];

    public Device(string id, string zone, string shop, string bay, string site,
        string panTilt, string edgePc, string inferenceWs)
    {
        Id = id;
        Zone = zone;
        Shop = shop;
        Bay = bay;
        Site = site;
        PanTilt = panTilt;
        EdgePc = edgePc;
        InferenceWs = inferenceWs;
        _stages = Zones.StagesOf(zone);
        _rng = new Random(id.GetHashCode(StringComparison.Ordinal));

        TemperatureC = 34 + _rng.NextDouble() * 8;      // 34-42 도
        ScanRatePtsPerSec = 220_000 + _rng.Next(0, 60_000);
        ConnectivityRssi = -45 - _rng.Next(0, 25);      // -45 ~ -70 dBm
        MatchConfidence = 0.88 + _rng.NextDouble() * 0.10;
        ProgressRate = _rng.NextDouble() * 60;
        LastHeartbeatAt = DateTimeOffset.Now;
        FovMode = FovModes[_rng.Next(FovModes.Length)];
    }

    public string Id { get; }
    public string Zone { get; }
    public string Shop { get; }
    public string Bay { get; }
    public string Site { get; }
    public string PanTilt { get; }
    public string EdgePc { get; }
    public string InferenceWs { get; }
    public int Index { get; set; }

    public string Status { get; private set; } = "ONLINE";
    public string? ErrorCode { get; private set; }
    public string FovMode { get; private set; }
    public double TemperatureC { get; private set; }
    public long ScanRatePtsPerSec { get; private set; }
    public long ConnectivityRssi { get; private set; }
    public double MatchConfidence { get; private set; }
    public double ProgressRate { get; private set; }
    public DateTimeOffset LastHeartbeatAt { get; private set; }
    public int Cycle { get; private set; }

    /// <summary>스캔마다 공정이 달라진다 — 한 장비의 태그가 stage 토픽 전부에 걸친다.</summary>
    public string Stage => _stages[Cycle % _stages.Length];

    public string EventType => EventTypes[Cycle % EventTypes.Length];

    public string SourceSystem => Zone == Zones.Assembly ? "AI Inference Service" : "LiDAR Edge Service";

    public string HullNo => $"H{1200 + (Index % 40)}";

    public string BlockId => $"B{100 + (Index % 90)}{(Index % 2 == 0 ? "P" : "S")}";

    /// <summary>OCR 이 개입한 결과에만 채워진다 — 대부분의 스캔에서는 null 이다.</summary>
    public string? VisionOcr => Index % 7 == 0 ? $"OCR-{Id.Split('-')[1]}-{Bay}-01" : null;

    /// <summary>
    /// 하트비트 주기(기본 1초)마다 도는 값 - 상태 전이와 장비 계측값.
    /// </summary>
    /// <remarks>
    /// 아래 확률과 진폭은 <b>1초 간격</b>을 전제로 잡혀 있다. <c>--status-interval</c> 을 크게
    /// 늘리면 상태가 그만큼 드물게 바뀌고 계측값도 천천히 움직인다.
    /// </remarks>
    public void AdvanceHealth()
    {
        AdvanceStatus();

        // OFFLINE 이면 하트비트가 멈춘다 - 이게 "죽은 장비"의 관측 가능한 신호다.
        if (Status != "OFFLINE")
        {
            LastHeartbeatAt = DateTimeOffset.Now;
        }

        var stress = Status switch
        {
            "ERROR" => 1.0,
            "CALIBRATING" => 0.4,
            _ => 0.0,
        };

        TemperatureC = Math.Clamp(TemperatureC + Walk(0.05) + (stress * 0.02), 30, 78);
        ConnectivityRssi = (long)Math.Clamp(ConnectivityRssi + Walk(0.4), -92, -38);

        ScanRatePtsPerSec = Status switch
        {
            "OFFLINE" => 0,
            "ERROR" => (long)Math.Clamp((ScanRatePtsPerSec * 0.95) + Walk(1_500), 0, 280_000),
            "CALIBRATING" => (long)Math.Clamp((ScanRatePtsPerSec * 0.98) + Walk(1_500), 40_000, 280_000),
            _ => (long)Math.Clamp(ScanRatePtsPerSec + Walk(1_500), 200_000, 280_000),
        };
    }

    /// <summary>스캔 주기(기본 1분)마다 도는 값 - 공정·이벤트·진척률.</summary>
    public void AdvanceScan()
    {
        Cycle++;

        var stress = Status switch
        {
            "ERROR" => 1.0,
            "CALIBRATING" => 0.4,
            _ => 0.0,
        };

        MatchConfidence = Math.Clamp(MatchConfidence + Walk(0.01) - (stress * 0.03), 0.40, 0.999);

        // 블록 진척률은 되돌아가지 않는다. COMPLETE 를 지나면 다음 블록으로 넘어가 0부터 다시 오른다.
        ProgressRate = EventType == "COMPLETE"
            ? _rng.NextDouble() * 5
            : Math.Min(100, ProgressRate + (_rng.NextDouble() * 6));
    }

    private void AdvanceStatus()
    {
        var roll = _rng.NextDouble();

        // 초당 확률이다. ONLINE->CALIBRATING 0.0002 면 장비 하나가 평균 80분에 한 번 보정에 들어가고,
        // 350대 라인 전체로는 십수 초에 한 번꼴로 어딘가에서 상태가 바뀐다.
        switch (Status)
        {
            case "ONLINE" when roll < 0.0002:
                Enter("CALIBRATING");
                break;
            case "ONLINE" when roll < 0.0003:
                Enter("ERROR");
                break;

            case "CALIBRATING" when roll < 0.0200:
                Enter("ONLINE");
                break;

            case "ERROR" when roll < 0.0020:
                Enter("OFFLINE");
                break;
            case "ERROR" when roll < 0.0120:
                Enter("ONLINE");
                break;

            case "OFFLINE" when roll < 0.0030:
                Enter("ONLINE");
                break;
        }
    }

    private void Enter(string status)
    {
        Status = status;
        ErrorCode = status == "ERROR" ? ErrorCodes[_rng.Next(ErrorCodes.Length)] : null;
    }

    public int Next(int minInclusive, int maxExclusive) => _rng.Next(minInclusive, maxExclusive);

    public double NextDouble() => _rng.NextDouble();

    private double Walk(double amplitude) => (_rng.NextDouble() - 0.5) * 2 * amplitude;
}

static class TopicNames
{
    public const string RegisteredPcd = "REGISTERED_PCD";
    public const string TransformationMatrix = "TRANSFORMATION_MATRIX";
    public const string SegmentedPcd = "SEGMENTED_PCD";

    public static readonly string[] ArtifactTypes = [RegisteredPcd, TransformationMatrix, SegmentedPcd];

    public static string Status(string zone) => $"ot/device/{Zones.SlugOf(zone)}/lidar/status";

    public static string Actual(string stage) => $"ot/sensor/{stage.ToLowerInvariant()}/actual";

    public static string Artifact(string zone, string shop, string bay)
        => $"ot/pipeline/{Zones.SlugOf(zone)}/{shop}/{bay}/artifact";

    /// <summary>산출물은 장비 하나가 종류마다 다른 id 로 발행한다 - 태그가 종류별로 갈린다.</summary>
    public static string ArtifactId(string deviceId, string artifactType) => $"{deviceId}-{artifactType}";
}

/// <summary>
/// 한 번의 스캔이 만들어내는 메시지 묶음. 상태 1 + 실적 1 + 산출물(정합 1 · 변환행렬 1 · 세그먼트 N).
/// </summary>
/// <remarks>
/// scan_id 를 실적과 산출물이 공유한다. 이게 두 채널을 잇는 유일한 조인 키이므로
/// 한 스캔 안에서는 반드시 같은 값이어야 한다.
/// </remarks>
sealed class ScanContext(Device device, DateTimeOffset at, int segments)
{
    private readonly string _scanId = Guid.NewGuid().ToString();

    /// <summary>
    /// 상태는 스캔과 다른 박자로 뛴다 - 하트비트는 초 단위, 스캔은 분 단위다.
    /// 그래서 스캔 묶음에 끼우지 않고 따로 만든다.
    /// </summary>
    public static OutMessage StatusMessage(Device device, DateTimeOffset at)
        => new(TopicNames.Status(device.Zone), StatusPayload(device, at));

    public IEnumerable<OutMessage> Messages()
    {
        yield return new OutMessage(TopicNames.Actual(device.Stage), ActualPayload());

        var artifactTopic = TopicNames.Artifact(device.Zone, device.Shop, device.Bay);
        yield return new OutMessage(artifactTopic, RegisteredPcdPayload());
        yield return new OutMessage(artifactTopic, TransformationMatrixPayload());

        for (var i = 1; i <= segments; i++)
        {
            yield return new OutMessage(artifactTopic, SegmentPayload(i));
        }
    }

    // 파일 기반 앱은 리플렉션 기반 System.Text.Json 이 꺼져 있으므로 Utf8JsonWriter 로 직접 쓴다.
    private static byte[] StatusPayload(Device device, DateTimeOffset at) => Build(device.Id, writer =>
    {
        writer.WriteString("device_role", "LIDAR");
        writer.WriteString("site", device.Site);
        writer.WriteString("zone", device.Zone);
        writer.WriteString("shop", device.Shop);
        writer.WriteString("bay", device.Bay);
        writer.WriteString("status", device.Status);
        writer.WriteString("last_heartbeat_at", Iso(device.LastHeartbeatAt));
        WriteNullableString(writer, "error_code", device.ErrorCode);
        writer.WriteString("occurred_at", Iso(at));
        writer.WriteString("ingested_at", Iso(at.AddMilliseconds(device.Next(200, 1400))));
        writer.WriteString("idempotency_key", $"{device.Id}:{at:yyyyMMdd'T'HHmmss}");
        writer.WriteNumber("scan_rate_pts_per_sec", device.ScanRatePtsPerSec);
        writer.WriteNumber("temperature_c", Math.Round(device.TemperatureC, 1));
        writer.WriteNumber("connectivity_rssi", device.ConnectivityRssi);
        writer.WriteString("fov_mode", device.FovMode);
    });

    /// <remarks>
    /// device_ids 는 중첩 객체지만 브로커로 나갈 때는 평탄화한다 - Agent 의 계약이
    /// raw_payload 한 겹뿐이라 중첩을 두면 tagMode=fields 로 바꿨을 때 키 이름이 달라진다.
    /// </remarks>
    private byte[] ActualPayload() => Build(device.Id, writer =>
    {
        writer.WriteString("zone", device.Zone);
        writer.WriteString("site", device.Site);
        writer.WriteString("shop", device.Shop);
        writer.WriteString("bay", device.Bay);
        writer.WriteString("stage", device.Stage);
        writer.WriteString("record_type", "ACTUAL");
        writer.WriteString("input_method", "AUTO");
        writer.WriteString("source_system", device.SourceSystem);
        writer.WriteString("hull_no", device.HullNo);
        writer.WriteString("block_id", device.BlockId);
        writer.WriteString("scan_id", _scanId);
        writer.WriteString("scanned_at", Iso(at));
        writer.WriteString("pan_tilt", device.PanTilt);
        writer.WriteString("edge_pc", device.EdgePc);
        writer.WriteString("inference_ws", device.InferenceWs);
        WriteNullableString(writer, "vision_ocr", device.VisionOcr);
        writer.WriteString("event_type", device.EventType);
        writer.WriteString("occurred_at", Iso(at));
        writer.WriteString("ingested_at", Iso(at.AddMilliseconds(device.Next(200, 1400))));
        writer.WriteString("idempotency_key", $"{Short(_scanId)}:{device.Stage}:{device.EventType}");
        writer.WriteNumber("block_progress_rate", Math.Round(device.ProgressRate, 1));
        writer.WriteString("reference_cad_id", $"CAD-{device.HullNo}-{device.BlockId}");
        writer.WriteNumber("match_confidence", Math.Round(device.MatchConfidence, 2));
        writer.WriteString("model_version", "seg-v1.4.2");
    });

    private byte[] RegisteredPcdPayload() => Build(
        TopicNames.ArtifactId(device.Id, TopicNames.RegisteredPcd),
        writer =>
        {
            WriteArtifactHead(writer, TopicNames.RegisteredPcd);
            writer.WriteNull("segment_id");
            writer.WriteString("storage_uri", $"file://ot-a/pcd/{at:yyyy'/'MM'/'dd}/{Short(_scanId)}/registered.pcd");
            writer.WriteNumber("file_size_bytes", 150_000_000L + device.Next(0, 90_000_000));
            writer.WriteString("checksum", Checksum());
            writer.WriteNull("transformation_matrix");
            WriteArtifactTail(writer);
        });

    /// <remarks>4x4 = 숫자 16개뿐이라 파일로 빼지 않고 메시지에 직접 싣는다.</remarks>
    private byte[] TransformationMatrixPayload() => Build(
        TopicNames.ArtifactId(device.Id, TopicNames.TransformationMatrix),
        writer =>
        {
            WriteArtifactHead(writer, TopicNames.TransformationMatrix);
            writer.WriteNull("segment_id");
            writer.WriteNull("storage_uri");
            writer.WriteNull("file_size_bytes");
            writer.WriteNull("checksum");

            writer.WriteStartArray("transformation_matrix");
            for (var row = 0; row < 4; row++)
            {
                for (var col = 0; col < 4; col++)
                {
                    var identity = row == col ? 1.0 : 0.0;
                    var value = col == 3 && row < 3
                        ? Math.Round(1000 + (device.NextDouble() * 500), 1)   // 평행이동 성분(mm)
                        : Math.Round(identity + ((device.NextDouble() - 0.5) * 0.1), 4);
                    writer.WriteNumberValue(value);
                }
            }

            writer.WriteEndArray();
            WriteArtifactTail(writer);
        });

    private byte[] SegmentPayload(int index) => Build(
        TopicNames.ArtifactId(device.Id, TopicNames.SegmentedPcd),
        writer =>
        {
            WriteArtifactHead(writer, TopicNames.SegmentedPcd);
            writer.WriteString("segment_id", $"SEG-{index:D3}");
            writer.WriteString("storage_uri", $"file://ot-a/pcd/{at:yyyy'/'MM'/'dd}/{Short(_scanId)}/seg-{index:D3}.pcd");
            writer.WriteNumber("file_size_bytes", 12_000_000L + device.Next(0, 20_000_000));
            writer.WriteString("checksum", Checksum());
            writer.WriteNull("transformation_matrix");
            WriteArtifactTail(writer);
        });

    private void WriteArtifactHead(Utf8JsonWriter writer, string artifactType)
    {
        writer.WriteString("scan_id", _scanId);
        writer.WriteString("artifact_type", artifactType);
        writer.WriteString("hull_no", device.HullNo);
        writer.WriteString("block_id", device.BlockId);
    }

    private void WriteArtifactTail(Utf8JsonWriter writer)
    {
        writer.WriteString("produced_by_device_id", device.InferenceWs);
        writer.WriteString("model_version", "seg-v1.4.2");
        writer.WriteString("occurred_at", Iso(at));
        writer.WriteString("ingested_at", Iso(at.AddMilliseconds(device.Next(200, 2400))));
    }

    private string Checksum()
    {
        Span<char> hex = stackalloc char[16];
        for (var i = 0; i < hex.Length; i++)
        {
            hex[i] = "0123456789abcdef"[device.Next(0, 16)];
        }

        return $"sha256:{new string(hex)}";
    }

    private static void WriteNullableString(Utf8JsonWriter writer, string name, string? value)
    {
        if (value is null)
        {
            writer.WriteNull(name);
            return;
        }

        writer.WriteString(name, value);
    }

    private static string Short(string scanId) => scanId[..8];

    private static string Iso(DateTimeOffset value) => value.ToString("O", CultureInfo.InvariantCulture);

    private static byte[] Build(string id, Action<Utf8JsonWriter> writeRawPayload)
    {
        var buffer = new ArrayBufferWriter<byte>(1024);
        using (var writer = new Utf8JsonWriter(buffer, Json.WriterOptions))
        {
            writer.WriteStartObject();
            writer.WriteString("id", id);
            writer.WriteStartObject("raw_payload");
            writeRawPayload(writer);
            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return buffer.WrittenSpan.ToArray();
    }
}

/// <summary>한 토픽과, 그 토픽으로 태그를 만들 id 목록.</summary>
sealed record TopicSpec(string Topic, int Qos, IReadOnlyList<string> Ids);

/// <summary>
/// 발행 토픽에서 그대로 태그 정의 CSV 를 만든다. 발행기와 태그 목록이 따로 관리되면
/// 반드시 어긋나므로, 둘의 출처를 하나로 둔다.
/// </summary>
static class TagCatalog
{
    /// <summary>Engine 의 TagCatalogId 컬럼 상한 - MqttTagId.MaxLength 와 같은 값이다.</summary>
    private const int TagIdMaxLength = 100;

    private const string RawPayloadField = "raw_payload";

    /// <summary>UI 의 MQTT 태그 가져오기가 읽는 헤더. 한 파일이 정확히 한 토픽을 설명한다.</summary>
    private const string Header = "topic,qos,tagMode,deviceId,field,dataType,monitoring";

    public static IReadOnlyList<TopicSpec> Topics(Device[] fleet, Options o)
    {
        var specs = new List<TopicSpec>();

        foreach (var zone in fleet.Select(d => d.Zone).Distinct())
        {
            var zoneDevices = fleet.Where(d => d.Zone == zone).ToArray();

            specs.Add(new TopicSpec(
                TopicNames.Status(zone),
                o.Qos,
                [.. zoneDevices.Select(d => d.Id)]));

            // 스캔마다 stage 가 바뀌므로 한 장비의 태그가 그 구역의 stage 토픽 전부에 걸린다.
            foreach (var stage in Zones.StagesOf(zone))
            {
                specs.Add(new TopicSpec(
                    TopicNames.Actual(stage),
                    o.Qos,
                    [.. zoneDevices.Select(d => d.Id)]));
            }

            foreach (var group in zoneDevices.GroupBy(d => (d.Shop, d.Bay)))
            {
                specs.Add(new TopicSpec(
                    TopicNames.Artifact(zone, group.Key.Shop, group.Key.Bay),
                    o.Qos,
                    [.. group.SelectMany(d => TopicNames.ArtifactTypes.Select(t => TopicNames.ArtifactId(d.Id, t)))]));
            }
        }

        return specs;
    }

    /// <summary>
    /// UI 의 <c>tagFileName</c> 과 같은 규칙으로 짓는다 -
    /// <c>mqtt-tags_{edgeGroupId}_{topic}.csv</c>, 둘 다 토큰 불가 문자가 '_' 로 접힌다.
    /// UI 에서 내려받은 파일과 이름이 같아야 어느 쪽에서 나온 것이든 섞어 쓸 수 있다.
    /// </summary>
    private static string FileName(string edgeGroupId, string topic)
        => $"mqtt-tags_{Normalize(edgeGroupId)}_{Normalize(topic)}.csv";

    private static string Normalize(string value)
    {
        var builder = new StringBuilder(value.Length);
        foreach (var ch in value.Trim())
        {
            var safe = ch is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '-' or '_';
            if (safe)
            {
                builder.Append(ch);
                continue;
            }

            if (builder.Length > 0 && builder[^1] != '_')
            {
                builder.Append('_');
            }
        }

        return builder.ToString().Trim('_');
    }

    public static int Export(Device[] fleet, Options o, string directory)
    {
        Directory.CreateDirectory(directory);

        var specs = Topics(fleet, o);
        var tooLong = new List<string>();
        var written = 0;
        var rows = 0;

        foreach (var spec in specs)
        {
            // CRLF 로 쓴다 - Windows 의 Excel 이 .csv 에서 기대하는 줄바꿈이고,
            // UI 의 serializeTopicTagFile 도 같은 것을 쓴다.
            var builder = new StringBuilder();
            builder.Append(Header).Append("\r\n");

            foreach (var id in spec.Ids)
            {
                var tagId = $"{id}.{spec.Topic.Replace('/', '_')}.{RawPayloadField}";
                if (tagId.Length > TagIdMaxLength)
                {
                    tooLong.Add($"{tagId.Length}자  {tagId}");
                    continue;
                }

                builder.Append($"{spec.Topic},{spec.Qos},raw,{id},{RawPayloadField},STRING,true").Append("\r\n");
                rows++;
            }

            var path = Path.Combine(directory, FileName(o.EdgeGroupId, spec.Topic));
            File.WriteAllText(path, builder.ToString(), new UTF8Encoding(true));
            written++;
            Console.WriteLine($"{path}  ({spec.Ids.Count}건)");
        }

        Console.WriteLine();
        Console.WriteLine($"토픽 {written}개 · 태그 {rows:N0}개 생성.");

        if (tooLong.Count > 0)
        {
            Console.Error.WriteLine();
            Console.Error.WriteLine($"TagId 가 {TagIdMaxLength}자를 넘어 제외된 태그 {tooLong.Count}건:");
            foreach (var line in tooLong.Take(10))
            {
                Console.Error.WriteLine($"  {line}");
            }

            return 1;
        }

        return 0;
    }
}

sealed record Options(
    string Host,
    int Port,
    int AssemblyDevices,
    int OutfittingDevices,
    int Bays,
    string Site,
    string SiteCode,
    int IntervalMs,
    int StatusIntervalMs,
    int StaggerMs,
    int SegmentCount,
    string EdgeGroupId,
    int Qos,
    bool Retain,
    MqttProtocolVersion ProtocolVersion,
    string ClientId,
    string? Username,
    string? Password,
    bool DryRun,
    string? ExportTagsDir)
{
    public static Options? Parse(string[] args)
    {
        var host = "localhost";
        var port = 1884;               // EMQX(mqtt-emqx) 의 호스트 포트. 1883 은 mosquitto, 1885 는 NATS 다.
        var assembly = 210;            // 필드 정의서 4.2/5.2 - 조립 LiDAR
        var outfitting = 140;          // 필드 정의서 4.2/5.2 - 선행의장 LiDAR
        var bays = 7;
        var site = "geoje";
        var siteCode = "GJ";
        var interval = 60_000;         // 스캔(실적·산출물) 1분/대
        var statusInterval = 1_000;    // 상태 1초/대 - cdc 검토문서의 D1 수집 주기
        var stagger = 150;
        var segments = 10;             // 스캔당 세그먼트 5~20 가정의 중앙값
        var edgeGroupId = "edge.mqtt.p3.ot";   // Aspire Program.cs 의 MQTT Agent 그룹
        var qos = 1;
        var retain = false;
        var protocol = MqttProtocolVersion.V311;
        var clientId = $"lidar-sim-{Environment.ProcessId}";
        string? user = null;
        string? pass = null;
        var dryRun = false;
        string? exportTags = null;

        for (var i = 0; i < args.Length; i++)
        {
            string Next(string name) => i + 1 < args.Length
                ? args[++i]
                : throw new ArgumentException($"{name} 에 값이 필요합니다.");

            int NextInt(string name) => int.Parse(Next(name), CultureInfo.InvariantCulture);

            switch (args[i])
            {
                case "--host": host = Next("--host"); break;
                case "--port": port = NextInt("--port"); break;
                case "--assembly-devices": assembly = NextInt("--assembly-devices"); break;
                case "--outfitting-devices": outfitting = NextInt("--outfitting-devices"); break;
                case "--bays": bays = NextInt("--bays"); break;
                case "--site": site = Next("--site"); break;
                case "--site-code": siteCode = Next("--site-code"); break;
                case "--interval": interval = NextInt("--interval"); break;
                case "--status-interval": statusInterval = NextInt("--status-interval"); break;
                case "--stagger": stagger = NextInt("--stagger"); break;
                case "--segments": segments = NextInt("--segments"); break;
                case "--edge-group-id": edgeGroupId = Next("--edge-group-id"); break;
                case "--qos": qos = NextInt("--qos"); break;
                case "--retain": retain = true; break;
                case "--protocol":
                    protocol = Next("--protocol") switch
                    {
                        "311" or "3.1.1" => MqttProtocolVersion.V311,
                        "310" or "3.1" => MqttProtocolVersion.V310,
                        "500" or "5" or "5.0" => MqttProtocolVersion.V500,
                        var v => throw new ArgumentException($"알 수 없는 --protocol 값: {v}"),
                    };
                    break;
                case "--client-id": clientId = Next("--client-id"); break;
                case "--username": user = Next("--username"); break;
                case "--password": pass = Next("--password"); break;
                case "--dry-run": dryRun = true; break;
                case "--export-tags": exportTags = Next("--export-tags"); break;
                case "-h":
                case "--help":
                    PrintHelp();
                    return null;
                default:
                    Console.Error.WriteLine($"알 수 없는 옵션: {args[i]}");
                    PrintHelp();
                    return null;
            }
        }

        if (bays <= 0)
        {
            Console.Error.WriteLine("--bays 는 1 이상이어야 합니다.");
            return null;
        }

        return new Options(host, port, assembly, outfitting, bays, site, siteCode,
            interval, statusInterval, stagger, segments, edgeGroupId, qos, retain, protocol, clientId, user, pass, dryRun, exportTags);
    }

    private static void PrintHelp() => Console.WriteLine("""
        조립·선행의장 LiDAR 필드 데이터 MQTT 발행기 (3채널)

          --host <h>              브로커 호스트            (기본 localhost)
          --port <n>              브로커 포트              (기본 1884 — EMQX)
          --assembly-devices <n>  조립 LiDAR 수            (기본 210)
          --outfitting-devices <n> 선행의장 LiDAR 수       (기본 140)
          --bays <n>              구역당 bay 수            (기본 7)
          --site <s>              site 값                  (기본 geoje)
          --site-code <s>         장비 ID 의 사이트 코드    (기본 GJ)
          --interval <ms>         장비당 스캔 주기(ms)     (기본 60000 = 1분)
          --status-interval <ms>  장비당 상태 주기(ms)     (기본 1000 = 1초)
          --stagger <ms>          한 스캔 안 메시지 간격   (기본 150)
          --segments <n>          스캔당 세그먼트 PCD 수   (기본 10)
          --qos <0|1|2>           QoS                      (기본 1)
          --retain                retain 플래그
          --protocol <v>          MQTT 버전 311|310|500     (기본 311 — Mqtt.Agent 와 동일)
          --client-id <id>        MQTT client id
          --username <u>          인증 사용자
          --password <p>          인증 비밀번호
          --dry-run               연결 없이 샘플 페이로드만 출력
          --edge-group-id <id>    태그 CSV 파일명의 Edge 그룹  (기본 edge.mqtt.p3.ot)
          --export-tags <dir>     발행하지 않고 태그 정의 CSV 만 생성

        채널 (장비 1대 기준)
          ot/device/{zone}/lidar/status              상태          1건 / 상태 주기
          ot/sensor/{stage}/actual                   실적          1건 / 스캔 주기
          ot/pipeline/{zone}/{shop}/{bay}/artifact   산출물 메타  12건 / 스캔 주기
        """);
}
