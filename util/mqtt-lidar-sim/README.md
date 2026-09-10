# 조립·선행의장 LiDAR 필드 데이터 발행기

조립 210대 + 선행의장 140대, 총 **350대**의 LiDAR가 만드는 세 채널의 데이터를 MQTT 브로커로
실시간으로 흘려보낸다. Mqtt.Agent 를 실제 데이터로 돌려보기 위한 부하·시나리오 생성기다.

## 빠른 실행 (바탕화면 배치본)

`.cmd` 를 더블클릭하면 된다. 명령을 외울 필요 없다.

| 파일 | 하는 일 |
|---|---|
| `1-publish.cmd` | **기본 발행.** EMQX(1884)로 350대 × 22토픽, 약 426 msg/s |
| `2-publish-slow.cmd` | 저부하 발행. 상태·스캔 모두 10분 주기 → 약 8.2 msg/s |
| `3-export-tags.cmd` | 발행하지 않고 `tags\` 폴더에 태그 정의 CSV 만 생성 |
| `4-dry-run.cmd` | 브로커 연결 없이 페이로드 모양만 출력 |
| `5-help.cmd` | 전체 옵션 보기 |

중지는 창에서 **Ctrl+C**.

**미리 필요한 것**
- .NET SDK 10 이상 (`dotnet --version` 으로 확인. 현재 PC: 10.0.302)
- MQTT 브로커가 떠 있을 것. 이 저장소의 컨테이너 스택 기준 EMQX = **1884**, mosquitto = 1883, NATS = 1885

> 이 폴더는 원래 저장소의 `tools/mqtt-lidar-sim/` 에 있었다. git 미추적 파일이라 옮겨도 이력에 영향이 없다.
> 저장소 경로를 참조하는 설정은 없으므로 이 폴더째로 어디에 두든 동작한다.

---

## 실행

```bash
# 기본값 그대로 (EMQX localhost:1884, 350대, 상태 1초 + 스캔 1분 → 약 426 msg/s)
dotnet run tools/mqtt-lidar-sim/lidar-sim.cs

# 규모·주기 바꾸기
dotnet run tools/mqtt-lidar-sim/lidar-sim.cs -- --assembly-devices 210 --outfitting-devices 140 --status-interval 1000 --interval 60000

# 컨플 전제(전부 10분)로 되돌리기 → 약 8.2 msg/s
dotnet run tools/mqtt-lidar-sim/lidar-sim.cs -- --status-interval 600000 --interval 600000

# 연결 없이 페이로드 모양만 확인
dotnet run tools/mqtt-lidar-sim/lidar-sim.cs -- --dry-run --segments 2

# 태그 정의 CSV 만 생성 (발행하지 않음)
dotnet run tools/mqtt-lidar-sim/lidar-sim.cs -- --export-tags docs/mqtt-tags
```

`--help` 로 전체 옵션을 볼 수 있다. Ctrl+C 로 중지한다.

> **브로커 포트에 주의한다.** 이 저장소 주변에 MQTT를 받는 컨테이너가 셋 있다.
> `mqtt-emqx` = **1884**, `mqtt-mosquitto` = 1883, `mqtt-nats` = 1885.
> 기본값은 EMQX(1884)다. 컨테이너 안에서 실행할 때만 `mqtt-emqx:1883` 이 해석된다.

## 세 채널

**상태와 스캔은 박자가 다르다.** 하트비트는 초 단위로 계속 뛰고, 스캔은 분 단위로 한 번에
13건을 쏟아낸다. 그래서 장비마다 루프가 둘이다.

| 채널 | 토픽 | 주기 | 건수 | id |
|---|---|---|---|---|
| 장비 상태 | `ot/device/{zone}/lidar/status` | `--status-interval` 1초 | 1건 | `LDR-GJ-A1B3-07` |
| 실적 결과 | `ot/sensor/{stage}/actual` | `--interval` 1분 | 1건 | `LDR-GJ-A1B3-07` |
| 산출물 메타 | `ot/pipeline/{zone}/{shop}/{bay}/artifact` | `--interval` 1분 | 12건 | `LDR-GJ-A1B3-07-SEGMENTED_PCD` |

기본값 기준 부하는 상태 350 msg/s + 스캔 75.8 msg/s = 약 **426 msg/s** (실측 427).

> 상태 주기는 붙여준 두 문서가 서로 다르게 본다. 컨플은 "업데이트 주기 10분/대"로 잡고
> 시간당 29,400건을 계산했고, cdc 검토문서의 D1(장비 상태)은 수집 주기 1초에 평균 적재율
> 1,440행/초로 잡혀 있다. 기본값은 **1초** 쪽이다 — Agent 부하 시험에서 의미 있는 그림이
> 나오는 쪽이기 때문이다. `--status-interval 600000` 으로 컨플 전제로 되돌릴 수 있다.

산출물 12건은 정합 PCD 1 + 변환행렬 1 + 세그먼트 PCD 10(`--segments`)이다.
파일 본체는 보내지 않고 `storage_uri`·`file_size_bytes`·`checksum` 만 싣는다.
변환행렬만 숫자 16개뿐이라 예외적으로 메시지에 직접 들어간다.

`scan_id` 를 실적과 산출물이 공유한다 — 두 채널을 잇는 유일한 조인 키다.

## 계약

`Mqtt.Agent.Core.Domain.MqttPayloadParser` 가 받는 형식 그대로, **세 채널 모두** 같은 모양이다.

```json
{
  "id": "LDR-GJ-A1B3-07",
  "raw_payload": {
    "device_role": "LIDAR",
    "site": "geoje", "zone": "ASSEMBLY", "shop": "assembly1", "bay": "bay3",
    "status": "ONLINE",
    "last_heartbeat_at": "2026-09-10T12:59:23.3532050+09:00",
    "error_code": null,
    "occurred_at": "2026-09-10T12:59:23.3603189+09:00",
    "ingested_at": "2026-09-10T12:59:24.5303189+09:00",
    "idempotency_key": "LDR-GJ-A1B3-07:20260910T125923",
    "scan_rate_pts_per_sec": 229866,
    "temperature_c": 40.9,
    "connectivity_rssi": -52,
    "fov_mode": "wide"
  }
}
```

- 필드 정의서의 중첩 객체(`device_ids`)는 브로커로 나갈 때 평탄화한다 — Agent 의 계약이
  `raw_payload` 한 겹뿐이라 중첩을 두면 `tagMode=fields` 로 바꿨을 때 키 이름이 달라진다.
- `occurred_at` 이 태그의 `changedAt` 이 된다.
- 프로토콜은 3.1.1 로 고정돼 있다 — `MqttNetSession.BuildOptions` 와 같은 조건으로 붙기 위해서다.
  v5 로 붙으면 브로커가 CONNACK `0x01` 로 즉시 끊는다.

## 값이 움직이는 방식

장비마다 고정 시드의 난수를 들고 있어 350대가 한 몸처럼 오르내리지 않는다.
상태는 `ONLINE → CALIBRATING / ERROR → OFFLINE` 로 낮은 확률로 전이하고, 상태에 따라
스캔 속도·온도·정합 신뢰도가 함께 움직인다. `OFFLINE` 이면 `last_heartbeat_at` 이 멈추고
`scan_rate_pts_per_sec` 가 0 이 된다 — 죽은 장비를 다운스트림에서 구분할 수 있는 신호다.

전이 확률과 계측값의 진폭은 `Device.AdvanceHealth` 에 있고 **1초 간격을 전제로** 잡혀 있다.
장비 하나가 평균 80분에 한 번 보정에 들어가는 정도라, 350대 라인 전체로는 십수 초에 한 번꼴로
어딘가에서 상태가 바뀐다. `--status-interval` 을 크게 늘리면 그만큼 드물게 움직인다.

`block_progress_rate` 는 되돌아가지 않는다. `event_type` 이 `COMPLETE` 를 지나면 다음 블록으로
넘어가 0부터 다시 오른다. 스캔마다 `stage` 가 바뀌므로 한 장비의 태그가 그 구역의 stage 토픽
전부에 걸친다.

## 태그 등록

`--export-tags` 가 **발행 토픽에서 그대로** 태그 정의 CSV 를 만든다. 발행기와 태그 목록을
따로 관리하면 반드시 어긋나므로 출처를 하나로 뒀다.

```bash
dotnet run tools/mqtt-lidar-sim/lidar-sim.cs -- --export-tags docs/mqtt-tags
```

토픽 22개 · 태그 2,520개가 `docs/mqtt-tags/mqtt-tags_{토픽}.csv` 로 떨어진다.
UI 의 MQTT 태그 가져오기는 **한 파일이 한 토픽**이므로 파일을 토픽마다 따로 넣는다.
모든 행이 `tagMode=raw` 라 `raw_payload` 통째가 태그 하나(`{id}.{topic}.raw_payload`)의
STRING 값이 된다.

> **TagId 100자 상한에 주의한다.** Engine 의 `TagCatalogId` 컬럼 제약이자 `MqttTagId.MaxLength` 다.
> 산출물 채널은 id 에 `-TRANSFORMATION_MATRIX` 가 붙고 토픽에 shop·bay 까지 들어가 현재 최대 97자로,
> 여유가 3자뿐이다. shop·bay 이름을 늘리면 넘치고, 넘친 태그는 `--export-tags` 가 제외하며 오류로 끝난다.

## 미확정

아래는 필드 정의서에 없어 임시로 채운 값이다. 확정되면 해당 지점만 고치면 된다.

| 항목 | 현재 값 | 위치 |
|---|---|---|
| 조립 stage 4종 | ARRANGEMENT, FITTING, WELDING, INSPECTION | `Zones.StagesOf` |
| 의장 stage 2종 | WIRING(전장), PIPING(관철) | `Zones.StagesOf` |
| shop·bay 구성 | assembly1 × bay1–7, outfitting1 × bay1–7 | `Fleet.Build`, `--bays` |
| 상태 주기 | 1초/대 (cdc 문서 D1 기준. 컨플은 10분으로 봄) | `--status-interval` |
| 스캔 주기 | 1분/대 (컨플은 10분/대 — 부하를 보려고 줄여 둠) | `--interval` |
| 스캔당 세그먼트 | 10 (5~20 가정의 중앙값) | `--segments` |

페이로드 규격 자체도 필드 정의서 §6.2~§6.4 의 **제안 규격**이며 ISL 벤더 합의 전 미확정이다.
