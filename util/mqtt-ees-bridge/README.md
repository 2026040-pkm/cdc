# MQTT → Kafka(EES) 브리지

EMQX 에 올라오는 LiDAR 350대의 세 채널을 구독해 **InterSysLink 의 EES Kafka Provider 가
내보내는 것과 같은 레코드**로 바꿔, 이 스택이 띄우는 Kafka 에 넣는다. Kafka UI 로 눈으로 본다.

Aspire(InterSysLink) 를 띄우지 않아도 `ot.lidar.*` 세 토픽이 같은 모양으로 채워진다.
수신 측(`src/timescaledb` 의 lidar-ingest)은 원본과 이것을 구분하지 못한다 — 구분할 근거가
레코드에 없다.

```
 lidar-sim.cs        EMQX          이 스택                       수신 측
 350대 · 22토픽  →  :1884  →  ees-bridge → Kafka :61092  →  lidar-ingest → TimescaleDB
 (util/mqtt-lidar-sim)        (형식 변환)   + Kafka UI :61080     (src/timescaledb)
```

## 빠른 실행

```powershell
scripts\up.ps1              # Kafka + Kafka UI + 브리지 기동, 토픽 3개 생성
..\mqtt-lidar-sim\1-publish.cmd   # 발행기를 돌려야 데이터가 흐른다
scripts\verify.ps1          # 오프셋·지표·레코드 한 건으로 확인
```

| 주소 | |
|---|---|
| http://localhost:61080 | Kafka UI — Topics → `ot.lidar.status` → Messages |
| http://localhost:61090/metrics | 브리지 지표 |
| `localhost:61092` | Kafka (호스트에서 붙을 때). 컨테이너끼리는 `kafka:9093` |

정지는 `scripts\down.ps1`, 볼륨까지 지우려면 `scripts\down.ps1 -Volumes`.
bash 판(`up.sh` · `down.sh` · `verify.sh`)도 같이 있다.

`verify` 안에 **계약 검사**가 들어 있다 — 넣은 레코드를 수신 측 규칙(`ingest.py` 의
parse_record · parse_item)대로 다시 읽어 헤더 11개의 순서, Key, ns epoch, 숫자 tid,
채널별 필수 키, tid 가 조회표로 풀리는지까지 본다. 따로 돌릴 수도 있다.

```powershell
podman exec ees-bridge python check_contract.py
```

## 무엇이 어떻게 바뀌는가

MQTT 메시지 하나가 EES **항목** 하나가 되고, 항목 여럿이 레코드 하나로 묶인다.

```jsonc
// MQTT  ot/device/assembly/lidar/status
{"id": "LDR-GJ-A1B3-07", "raw_payload": {"device_role": "LIDAR", "status": "ONLINE", ...}}

// Kafka  ot.lidar.status   (Key = "ot.lidar.status")
[{"content": {"timestamp": 1789012763353205000,          // raw_payload.occurred_at 의 ns epoch
              "tid": "1",                                 // 숫자 ParameterId. TagId 가 아니다
              "value": "{\"device_role\":\"LIDAR\",...}",  // raw_payload 통째의 JSON 문자열
              "pm_mode": false}}, ...]
```

헤더는 11개로 고정이다 (`EesHeaderBuilderV1_0`). 값은 ISL 등록부의 실제 설정
(`EES_MESSAGE.ConfigJson`)에서 그대로 가져왔다.

| 헤더 | 값 |
|---|---|
| `project_id` · `infra_proc_name` · `task_area_code` · `request` | `ot.lidar.status` (채널마다 send_topic 과 같은 값) |
| `data_type` | `value` |
| `method_id` | `TRACE` — `sendPolicy.mode=OnChange` 이므로. 주기 스냅샷이면 `BATCH` |
| `message_version` | `1.0` |
| `msg_timestamp` | 레코드를 만든 시각의 ns epoch |
| `uniqueid` | 레코드마다 새 GUID |
| `reply` · `replycluster` | 빈 값 |

| 채널 | MQTT 토픽 | Kafka 토픽 | 발행 주기 | Kafka 레코드 |
|---|---|---|---|---|
| 장비 상태 | `ot/device/{zone}/lidar/status` | `ot.lidar.status` | 1건/1초·대 | 1개/초 · 350항목 |
| 실적 결과 | `ot/sensor/{stage}/actual` | `ot.lidar.actual` | 1건/1분·대 | 1개/초 · 6항목 |
| 산출물 메타 | `ot/pipeline/{zone}/{shop}/{bay}/artifact` | `ot.lidar.artifact` | 12건/1분·대 | 1개/초 · 70항목 |

**MQTT 메시지 하나가 Kafka 레코드 하나가 아니다.** 채널마다 따로 묶어 `BATCH_MAX_ITEMS`
(500) 또는 `BATCH_MAX_WAIT_MS`(1000) 중 먼저 오는 쪽에서 끊는데, 지금 유량에서는 늘 시간
쪽이 먼저다 — 그래서 **토픽마다 초당 레코드 하나**가 나가고 채널 차이는 레코드당 항목 수로
나타난다. 발행기가 장비마다 시작을 밀어 두어(`offset = interval × index / 350`) 1분 주기인
실적·산출물도 한 번에 터지지 않고 초당 76건씩 고르게 들어오기 때문이다.

주기 1초·1분은 레코드 빈도가 아니라 **항목의 `timestamp`**(`raw_payload.occurred_at` 의 ns
epoch)에 남는다. 수신 측이 레코드를 풀어 항목 단위로 넣으므로 DB 에서는 원래 주기가 그대로
보인다.

> 위 표는 발행기 **기본값(장비마다 시차를 두고 발행)** 기준이다. `--burst` 로 돌리면 350대가
> 같은 순간에 쏘므로 1분 주기 채널이 60초에 한 번만 터지고, 그때는 `BATCH_MAX_WAIT_MS` 가
> 아니라 `BATCH_MAX_ITEMS`(500) 가 레코드를 끊는다. 2026-09-11 실측 (70초 · `--burst`):
>
> | 토픽 | 항목/초 | 레코드/초 | 레코드당 항목 |
> |---|---|---|---|
> | `ot.lidar.status` | 350 | 0.80 | 437 |
> | `ot.lidar.artifact` | 60 | 0.13 (60초에 9개) | 500 × 8 + 200 |
> | `ot.lidar.actual` | 5 | 0.01 (60초에 1개) | 350 |
>
> status 가 레코드당 350 이 아니라 437 인 것은 1초 배치 창과 1초 발행 클럼프의 위상이 어긋나
> 이따금 두 클럼프가 한 창에 들어오기 때문이다. 항목 수(=적재 행)는 어느 쪽이든 같다.

이 알갱이는 ISL Provider 의 관측값(레코드당 ~210항목 · 1~3초 간격)과 같은 자릿수로 맞춘
것이다. `BATCH_MAX_WAIT_MS` 를 200 으로 줄이면 항목 수는 그대로인데 레코드가 5배로 잘아져,
레코드 수로 눈금을 잡아 둔 수신 측 Kafka 지연 임계(`LidarKafkaLagHigh`)의 의미가 달라진다.

**토픽이 파티션 1개인 이유.** Provider 는 Kafka Key 를 정규화한 `project_id` 로 쓰는데
그 값이 채널마다 상수다. 파티션을 늘려도 레코드는 전부 한 파티션에 간다 — 계약의 성질이지
설정 실수가 아니다.

## 숫자 tid — 이 표가 없으면 장비 축이 사라진다

Provider 는 태그를 실을 때 문자열 TagId 를 버리고 등록부의 **ParameterId(숫자)** 만 싣는다.
`raw_payload` 안에도 장비 id 필드가 없다. 즉 Kafka 만 읽어서는 어느 장비인지 알 수 없고,
수신 측은 `lidar_tag_catalog` 로 (토픽, 숫자) → 장비를 되돌린다.

그래서 이 브리지도 숫자를 지어내지 않는다. ISL 등록부에서 뽑은 표를 그대로 쓴다.

```
MQTT  {"id": "LDR-GJ-A1B3-07"}  on  ot/device/assembly/lidar/status
  → TagId  LDR-GJ-A1B3-07.ot_device_assembly_lidar_status.raw_payload
  → 조회표 ("ot.lidar.status", 1)
  → tid    "1"
```

표는 `tags/tag-catalog.json` 이고, 만드는 것은 `scripts/export-tag-map.py` 다.

```powershell
python scripts\export-tag-map.py                     # 03-tag-catalog.sql 에서 (기본)
python scripts\export-tag-map.py --db <isl-secondary.db>   # ISL 등록부에서 직접
podman restart ees-bridge                            # 다시 읽힌다 (이미지를 다시 굽지 않는다)
```

기본 원본을 `src/timescaledb/infra/db/init/03-tag-catalog.sql` 로 잡은 것은 **수신 측 카탈로그와
같은 파일**이기 때문이다. 한쪽만 갱신해서 어긋나는 일이 생기지 않는다.

### 등록부에 없는 태그

`AUTO_REGISTER_TAGS=true`(기본)면 `AUTO_PARAM_ID_BASE`(기본 900000)부터 번호를 새로 매겨
내보내고, `/state/auto-tags.json` 에 적어 재기동해도 같은 번호를 준다. 같이 쓰는
`/state/auto-tags.sql` 을 수신 측 DB 에 실행하면 장비 축이 살아난다.

```powershell
podman exec ees-bridge cat /state/auto-tags.sql | podman exec -i tsdb-lidar-pg psql -U postgres -d lidar
```

`false` 면 버리고 센다 — 지금 ISL 이 하는 그대로다.

> **지금 등록부에는 `ot/sensor/fitting/actual` 이 없다.** 조립 stage 4종(ARRANGEMENT ·
> FITTING · WELDING · INSPECTION) 중 FITTING 만 태그 등록이 빠져 있어, 조립 210대의 FITTING
> 실적이 ISL 에서는 조용히 사라진다. 등록은 2,310개(상태 350 + 실적 910 + 산출물 1,050)인데,
> 실적은 조립 210×4 + 의장 140×2 = 1,120 이어야 한다 — 차이 210 이 그대로 FITTING 이다.
> 기본값이면 여기서는 살아서 나가고, 그 사실이
> `ees_bridge_tag_unregistered_total` 과 로그에 남는다.

## 수신 측을 이쪽 Kafka 로 돌리기

`src/timescaledb` 의 lidar-ingest 는 `kafka:9093` 을 찾는다. 이 브로커가 그 이름 그대로
광고하므로, `tsdb-net` 에 alias `kafka` 로 붙이기만 하면 소비자 설정을 한 줄도 안 고치고
Aspire Kafka 대신 이쪽을 읽는다.

```powershell
scripts\up.ps1 -Tsdb
```

붙어 있던 Aspire Kafka(`kafka-xxxxxxxx`)를 먼저 떼고 이쪽을 붙인 뒤 소비자를 재기동한다.
같은 네트워크에 alias `kafka` 가 둘이면 소비자가 어느 쪽에 붙을지 알 수 없기 때문이다.
되돌리려면 `scripts\down.ps1` 뒤 `src/timescaledb/scripts/up.ps1` 를 다시 돌린다.

소비자의 오프셋은 브로커마다 따로다. 브로커를 바꾸면 `KAFKA_AUTO_OFFSET_RESET=earliest`
설정에 따라 이 토픽에 쌓인 것을 처음부터 다시 읽는다.

## 설정

`docker-compose.yml` 의 `ees-bridge` 환경변수로 바꾼다. 자주 만지는 것만 적는다.

| 변수 | 기본값 | |
|---|---|---|
| `MQTT_HOST` · `MQTT_PORT` | `host.docker.internal` · `1884` | EMQX. 브로커 스택 네트워크에 넣는 대신 호스트 포트로 붙는다 |
| `MQTT_TOPICS` | 세 패턴 | 22개 토픽을 `+` 로 덮는다 |
| `MQTT_QOS` | `1` | 발행기와 같게 |
| `BATCH_MAX_ITEMS` · `BATCH_MAX_WAIT_MS` | `500` · `1000` | 먼저 오는 쪽에서 레코드를 끊는다. 아래 참고 |
| `AUTO_REGISTER_TAGS` | `true` | 위 참고 |
| `EES_METHOD_ID` | `TRACE` | 주기 스냅샷 시나리오면 `BATCH` |

브로커를 `util/docker-mqtt` 스택이 아닌 곳에 두었다면 `MQTT_HOST` 만 바꾸면 된다.
같은 podman 안의 브로커에 컨테이너 이름으로 붙이려면 compose 에 그 네트워크를
`external: true` 로 추가하고 `MQTT_HOST=mqtt-emqx`, `MQTT_PORT=1883` 으로 둔다.

## 지표

`http://localhost:61090/metrics`

| 지표 | |
|---|---|
| `ees_bridge_mqtt_messages_total{channel}` | 받은 MQTT 메시지 |
| `ees_bridge_mqtt_drops_total{reason}` | 못 보낸 것 — 이유별 |
| `ees_bridge_records_total{topic}` · `ees_bridge_items_total{topic}` | 만든 레코드 · 항목 |
| `ees_bridge_delivered_total` · `ees_bridge_delivery_errors_total` | 브로커가 받은 것 · 실패 |
| `ees_bridge_record_items` · `ees_bridge_record_bytes` | 레코드 하나의 항목 수 · 크기 |
| `ees_bridge_handoff_seconds` | 장비의 `occurred_at` → Kafka 송신까지 |
| `ees_bridge_tag_catalog_rows` · `ees_bridge_tag_auto_rows` | 등록부에서 읽은 태그 · 직접 번호를 매긴 태그 |
| `ees_bridge_tag_unregistered_total{channel}` | 등록부에 없던 태그로 온 메시지 |
| `ees_bridge_mqtt_connected` | MQTT 세션이 붙어 있으면 1 |
| `ees_bridge_pending_items{channel}` | 배치에 쌓여 있는 항목 |

## 알아 둘 것

- **전달 보장은 at-least-once 다.** Kafka 쪽은 `enable.idempotence` + `acks=all` 로 재시도가
  중복을 남기지 않고, MQTT 쪽은 QoS 1 이라 브로커가 다시 보낼 수 있다. 같은 항목이 두 번
  실려도 수신 측의 `(idempotency_key, time)` 유니크 인덱스가 걸러 낸다.
- **토픽을 지워도 발행만으로는 다시 안 생긴다.** 브로커가 `auto.create.topics.enable=false`
  라(오타 난 토픽이 조용히 생기는 것을 막으려는 설정) 초당 426건이 계속 들어와도 토픽은
  돌아오지 않는다. 그래서 브리지가 직접 만든다 — 기동할 때 한 번, 그리고 송신이 실패할 때
  (`ensure_topics`, 1파티션 · 24시간 보존으로 `kafka-init` 과 같은 모양).
  실패가 두 모양으로 온다.

  | 언제 | 오류 | 어디서 잡히나 |
  |---|---|---|
  | 기동할 때부터 없음 | `_UNKNOWN_TOPIC` | `produce()` 가 곧바로 던진다 |
  | 돌아가는 중에 지움 | `_UNKNOWN_PARTITION` | `produce()` 는 조용히 받고 전달 콜백으로 온다 |

  두 번째가 함정이다 — librdkafka 가 메타데이터를 들고 있어 파티션 수만 1→0 이 되므로
  `produce()` 는 예외를 내지 않는다. 그래서 전달 콜백에서도 재생성을 건다.
  **복구는 그 채널의 다음 발행 시도 때 일어난다.** 1분 주기 채널이면 최대 1분이 걸린다
  (2026-09-11 실측: `ot.lidar.actual` 삭제 → 38초 뒤 복구, 그동안 잃은 레코드 1개).
  못 보낸 레코드는 `ees_bridge_delivery_errors_total` 로 센다. 손으로 되돌리려면
  `scripts/up.ps1` 을 다시 돌려 `kafka-init` 에게 맡겨도 된다.
- **MQTT 세션은 clean session 이다.** 브리지가 죽어 있는 동안 온 메시지는 잃는다. 초당
  426건이라 브로커 큐(EMQX 기본 1000)를 금방 넘기므로, 쌓다가 한꺼번에 버리는 것보다
  흘려보내는 쪽을 골랐다. `MQTT_CLEAN_SESSION=false` 로 바꿀 수 있다.
- **`value` 는 받은 바이트 그대로가 아니라 다시 직렬화한 JSON 문자열이다.** 키 순서는 받은
  그대로 두고 공백만 없앤다. 발행기도 공백 없이 쓰므로 실질적으로 같은 문자열이다.
- **podman machine 메모리가 2GiB 다.** Kafka 힙을 512m, Kafka UI 를 384m 로 못 박아 두었다.
  다른 스택(Aspire · timescaledb · embedded-cdc-tsdb)이 같이 떠 있으면 빠듯하다 —
  안 쓰는 스택을 내리고 올리는 편이 낫다.
- 포트는 61xxx 대를 쓴다. 56xxx~60xxx 는 다른 스택이 이미 쓰고 있다.
