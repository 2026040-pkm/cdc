# timescaledb — OT LiDAR 필드 데이터 → TimescaleDB 적재 + 모니터링

Aspire(InterSysLink) 가 Kafka 로 흘려보내는 조립·선행의장 LiDAR 필드 데이터를
PostgreSQL 17 + TimescaleDB 2.29 에 쌓고, 그 DB 와 적재 파이프라인을 Prometheus · Grafana 로 본다.

```
LiDAR 350대 ──MQTT──▶ Mqtt.Agent ──▶ Engine ──▶ EES Kafka Provider
                                                       │
                              Aspire(InterSysLink) Kafka ──▶ lidar-ingest ──▶ TimescaleDB(lidar)
                                                                  │                 │
                                                               /metrics       postgres_exporter
                                                                  └────▶ Prometheus ◀────┘
                                                                              │
                                                                           Grafana
```

구독할 Kafka 토픽은 `kafka.env` 의 `KAFKA_TOPIC` 이 정한다 — `ot.lidar.status` · `ot.lidar.actual` ·
`ot.lidar.artifact` 셋이다. 발행 토픽 이름은 InterSysLink 의 EES 메시지 설정
(`msgHeaderFormat.topic.send_topic`)이 정하므로 MQTT 토픽과 다르고, **채널마다 EES 메시지를 따로
만들어 두어 토픽 하나에 채널 하나**가 실려 온다 — 그래서 채널 판정은 토픽 이름의 마지막 마디로 한다.

**장비 id 는 Kafka 선로에 실리지 않는다.** `content.tid` 는 문자열 TagId 가 아니라 EES ParameterId
(숫자)이고 `raw_payload` 안에도 장비 필드가 없다. 그래서 InterSysLink 등록부를 옮겨 둔
`lidar_tag_catalog` 가 숫자를 장비로 되돌린다(아래).

## 기동 · 정지

```bash
scripts/up.sh        # 또는 scripts\up.ps1 — DB + 소비자 + 모니터링, Kafka 컨테이너를 tsdb-net 에 붙인다
scripts/verify.sh    # 오프셋 · 표별 행 수 · 채널 분포 · 장비 상태 · 정책 잡 · 소비자 지표 한 번에
scripts/down.sh      # 정지 (데이터 유지).  -v 를 주면 볼륨까지 삭제 → init SQL 이 다시 돈다
```

| 서비스 | 주소 |
|---|---|
| TimescaleDB | `localhost:59432` · DB `lidar` · postgres / postgres |
| lidar-ingest | http://localhost:59080/metrics |
| Prometheus | http://localhost:59090 |
| Grafana | http://localhost:59380 · admin / admin (익명 Viewer 허용) |

Kafka 는 이 스택이 띄우지 않는다. Aspire 세션이 띄우는 `kafka-xxxxxxxx` 컨테이너를 쓴다.
**Aspire 세션을 다시 띄웠으면 `scripts/up.sh` 를 다시 실행한다** — 새 Kafka 컨테이너를 다시 붙여야 한다.

### 왜 Kafka 컨테이너를 이쪽 네트워크에 붙이는가

브로커는 자기 네트워크 안에서 `kafka:9093` 으로 광고된다. 호스트의 `127.0.0.1:9092` 는 Aspire 프록시라
podman VM 안 컨테이너에서는 보이지 않는다. 소비자를 Aspire 세션 네트워크에 넣어 보면 **Aspire 가 5초 안에
낯선 컨테이너를 떼어 낸다** (`podman events --filter type=network` 로 확인, 2026-09-09). 반대로 Kafka
컨테이너를 `tsdb-net` 에 alias `kafka` 로 붙이면 떼지 않는다. `up` 이 그 일을 하고 `down` 이 되돌린다.

## 메시지 → 표

레코드 하나 = 태그 여러 개의 값 배열(EES Kafka Provider · `data_type=value` · V1.0).
헤더는 11개 계약이고 그중 `uniqueid` · `msg_timestamp` · `message_version` · `method_id` ·
`data_type` · `project_id` · `infra_proc_name` · `task_area_code` · `request`(= `send_topic`) 를 받는다.

```json
[{"content": {"timestamp": 1789030763360318900, "pm_mode": false,
              "tid": "135",
              "value": "{\"device_role\":\"LIDAR\",\"site\":\"geoje\",\"zone\":\"ASSEMBLY\",
                        \"shop\":\"assembly1\",\"bay\":\"bay3\",\"status\":\"ONLINE\",
                        \"last_heartbeat_at\":\"2026-09-10T12:59:23.3532050+09:00\",\"error_code\":null,
                        \"occurred_at\":\"2026-09-10T12:59:23.3603189+09:00\",
                        \"ingested_at\":\"2026-09-10T12:59:24.5303189+09:00\",
                        \"idempotency_key\":\"LDR-GJ-A1B3-07:20260910T125923\",
                        \"scan_rate_pts_per_sec\":229866,\"temperature_c\":40.9,
                        \"connectivity_rssi\":-52,\"fov_mode\":\"wide\"}"}}, ...]
```

### 채널은 토픽이 말한다

| Kafka 토픽 | MQTT 토픽 | 주기 · 건수 | 표 |
|---|---|---|---|
| `ot.lidar.status` | `ot/device/{zone}/lidar/status` | 1초 · 1건/대 | `tsdb.lidar_status` |
| `ot.lidar.actual` | `ot/sensor/{stage}/actual` | 1분 · 1건/대 | `tsdb.lidar_scan_actual` |
| `ot.lidar.artifact` | `ot/pipeline/{zone}/{shop}/{bay}/artifact` | 1분 · 12건/대 | `tsdb.lidar_scan_artifact` |

세 채널 모두 `tagMode=raw` 다 — `content.value` 는 `raw_payload` 객체 통째의 JSON 문자열이고
채널마다 키가 다르다. 마지막 마디가 셋 중 하나가 아닌 토픽에서 온 레코드는 통째로
`lidar_ingest_reject` 로 격리한다 — 조용히 엉뚱한 표에 넣지 않기 위해서다.

`scan_id` 가 실적과 산출물을 잇는 유일한 조인 키다.

### 장비는 태그 카탈로그가 말한다

`content.tid` 는 **숫자**다(`"135"` · `"2257"`). Provider 가 값을 실을 때 문자열 TagId 를 버리고
EES ParameterId 만 쓰기 때문이다(`ValueMessageFormatterV1_0`). `raw_payload` 안에도 장비 id 필드가
없다 — 즉 **Kafka 만 읽어서는 어느 장비인지 알 수 없다.**

그래서 InterSysLink 의 태그 등록부(`EES_TAG` × `TAG_CATALOG`)를 `lidar_tag_catalog` 로 옮겨 두고,
소비자가 기동할 때 통째로 읽어 `(send_topic, param_id)` → 장비 id 로 푼다. 표에는 문자열 TagId ·
MQTT 토픽 · 채널 · 산출물 종류도 같이 들어 있다. 산출물 태그 id 에 붙는 `-REGISTERED_PCD` 같은
접미사는 카탈로그가 미리 떼어 `tid`(장비 축)와 `device_id`(태그 그대로)로 갈라 둔다.

```bash
python scripts/export-tag-catalog.py     # 등록부 → infra/db/init/03-tag-catalog.sql (태그 2,310 · 장비 350)
scripts/down.sh -v && scripts/up.sh      # init SQL 은 볼륨이 새로 만들어질 때만 돈다
```

기본 원본은 `D:\git\intersyslink-v4\src\Engine\Engine\bin\Debug\net10.0\isl-secondary.db` 다
(`--db` 로 바꾼다). Engine 이 원격이면 같은 값을 REST 로 받을 수 있다 —
`GET /api/workflows/{workflowId}/ees-messages/{messageId}/ees-tag-settings`.

카탈로그에 없는 숫자를 만나도 적재는 멈추지 않는다. 상태 채널은 `idempotency_key`
앞부분에서 장비 id 를 되찾고, 실적·산출물은 `#2257` 처럼 숫자를 그대로 장비 축에 둔다.
그 수는 `lidar_ingest_tag_unresolved_total` 과 `verify` 의 `unresolved devices` 로 보인다.

스키마는 둘이다. 나누는 선은 "행 단위 CDC 가 성립하는가" 다 — `tsdb` 는 하이퍼테이블과 연속 집계라
CDC 대상이 아니고, `rdb` 는 publication 에 실려 CDC 가 옮긴다. 조회하는 쪽이 스키마를 몰라도 되도록
`ALTER DATABASE lidar SET search_path = tsdb, rdb, public` 을 걸어 둔다(01-schema.sql).

| 표 | 성격 | 내용 |
|---|---|---|
| `tsdb.lidar_status` | 하이퍼테이블 · 청크 6h · 압축 3일 후 · 보존 31일 | 상태 항목 하나 = 행 하나. `time` = `occurred_at` |
| `tsdb.lidar_scan_actual` | 하이퍼테이블 · 청크 1일 · 압축 3일 후 · 보존 31일 | 실적 항목 하나 = 행 하나. 호선·블록·공정·진척률·정합 신뢰도 |
| `tsdb.lidar_scan_artifact` | 하이퍼테이블 · 청크 6h · 압축 3일 후 · 보존 31일 | 산출물 메타 하나 = 행 하나. 파일 본체는 오지 않고 `storage_uri` 만 온다 |
| `rdb.lidar_status_message` | 일반 테이블 | Kafka 레코드 하나 = 행 하나. 파티션·오프셋·헤더·채널별 항목 수·격리 수 |
| `rdb.lidar_tag_catalog` | 일반 테이블 · 태그당 1행 | 숫자 `tid` → 장비 id·TagId·MQTT 토픽·채널. InterSysLink 등록부의 사본 |
| `rdb.lidar_device_state` | 일반 테이블 · 장비당 1행 | 장비별 최신 상태. exporter 와 대시보드가 하이퍼테이블 대신 이것을 읽는다 |
| `rdb.lidar_ingest_reject` | 일반 테이블 | 파싱 실패 항목의 원문과 사유 |
| `tsdb.lidar_status_1m` | 연속 집계 | 장비별 1분: 온도·스캔 속도·RSSI, 상태별 건수 |
| `tsdb.lidar_error_1m` | 연속 집계 | 오류 코드별 1분 건수·장비 수 |
| `tsdb.lidar_scan_1m` | 연속 집계 | 장비·공정별 1분: 정합 신뢰도, 진척률, COMPLETE 건수 |
| `tsdb.lidar_artifact_1m` | 연속 집계 | 산출물 종류별 1분 건수·바이트 |

멱등성: `(idempotency_key, time)` 유니크 인덱스 + `ON CONFLICT DO NOTHING`. Kafka 오프셋은 DB 커밋 뒤에만
커밋한다(at-least-once). MQTT 가 QoS 1 이라 중복은 예외가 아니라 정상 동작이다 —
대시보드의 "중복 스킵" 이 그것이고 유실이 아니다. 산출물 채널만 발신 측 멱등 키가 없어
소비자가 `{scan_id}:{artifact_type}:{segment_id}` 로 만든다.

DDL 은 `infra/db/init/01-schema.sql`, 정책은 `02-policies.sql`, 태그 카탈로그 시드는 `03-tag-catalog.sql`
(자동 생성). 스키마를 고쳤으면 `down.sh -v` 로 볼륨을 지워야 반영된다.
CDC 원천으로 여는 설정은 `04-cdc-lidar.sql` 이고, 하이퍼테이블 셋은 publication 에 넣지 않는다
(청크·압축·보존이 행 단위 변경으로 보이지 않는다 — `docs/timescaledb-cdc-impact.html` B안).
그 네 표(`rdb.lidar_device_state` · `rdb.lidar_status_message` · `rdb.lidar_ingest_reject` · `rdb.lidar_tag_catalog`)를
받는 쪽은 `src/embedded-cdc` 와 `src/embedded-cdc-tsdb` 의 `infra/db/target/init/02-lidar.sql` 과
`LidarPassthroughHandlers` 의 컬럼 목록이다 — **원천 컬럼을 고치면 네 곳을 같이 고쳐야 한다.**

## 모니터링

| 대시보드 | 내용 |
|---|---|
| **OT LiDAR 적재 (Kafka → TimescaleDB)** | 적재 행/초 · Kafka 지연 · end-to-end 지연 · 배치 시간 · 중복/격리, 채널별 수신 항목/초, 상태별 장비 수, 장비 최신 상태 표, 장비별 온도·스캔 속도·정합 신뢰도 추이(`$tid`), 오류 코드별 발생 |
| **TimescaleDB 상태 (lidar)** | DB·하이퍼테이블 크기, 청크·압축률, TPS, 튜플 삽입 vs 적재 행, 캐시 적중률, WAL, 연결·락·dead tuple, 체크포인트, 백그라운드 워커, 디스크, 정책 잡 실행 표 |

데이터소스는 둘이다. Prometheus 는 집계(상태별 대수 등) 만 들고, 장비 단위 추이는 TimescaleDB 직결로
연속 집계를 SQL 로 읽는다 — 장비 350대 × 필드를 Prometheus 레이블로 올리면 시계열이 터진다.

exporter 커스텀 쿼리(`infra/monitoring/exporter/queries-tsdb.yaml`): 하이퍼테이블 크기·청크·압축 통계,
정책 잡 상태, 장비 상태 집계, WAL, 백그라운드 워커. 5초 스크랩이라 하이퍼테이블 전체를 훑는 쿼리는 없다.

경보(`infra/monitoring/prometheus/rules/tsdb-alerts.yml`): 소비 정지 · Kafka 지연 · end-to-end 지연 · 격리 발생 ·
DB 오류, ERROR 장비 수 · 무응답 장비 · 데이터 신선도, DB down · 정책 잡 실패 · 디스크 · 연결 수 · 캐시 적중률.

대시보드 JSON 옆의 `.outline.txt` 는 `../embedded-cdc/scripts/dashboard-outline.js --write` 로 만든 개요다.
JSON 을 고쳤으면 같이 갱신한다.

## 소비자 (dev/ingest)

Python · confluent-kafka · psycopg3. 레코드 최대 50개 또는 1초마다 한 트랜잭션으로
채널별 INSERT(`lidar_status` · `lidar_scan_actual` · `lidar_scan_artifact`) →
배치 안 장비별 최신 상태로 `lidar_device_state` UPSERT(오래된 이벤트는 못 덮음) →
`lidar_status_message` INSERT → 격리 INSERT → COMMIT → Kafka 오프셋 커밋. DB 오류는 같은 배치를 물고
재시도하고, Kafka 오류(브로커 해석 실패 등)는 죽지 않고 재시도한다.

지표는 채널 축(`lidar_ingest_items_total{channel=}`)과 표 축(`lidar_ingest_table_rows_total{table=}`)으로 나뉜다.

환경변수는 `kafka.env` 와 `dev/docker-compose.app.yml` 참고.

## 규모 (발행기 기본값 · `intersyslink-v4/tools/mqtt-lidar-sim`)

- 장비 350대 = 조립 210(`assembly1` × `bay1~7`) + 선행의장 140(`outfitting1` × `bay1~7`).
- 상태 1초/대 = 350 msg/s, 스캔 1분/대 × 13건 = 75.8 msg/s → 합계 약 426 msg/s.
- 상태 전이 `ONLINE ↔ CALIBRATING` · `ONLINE → ERROR → OFFLINE`. `OFFLINE` 이면 하트비트가 멈추고
  `scan_rate_pts_per_sec` 가 0 이 된다 — 죽은 장비를 구분하는 신호다.
- 오류 코드: E-NET-0007, E-LDR-0101 / 0203 / 0311 / 0402.
- 공정(stage): 조립 ARRANGEMENT · FITTING · WELDING · INSPECTION, 의장 WIRING · PIPING.
- 산출물 12건 = 정합 PCD 1 + 변환행렬 1 + 세그먼트 PCD 10.

발행기 옵션(`--status-interval` · `--interval` · `--segments`)을 바꾸면 이 값이 전부 바뀐다.
페이로드 규격 자체가 ISL 벤더 합의 전 제안 규격이므로, 확정되면 `01-schema.sql` 의 컬럼과
`ingest.py` 의 채널별 빌더를 함께 고친다.
