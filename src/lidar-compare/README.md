# lidar-compare — 같은 MQTT 입력, 세 적재 경로 비교

같은 LiDAR 350대 데이터를 세 경로로 각자의 hot DB(TimescaleDB)에 넣고 Grafana 한 화면에서 비교한다.

```
                          ┌─ A · isl-kafka ─────────────────────────────────────────────────────────────────────────┐
lidar-sim ─▶ EMQX :1884 ──┤  Mqtt.Agent ─gRPC─▶ Engine ─NATS─┬▶ EES Kafka Provider ×3 ─▶ Kafka ─▶ lidar-ingest ─▶ tsdb-lidar-pg :59432
 (350대 · 426 msg/s)      │                                   │                                   (SOURCE=kafka)
                          │                                   └─ C · isl-db ─▶ Db.Provider ─▶ isl.apply_tag_batch() ─▶ cmp-provider-pg :63433
                          │                                                    (호스트 프로세스)   (DB 함수가 표로 펼친다)
                          └─ B · mqtt-direct ─▶ lidar-direct-ingest ─▶ cmp-direct-pg :63432
                                                 (SOURCE=mqtt · ISL 없음)
```

A 와 C 는 Mqtt.Agent · Engine · NATS 주제(`intersyslink.workflow.ot.lidar.all.tags`)까지 같다. Engine 은 이 주제를
JetStream 이 아닌 일반 Publish 로 보내므로 구독자마다 사본을 받는다 — C 를 붙여도 A 가 받는 양은 그대로다.
둘은 Provider 부터 갈린다.

A · B 소비자는 **같은 이미지 · 같은 `ingest.py`** 다(`src/timescaledb/dev/ingest`). 소스(Kafka / MQTT)만 다르고
파싱 뒤의 행 모양 · DB 쓰기 · 지표 이름이 같다. C 는 ISL 에 새로 만든 **Db.Provider**(`intersyslink-v4/src/Edge/Providers/Db`)가
태그 변경을 1초씩 묶어 DB 함수 `isl.apply_tag_batch(jsonb)`(`infra/db/10-isl-db-provider.sql`) 한 번으로 넘긴다.
Provider 는 lidar 를 모른다 — 태그를 어느 표 어느 행으로 펼칠지는 그 함수가 `ingest.py` 와 같은 규칙으로 정한다.
그래서 세 hot DB 는 같은 이미지 · 같은 스키마(`01-schema.sql` · `02-policies.sql`) · 같은 행이고, 지표 이름도 같다.
세 경로 모두 1초마다 한 트랜잭션으로 쓴다.

## 무엇이 다른가 — 필드 카탈로그

| 필드 | 원천 | A (Kafka 레코드) | B (MQTT 메시지) | C (Engine NATS 태그 변경) |
|---|---|---|---|---|
| 장비 id | 발행기 `id` | **없음.** `content.tid` 는 EES ParameterId(숫자). `lidar_tag_catalog` 로 되돌린다 | `id` 그대로 | `tagId` 첫 마디 (산출물은 `-SEGMENTED_PCD` 등 접미사를 뗀다) |
| TagId | Agent `MqttTagId` | 없음 (Provider 가 버린다) | 서비스가 같은 규칙(`{id}.{토픽 '/'→'_'}.raw_payload`)으로 만든다 | `tagId` 그대로 (Engine 이 Agent 의 TagId 문자열을 싣는다) |
| 채널 | — | Kafka 토픽 이름(`ot.lidar.*`) 마지막 마디 | MQTT 토픽 마지막 마디 | `tagId` 가운데 마디(토픽)의 마지막 `_` 조각 |
| 값 | 발행기 `raw_payload` | `content.value` (JSON 문자열) | `raw_payload` 객체 | `value` (JSON 문자열, `valueKind=String`) |
| 이벤트 시각 | `raw_payload.occurred_at` | `content.timestamp` (Agent 가 occurred_at 으로 정한 ns epoch) | `raw_payload.occurred_at` | `changedAt` (Agent 가 occurred_at 으로 정한 ISO 시각) |
| 묶음 단위 | — | Provider 가 채널별로 **태그당 최신값** 스냅샷 | 메시지 하나 = 항목 하나. 서비스가 1초씩 묶는다 | 변경 하나 = 항목 하나. **합치지 않는다.** Provider 가 1초씩 묶는다 |
| 전달 보장 | — | Kafka 오프셋을 DB 커밋 뒤 커밋 | QoS 1 · `clean_session=False` · **PUBACK 을 DB 커밋 뒤** 보낸다 | NATS 일반 구독이라 재전달이 없다. 받은 뒤 DB 실패는 같은 배치로 재시도(메모리 큐). Provider 가 죽어 있던 동안의 변경은 잃는다 |
| param_id · pm_mode | — | EES 계약 값 | NULL | NULL |

Engine 이 NATS 로 보내는 모양 (2026-09-17 실측, 1초에 메시지 1개 · 항목 ~411개 · ~357KB):

```jsonc
// intersyslink.workflow.ot.lidar.all.tags
{"edgeGroupId": "edge.mqtt.p3.ot",
 "tags": [{"tagId": "LDR-GJ-A1B4-29.ot_device_assembly_lidar_status.raw_payload",
           "valueKind": "String",
           "value": "{\"device_role\":\"LIDAR\",\"status\":\"ONLINE\",\"occurred_at\":\"2026-09-17T04:54:40.4233981+00:00\",...}",
           "changedAt": "2026-09-17T04:54:40.4233981+00:00"}, ...]}
```

**Provider 는 태그(=장비·채널)마다 최신값을 들고 있다가 보낸다.** 같은 태그에 짧은 간격으로 여러 건이
오면 마지막 것만 나간다. 산출물 채널은 스캔 한 번에 `SEGMENTED_PCD` 10건이 같은 태그로 연달아 와 A 에서 합쳐진다.
B 는 브로커가 준 메시지를 전부 적재한다. `verify.ps1` 의 도달률이 그 차이다.

B 는 수동 ack 라 EMQX 의 세션당 in-flight 창(`mqtt.max_inflight`, 기본 32)이 1초치 메시지보다 커야 한다.
compose 에서 `EMQX_MQTT__MAX_INFLIGHT=10000` 으로 올려 두었다. Mqtt.Agent 는 받는 즉시 ack 해 영향이 없다.

## 띄우기

전제: podman(또는 docker) · .NET 10 SDK · `D:\git\intersyslink-v4` (A · C 경로용). B 경로만 볼 거면 4) 만 하면 된다.

```powershell
# 1) ISL 빌드 (Db.Provider 는 AppHost 에 안 묶여 있어 따로 빌드한다)
dotnet build D:\git\intersyslink-v4\src\Aspire\InterSysLink.AppHost\InterSysLink.AppHost.csproj
dotnet build D:\git\intersyslink-v4\src\Edge\Providers\Db\Db.Provider\Db.Provider.csproj

# 2) Aspire — NATS · Kafka 컨테이너와 대시보드(http://localhost:15147)를 띄운다. 숨은 창으로 둔다.
Start-Process dotnet -ArgumentList 'run','--no-build','--project','D:\git\intersyslink-v4\src\Aspire\InterSysLink.AppHost\InterSysLink.AppHost.csproj','--launch-profile','http' `
  -WorkingDirectory D:\git\intersyslink-v4\src\Aspire\InterSysLink.AppHost -WindowStyle Hidden `
  -RedirectStandardOutput logs\aspire.out.log -RedirectStandardError logs\aspire.err.log

# 3) 데이터 경로 ISL 서비스를 NATS 가 응답하는 것을 확인한 뒤 직접 띄운다 (아래 "왜" 참고)
scripts\isl-services.ps1 -Restart

# 4) A 스택(src/timescaledb) + 이 스택 + Kafka 컨테이너 네트워크 연결 + ISL 프로세스 지표 수집기
scripts\up.ps1

# 5) 확인
scripts\verify.ps1            # 세 DB 의 채널별 행 수 · 기대 대비 도달률 · 장비 수 · 지연
scripts\watch.ps1             # 세 DB 에 쌓이는 속도(적재 시각 기준)를 주기적으로 찍는다
```

| 주소 | |
|---|---|
| http://localhost:63380 | **비교 Grafana** (admin/admin) — 홈이 비교 대시보드다 |
| http://localhost:63090 | 비교 Prometheus |
| http://localhost:59380 | A 스택 Grafana (적재 · DB 상태 대시보드 기존 그대로) |
| http://localhost:18083 | EMQX 대시보드 (admin/public) |
| http://localhost:15147 | Aspire 대시보드 (로그인 URL 은 `logs\aspire.out.log`) |
| `localhost:59432` / `localhost:63432` / `localhost:63433` | A / B / C hot DB (`lidar`, postgres/postgres) |
| http://localhost:59080/metrics · http://localhost:63080/metrics · `isl-metrics\isl-db-provider.prom` | A / B 소비자 지표 · C Provider 지표(textfile) |

정지: `scripts\down.ps1` (이 스택만) · `-All` (A 스택도) · `-Volumes` (볼륨까지).
ISL 서비스는 `scripts\isl-services.ps1 -Stop`, Aspire 는 `logs\aspire.pid` 의 프로세스 트리를 내린다.

### 왜 ISL 서비스를 Aspire 밖에서 다시 띄우나

Aspire 가 NATS 컨테이너와 Engine 을 거의 동시에 띄우는데, Engine 은 기동 중 NATS 연결이 한 번 끊기면
(`NATSConnectionException: Connect read error` · 10053) 재시도 없이 종료한다(`JetStreamProvisioner`).
Mqtt.Agent 는 Engine 이 없으면 부트스트랩 재시도 8회를 소진하고 멈춘다. 2026-09-17 두 번 연속 재현됐다.
`isl-services.ps1` 은 4222·4223 이 INFO 를 돌려주는 것을 확인한 뒤 Engine → Provider(Kafka ×3 · Db) → Agent 순으로 띄운다.
환경변수는 AppHost(`EngineResourceBuilderExtensions` · `EdgeResourceBuilderExtensions`)와 같다. 다른 점 셋:

- `Nats__Url` 호스트를 `localhost` 가 아니라 `127.0.0.1` 로 준다. podman 은 127.0.0.1 에만 바인드하는데
  NATS.Client(v1) 가 `::1` 부터 시도하다 2초 연결 제한을 넘긴다.
- NATS 비밀번호는 노드(primary/secondary)마다 다르다. 컨테이너 인자 `--pass` 에서 각각 읽는다.
- Edge 서비스에는 `DOTNET_ENVIRONMENT=Development` 를 주지 않는다. Mqtt.Agent 가 Development 의 DI 스코프
  검증(`MqttConfigurationResponderWorker` → scoped 핸들러)에 걸려 기동하지 못한다.

### 네트워크

Aspire 네트워크에 우리 컨테이너를 넣으면 Aspire 가 떼어 낸다. 그래서 반대로 Kafka 컨테이너를 `tsdb-net` 에
alias `kafka` 로 붙인다(A 스택 `up`). **Aspire 를 다시 띄우면 컨테이너 이름이 바뀌므로 `scripts\up.ps1` 을 다시 돌린다.**
B 경로는 같은 compose 안의 EMQX 만 쓰므로 Aspire 와 무관하다.

## C 경로 — Db.Provider

| 항목 | 값 |
|---|---|
| 프로젝트 | `intersyslink-v4/src/Edge/Providers/Db/Db.Provider` (Edge.Runtime · Protos.Client · NATS.Client · Npgsql) |
| EdgeGroupId · Kind | `edge.db.p3.lidar` · `DbProvider` — Engine 이 처음 보는 Kind 라 `EdgeKind.Unknown` 으로 자동 등록한다. **Engine 코드는 고치지 않았다** |
| 구독 | Engine 바인딩(`WORKFLOW_EDGE_BINDING`)이 있으면 그것, 없으면 설정 `DbProvider:WorkflowIds`(`ot.lidar.all`). 지금은 바인딩 없이 설정값으로 돈다 |
| 역할 | Engine 하트비트로 받은 역할이 Active/Single 일 때만 쓴다 (Kafka Provider 와 같다) |
| 배치 | `BatchMaxItems` 5000 · `BatchMaxWaitMs` 1000 — MQTT 직결 소비자와 같은 규칙. 큐 상한 `QueueCapacity` 100만, 넘치면 `isl_db_provider_queue_dropped_total` |
| DB 호출 | `SELECT kind, name, n FROM isl.apply_tag_batch(@items::jsonb)` — 배치 하나 = 문장 하나 = 트랜잭션 하나. 돌려받은 (kind, name, n) 이 채널별 항목 · 표별 삽입 · 격리 지표가 된다 |
| 지표 | `lidar_ingest_*{pipeline="isl-db"}` 를 `isl-metrics/isl-db-provider.prom` 에 2초마다 쓴다. 이름 · 버킷은 `ingest.py` 와 같다 |

`10-isl-db-provider.sql` 은 `ingest.py` 의 행 규칙(`status_row` · `actual_row` · `artifact_row` · `state_rows_of` ·
`progress_rows` · `artifact_rows` · 격리 사유)을 SQL 로 옮긴 것이다. **한쪽을 고치면 다른 쪽도 고친다.**
다른 점 둘: 블록 키(`hull_no` · `block_id`)가 비어 있는 항목은 상태 표 UPSERT 에서 빼고(파이썬은 배치째 실패한다),
배치 원장(`rdb.lidar_status_message`)의 `kafka_topic` 은 `isl-db` 다.

A 와 C 의 차이는 Provider 의 성질이다. EES Kafka Provider 는 태그마다 최신값만 들고 있다가 보내므로 한 스캔의
`SEGMENTED_PCD` 10건이 한 건으로 합쳐진다. Db.Provider 는 받은 변경을 전부 넘긴다.

## ISL 쪽 변경 (intersyslink-v4 · 브랜치 mqtt-agent · 미커밋)

| 파일 | 내용 |
|---|---|
| `Edge/Providers/Db/Db.Provider/*` · `InterSysLink.sln` | **신규.** C 경로 Provider (위 절) |
| `Aspire/InterSysLink.AppHost/Program.cs` | Provider `a1`(actual) · `a2`(artifact) 추가 — 워크플로우 `ot.lidar.all` 이 채널마다 Provider 를 따로 두는데 AppHost 에는 status 용 하나만 있었다 |

로컬 Engine DB(`bin/Debug/net10.0/isl-*.db`)는 마이그레이션 이력만 고쳤다. DB 가 `20260910053718_Migration_1.0.0` 으로
만들어졌는데 브랜치 코드는 `20260911000559_Migration_4.0.0.0` 한 벌로 합쳐져 있어 Engine 이 기동 중 `CREATE TABLE APP_LOG`
에서 죽었다. `dotnet ef migrations script` 로 뽑은 스키마와 대조해 같음을 확인하고 이력 행만 넣었다.
원본은 같은 폴더 `db-backup-20260917/` 에 있다.

## 대시보드

`infra/monitoring/grafana/dashboards/lidar-compare.json` 은 `scripts/build-dashboard.py` 가 만든다. 경로 목록을
한 번만 적고 경로마다 같은 쿼리를 찍어 내므로, 패널을 고칠 때는 JSON 이 아니라 스크립트를 고친다.

경로 색은 A 주황 · B 파랑 · C 초록이다.

| 행 | 보는 것 |
|---|---|
| 한눈에 | 적재 행/초 · e2e p95 · 소스 대기 · 최근 1분 장비 수 · 미해석 장비 축 · 격리 |
| 처리량 | 행/초 · 채널별 항목/초 · **hot DB 직접** 분당 행(status / actual · artifact) · 표별 행 수 |
| 지연 | e2e p50/p95/p99 · DB 직접 `received_at − time` · 소스 대기 · 배치 시간 · 배치당 행 |
| 정합성 · 품질 | 중복 · 격리 · A 장비 미해석 · **장비별 A−B · C−B 건수 차이** |
| 리소스 | ISL 호스트 프로세스(Agent · Engine 은 A·C 공통, EES Kafka Provider 는 A, Db.Provider 는 C) CPU/메모리 · 컨테이너 CPU/메모리 · 경로 전용 CPU 합 · DB 크기 |

ISL 은 호스트 .NET 프로세스라 podman-exporter 에 안 잡힌다. `scripts/isl-process-metrics.ps1` 이 5초마다
`Get-Process` 를 `isl-metrics/isl.prom` 에 쓰고 node-exporter(textfile)가 읽는다. `up` 이 숨은 창으로 띄운다.
