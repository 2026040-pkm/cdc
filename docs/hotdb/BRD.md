# HotDB 구성 BRD

v0.3 · 2026-10-06 · 상태: 파란 영역(HotDB Provider 제외) 구현 · 로컬 대역 검증 완료 (`src/hotdb`) · §10 남은 결정 6건

### 결정 기록

| 날짜 | 결정 | 근거 · 결과 |
|---|---|---|
| 2026-10-06 | 구현은 **Java**, CDC 는 **Debezium Embedded — Kafka 없음** | 사용자 결정. 서비스 안에서 슬롯을 직접 읽는다 |
| 2026-10-06 | 스키마가 바뀌어도 **코드를 고치지 않는** 구조 | 사용자 요구. 원천 행을 맵으로 다루고, 원천 → 대상 규칙은 설정(`hotdb.routes`), 스키마는 Flyway V 파일. 기동 시 설정과 실제 표를 대조해 어긋나면 거부 |
| 2026-10-06 | Q1 → **A안 확정** (청크 스키마째 publication + 역매핑) | 스파이크: 새 청크 자동 편입 · publication 이 있어도 새 하이퍼테이블 · 연속집계 생성 가능. 압축 뭉치는 `_hyper_N_M_chunk_compressed` 로 실려 정규식으로 버린다 |
| 2026-10-06 | 2.29 의 자동 압축 정책은 지우고 **7일 뒤 압축**으로 다시 건다 | `tsdb.segmentby` 를 주면 기본 정책이 자동으로 붙는다 — 그대로 두면 CDC 가 읽기 전에 압축될 수 있다 |
| 2026-10-06 | Q3 → 레거시 사본은 **시험용으로 지어낸 표**로 둔다 (실제 표 이름 · 값을 쓰지 않음, 보안) | `erp`(SAP 사본) · `mes` · `lgs`(Oracle 사본) — 178표(erp 114 · mes 60 · lgs 3 · geo 1), `scripts/gen-sample-legacy.py` → `db/legacy-db/V2`. 이름 · 값은 지어내고 규모(표 · PK · 컬럼 수)만 실제와 맞춘다. 덤프 기반 DDL 생성은 폐기 |
| 2026-10-06 | **2단 CDC** — 권역 RDB 를 다시 CDC 로 받는 RFC Provider 를 붙인다 (`app/rfc-provider`) | 사용자 결정: Provider → tsdb → (CDC) 권역 서비스 → svc_* → (CDC) 다른 서비스. cdc-core 엔진을 그대로 쓰고 반영 대상만 `CdcSink` 로 바꾼다. SAP 송신은 dry-run |
| 2026-10-06 | `svc_cdc_pub` 를 `svc_*` 스키마 전체 → **`actual_result` 만**으로 좁힘 (V6) | 스키마째면 `device_status_current` 의 초당 갱신(≈ 350/s)이 전부 RFC Provider 로 가 버려진다. 대신 새 표는 자동으로 안 실린다 |
| 2026-10-06 | 개발 범위 = 그림의 **파란 영역**(Hot DB · RFC Service · DB Agent · 실적 판별 모듈), **HotDB Provider 는 제외** | 사용자 결정 (§00.1 그림) |
| 2026-10-06 | 실적 판별 모듈은 **N 개** — 가공 · 조립 · 의장 · 도장, 더 늘 수 있다 → `ops.provision_module()` 한 줄로 스키마 · 계정 · 표 · 관계 · publication 생성 (V7) | 사용자: "모듈은 다수가 될 수 있다". 모듈 수에 따라 바뀌던 곳(V3 의 고정 4개, exporter 의 UNION 4줄, compose 프로파일)을 등록부(`ops.module`) 하나로 모았다 |
| 2026-10-06 | 모듈 RDB 안의 **연관 데이터는 FK 로 잇는다** — 장비 마스터(`tsdb.device`) ◀ 최신 상태 ◀ 전이, 장비 ◀ 스캔 ◀ 실적 · 산출물 | 사용자: "연관이 있는 데이터는 연관성 있게". 모듈 안 FK 는 `DEFERRABLE INITIALLY DEFERRED`, 채널 순서가 뒤바뀐 경우(산출물 먼저)는 `scan-stub` 라우트가 부모 자리를 먼저 만든다 |
| 2026-10-06 | **RFC Service = SAP 폴링(→ erp) + 2단 CDC(→ SAP)** 한 서비스. 모듈 · 슬롯 · 계정 이름은 `rfc-provider` 그대로 | 그림의 O-8. 폴링은 `poll-core`(계정 `rfc_agent`), CDC 는 cdc-core(계정 `rfc_provider`) — 한 프로세스에 계정 둘 |
| 2026-10-06 | SAP 쓰기는 **갈아 끼우는 방식**(`RfcSender`) — 우선 **JDBC 로 Z 테이블 INSERT**, JCo(RFC 함수)는 커넥터를 받으면 구현 하나 추가 | 사용자 결정. JCo 는 S-user 로만 받을 수 있다 |
| 2026-10-06 | **DB Agent** = `poll-core` + Oracle JDBC. 워터마크(`upd_date ∥ upd_time`) 증분 · 설정의 작업 목록만 늘리면 표가 는다 | 그림의 O-6 |
| 2026-10-06 | 그림 갱신 — 네 개 모두 **실적 판별 모듈**(dockerized java module)로 통일, 모듈 묶음에 **"순차적으로 적재를 위한 큐 필요"** 메모 | 큐는 §10 Q10 — 의도 확인 전까지 구현하지 않음 |
| 2026-10-06 | 로컬 검증은 **SAP 대역 DB**(PostgreSQL) · **Oracle 대역**(oracle-free 23) — 실제 SAP 는 띄우지 않음 | 사용자 결정. 운영 HANA 는 접속 설정(`SAP_URL`)만 바꾼다 |
| 2026-10-06 | **레거시 DB 를 Hot DB 와 나눈다** — Hot DB(필드 DB) = tsdb · svc · ops, 레거시 DB(`hotdb-legacy-db`) = erp · mes · lgs · geo · `ops.poll_state` | 사용자 결정. V10 이 Hot DB 에서 폴링 상태 · `rfc_agent` · `db_agent` 를 지운다. `compose.legacy.yml` |

![HotDB 구조 — 파란 영역이 개발 범위](img/hotdb-architecture-2026-10-06.png)

1단계 실측(로컬, 서비스 2개): ×5 부하(≈ 2,000 행/초)에서 반영 지연 p99 0.3초, PG 0.26 코어, 서비스당 0.22 코어 ·
45초 정지 후 8초 만에 따라잡음 · 누락 0. 자세한 값과 실행법은 `src/hotdb/README.md`.

| 출처 | 쓰는 내용 |
|---|---|
| `docs/todo.md` | 요구사항 원문 4줄 |
| `시스템 아키텍쳐.drawio` (시스템 아키텍쳐 · 물리 배포 다이어그램) | 컴포넌트 · 서버 배치 |
| (폐기) 레거시 덤프 | 처음에는 레거시 DDL 을 덤프에서 뽑았으나, 보안상 시험용으로 지어낸 표(`db/legacy-db/V2`)로 바꿨다 |
| `docs/backup/latest/mqtt-agent-tag-catalog.html` | 필드 데이터 페이로드 · 토픽 · 태그 수 |
| `src/backup/hotdb-tsdb` · `docs/backup/hotdb/unified-pg-setup.html` | 직전 통합 PG 설계 (이 문서의 출발점) |
| `docs/backup/cdc/timescaledb-cdc-impact.html` | 하이퍼테이블 CDC 실측 (2026-09-17) |

---

## 00. 한 줄 요약

PostgreSQL 한 대(HotDB)에 **레거시 · 필드 데이터(tsdb) · 권역별 서비스 RDB** 를 스키마로 나눠 두고,
실적 판별 모듈들이 **tsdb 를 CDC 로 감지해 자기 RDB 스키마에 저장**한다.
1단계는 HotDB 자체(스키마 · 계정 · publication · 시드 · 테스트 · 모니터링)를 만들고,
CDC 소비자(권역별 서비스)는 그 위에서 2단계로 붙인다.

### 직전 설계에서 바뀌는 것 — 가장 큰 결정

직전 설계(`src/backup/hotdb-tsdb/init/20-cdc.sql`)는 **"tsdb 는 CDC 대상이 아니다"** 로 못 박았다.
이번 요구(`todo.md` 1행, drawio HotDB Provider 메모 "cdc로 해서 그것을 통해 감지해서 rdb에 저장")는
그 결정을 뒤집는다. 하이퍼테이블 CDC 는 2026-09-17 실측에서 **치명 3건**이 나왔으므로(§05.3)
이 BRD 는 그 대응을 설계 범위 안에 넣는다.

---

### 00.1 목표 구조 (2026-10-06 그림)

| 그림 | 이 저장소 | 상태 |
|---|---|---|
| Hot DB (O-7) — Legacy RDB sap · Legacy RDB oracle · TSDB field data · RDB field data | Hot DB(PostgreSQL 17 + TimescaleDB) `tsdb` · `svc` + 레거시 DB(PostgreSQL) `erp` · `mes`/`lgs` | 구현 |
| 가공 · 조립 · 의장 · 도장 실적 판별 모듈 (다수, 그림의 박스 하나) | `app/zone-service` 한 이미지 × 모듈 수 (`ZONE=mch·asm·oft·pnt`) — tsdb CDC → `svc_<모듈>` INSERT | 구현 (도장 · 가공은 필드 정의서 전이라 대역 장비로만 확인) |
| RFC Service (O-8) — Embed debezium · polling · RFC service | `app/rfc-provider` — ① SAP 폴링 → `erp` ② `svc.actual_result` CDC → SAP Z 표 | 구현 (SAP 대역으로 확인, JCo 는 미정) |
| DB Agent (O-6) | `app/db-agent` — Oracle 폴링 → `mes` · `lgs` | 구현 (Oracle 대역으로 확인) |
| HotDB Provider (파란 영역 밖) | — `field-simulator` 가 tsdb 에 직접 써서 대신한다 | 범위 밖 |

## 01. 요구사항

`docs/todo.md` 원문을 번호로 받는다.

| ID | 요구 | 단계 |
|---|---|---|
| R1 | 권역별 서비스는 HotDB tsdb 의 필드 데이터를 CDC 로 감지해 HotDB 의 RDB 에 저장한다 | 2 |
| R1-a | 실시간 CDC 의 부하(CPU · 메모리 · WAL · 슬롯 지연 등)를 확인할 수 있어야 한다 | 1 · 2 |
| R1-b | 로컬 한 대가 아니라 **서버 to 서버** 로 테스트한다 | 2 |
| R2 | HotDB Provider 에 필요한 스키마 — tsdb 스키마 · RDB 스키마를 구성한다 | **1** |
| R3 | RFC Service: SAP 폴링(SAP → 레거시 DB erp), CDC 수신(HotDB RDB → SAP)까지 구현한다 | 3 — **구현** |
| R4 | DB Agent 가 Oracle 을 일정 주기로 폴링해 HotDB 에 저장한다 | 3 — **구현** |
| R5 | 위 전부에 테스트 코드와 모니터링을 붙인다 (이번 요청 추가분) | 전 단계 |
| R6 | 실적 판별 모듈은 여러 개이고 늘 수 있다. 모듈 RDB 의 연관 데이터는 연관되게(관계) 둔다 (2026-10-06) | 2 — **구현** |

### 범위 밖

- HotDB Provider (MQTT Agent → tsdb) — 그림의 파란 영역 밖. `field-simulator` 가 tsdb 에 직접 써서 대신한다
- 장비 → MQTT 브로커 → MQTT Agent 구간 (태그 카탈로그 문서 소관)
- 실적 판별 **로직**(블록 진척률 → 실적 확정 규칙). 1단계 RDB 는 판별 결과를 담을 그릇까지만 만든다
- 레거시 **데이터** 이관. 레거시는 스키마(DDL)만 쓴다

---

## 02. 구성 요소와 배치

drawio 에서 HotDB 와 직접 닿는 것만 뽑는다.

| 컴포넌트 | 서버 (물리 배포) | HotDB 에 대해 | 대상 스키마 |
|---|---|---|---|
| HotDB (PostgreSQL) | Hot Data DB 서버 · RAM 32GB | — | — |
| HotDB Provider | (drawio 에 배치 미표기) | **쓰기** | `tsdb` 만 |
| 실적 판별 모듈 N 개 (가공 · 조립 · 의장 · 도장 …) | OT Server A (조립·도장), B (의장·가공) | tsdb **CDC 읽기**, 자기 RDB **쓰기** | `svc_<모듈>` |
| RFC Service (O-8) — 폴링 + Embed Debezium | RFC AGENT 박스 | ① SAP 폴링 → **쓰기** (`rfc_agent`) ② RDB **CDC 읽기** → SAP (`rfc_provider`) | ① `erp` ② `svc.actual_result` |
| DB Agent (O-6) | DB AGENT 박스 | Oracle 폴링 → **쓰기** (`db_agent`) | `mes` · `lgs` |
| OT API 서비스 · OT 대시보드 | — | 읽기 | 전부 |

원칙(직전 설계 유지): **스키마마다 쓰는 주체는 하나**다. 계정도 그 주체 단위로 나눈다.
HotDB Provider 는 RDB 에 쓰지 않는다 — RDB 행은 권역별 서비스만 쓴다(2026-09-30 결정).

```
 장비 ─MQTT─▶ MQTT Agent ─(태그)─▶ HotDB Provider ──INSERT──▶ [tsdb] ─┐
                                                                     │ WAL (logical)
                          ┌──────────────────────────────────────────┘
                          ▼
       가공/조립/의장/도장 … 실적 판별 모듈 (Embedded Debezium, 모듈마다 슬롯 1)
                          │ UPSERT (모듈 안 표끼리 FK)
                          ▼
                     [svc_mch · svc_asm · svc_oft · svc_pnt …] ──WAL──▶ RFC Service ─▶ SAP Z 표 (RFC_SENDER=jdbc)
 SAP HANA ─JDBC 폴링─▶ RFC Service ─▶ 레거시 DB [erp]
 Oracle ─JDBC 폴링─▶ DB Agent ─▶ 레거시 DB [mes · lgs · geo]
```

---

## 03. 필드 카탈로그 (tsdb 입력)

스키마보다 먼저 둔다. **누가 · 어떤 값을 · 얼마나 자주** 보내는지가 스키마 · 청크 간격 · CDC 부하를 정한다.
출처는 태그 카탈로그 v1(2026-09-22) 실제 발행 기준이고, 페이로드는 ISL 벤더 합의 전 제안 규격이다.
**정의서 확정본을 받으면 이 절을 통째로 교체한다.**

### 03.1 송신 주체 · 주기 · 규모

| 채널 | 송신 주체 | MQTT 토픽 | 주기 | 태그 수 (조립 · 의장) | 행/초 | 행/일 |
|---|---|---|---|---|---|---|
| status | LiDAR (Edge PC) | `ot/device/{zone}/status` | 1초 | 210 · 140 = 350 | 350 | 3,024만 |
| actual | AI Inference Service | `ot/sensor/{zone}/actual` | 1분 | 210 · 140 = 350 | 5.8 | 50만 |
| artifact | AI Inference Service | `ot/pipeline/{zone}/artifact` | 1분 | 630 · 420 = 1,050 (장비×3종) | 70 | 605만 |
| 합계 | | 토픽 6 | | 1,750 | **≈ 426** | ≈ 3,680만 |

- 산출물은 스캔 하나에 12행 (REGISTERED_PCD 1 · TRANSFORMATION_MATRIX 1 · SEGMENTED_PCD 10)
- **도장(PLC · modbus: 가스히터 · 제습기) · 가공 권역은 카탈로그가 없다.** 1단계에서 스키마 자리만 두고 필드는 비운다 (§10 Q5)

### 03.2 공통 봉투 · 키

| 필드 | 타입 | 값 · 범위 | tsdb 컬럼 | 비고 |
|---|---|---|---|---|
| `id` | string | `LDR-GJ-A1B3-07`, 산출물은 `-{artifact_type}` 접미사 | `tag_catalog.source_id` | MQTT Agent 가 `{id}.{토픽}.raw_payload` 태그로 접는다 → 태그 등록부(`tag_catalog`)로 장비를 푼다 |
| `site` · `zone` · `shop` · `bay` | string | `geoje` · `ASSEMBLY`/`OUTFITTING` · … | `tsdb.device` | 1초마다 안 바뀌므로 이력 행에 싣지 않는다 |
| `occurred_at` | ISO8601 (.NET 7자리) | | `event_time` + `source_time_text` | PG 는 마이크로초까지. 원문 보관 |
| `ingested_at` | ISO8601 | | `source_ingested_at` | |
| (Provider) | | | `received_at` | Provider 수신 시각 — CDC 지연 측정의 기준점 |

### 03.3 status

| 필드 | 타입 | 단위 · 범위 | 집계 |
|---|---|---|---|
| `device_role` | enum | `LIDAR` | — |
| `status` | enum | `ONLINE` · `CALIBRATING` · `ERROR` · `OFFLINE` | 마지막 값 · 상태 전이 |
| `error_code` | string? | | 마지막 값 |
| `last_heartbeat_at` | timestamp | | 최대 |
| `scan_rate_pts_per_sec` | int | pts/s, ≥ 0 (예 229,866) | avg · min |
| `temperature_c` | real | ℃ (예 40.9) | avg · max |
| `connectivity_rssi` | smallint | dBm (예 -52) | avg · min |
| `fov_mode` | enum | `wide` … | 마지막 값 |

### 03.4 actual

| 필드 | 타입 | 단위 · 범위 | 비고 |
|---|---|---|---|
| `scan_id` | uuid | | artifact 와 조인 키 |
| `event_type` | enum | `START` · `PROGRESS` · `COMPLETE` | 멱등 키 = `{scan_id}:{event_type}` |
| `hull_no` · `block_id` | string | `H1207` · `B107P` | RDB 실적 키 |
| `scanned_at` | timestamp | | |
| `block_progress_rate` | real | 0 ~ 100 % | 마지막 값 (avg 무의미) |
| `match_confidence` | real | 0 ~ 1 | |
| `reference_cad_id` · `model_version` | string | | |
| `record_type` · `input_method` · `source_system` | enum | `ACTUAL` · `AUTO` · `AI Inference Service` | 고정값 — 저장 안 함 |
| ~~`stage`~~ | | | 2026-09-22 재설계로 없음. 공정 단계는 레거시와 대조해 서비스가 판정 |

### 03.5 artifact

| 필드 | 타입 | 범위 | 제약 |
|---|---|---|---|
| `scan_id` · `hull_no` · `block_id` | | | |
| `artifact_type` | enum | `REGISTERED_PCD` · `TRANSFORMATION_MATRIX` · `SEGMENTED_PCD` | |
| `segment_id` | string? | | SEGMENTED_PCD 만 값 있음 → `segment_key` (`''` 정규화) |
| `storage_uri` | string? | `file://ot-a/pcd/...` | MATRIX 만 null |
| `file_size_bytes` | bigint? | ≥ 0 | |
| `checksum` | string? | `sha256:…` | |
| `transformation_matrix` | real[16]? | | MATRIX 만 값 있음 |
| `produced_by_device_id` · `model_version` | string | `INF-GJ-A1-01` | |

### 03.6 카탈로그가 바뀌면 다시 계산할 것

태그 수 → 청크 간격 · 행/초 → CDC 피크 처리량과 슬롯 지연 · 상하한 → Provider 격리율 · 값 타입 분포 → 압축률 ·
권역 추가(도장 · 가공) → 슬롯 수와 WAL 디코딩 배수(§05.4).

---

## 04. 스키마 구성

한 인스턴스, 스키마로 나눈다 (직전 설계 결정 유지 — 인스턴스 분리는 §10 Q6 에서 부하 실측 후 재검토).

| 스키마 | 내용 | 쓰는 주체 (계정) | 읽는 주체 | CDC |
|---|---|---|---|---|
| `erp` (레거시 DB) | SAP 사본 — 시험용 114표 (item · part · order_line) | RFC Service (`rfc_agent`) | 서비스 · API | 아님 |
| `mes` (레거시 DB) | Oracle 사본 생산 쪽 — 시험용 60표 (폴링 2표: work_log · alarm) | DB Agent (`db_agent`) | 서비스 · API | 아님 |
| `lgs` (레거시 DB) | Oracle 사본 물류 쪽 — 시험용 3표 (폴링 2표: shipment · tracking) | DB Agent | 서비스 · API | 아님 |
| `geo` (레거시 DB) | Oracle 사본 구역 지도 — 시험용 1표 | DB Agent | 서비스 · API | 아님 |
| `tsdb` | 필드 데이터 — device · tag_catalog · 채널별 이력 하이퍼테이블 3 | HotDB Provider (`hotdb_provider`) | 서비스 · API | **원천** (R1) |
| `svc_<모듈>` (지금 `svc_mch` · `svc_asm` · `svc_oft` · `svc_pnt`) | 실적 판별 모듈 RDB — "RDB field data" | 그 모듈 계정 (`svc_<모듈>`) | API · RFC Service | **원천** (R3, 실적만) |
| `ops` | 모듈 등록부 · heartbeat · dead letter · SAP 송신 기록 · 폴링 상태 · flyway 이력 | CDC 소비자 · 에이전트 | 모니터링 | heartbeat 만 |

- 레거시 표는 폴링 경로를 시험하려고 지어낸 것이다 — 이름 · 컬럼 · 값 모두 실제 레거시 시스템과 무관하고, 규모(표 178 · PK 있는 표 · 컬럼 수)만 실제와 비슷하게 맞췄다(`scripts/gen-sample-legacy.py`, §10 Q3). 실제 원천에 붙일 때는 작업 목록(`hotdb.poll.jobs`)과 레거시 DB 의 표 정의만 바꾼다
- 레거시 표는 Hot DB 가 아니라 별도 레거시 DB(`hotdb-legacy-db`, 59435)에 있다 (V10 분리)

### 04.1 tsdb

직전 DDL(`src/backup/hotdb-tsdb/init/10-tsdb.sql`)을 그대로 가져온다. 바뀌는 점만 적는다.

| 표 | 형태 | PK | 청크 | CDC 를 위해 바뀌는 것 |
|---|---|---|---|---|
| `device` | 일반 | (site, device_id) | — | publication 포함 (서비스가 zone 을 알기 위해) |
| `tag_catalog` | 일반 | tag_key | — | publication 포함 |
| `status_history` | 하이퍼 | (tag_key, event_time) | 6h | **압축 지연** (§05.3 B2) |
| `actual_history` | 하이퍼 | (tag_key, event_time, scan_id, event_type) | 1d | 〃 |
| `artifact_history` | 하이퍼 | (tag_key, event_time, scan_id, artifact_type, segment_key) | 1d | 〃 |

- 이력은 append-only — REPLICA IDENTITY 는 DEFAULT (FULL 불필요, WAL 배증 방지)
- 같은 키 재수신: PK 충돌 → Provider 가 내용 비교, 같으면 버리고 다르면 격리 (직전 설계 유지)

### 04.2 svc_<모듈> — 실적 판별 모듈 RDB (관계형)

모듈마다 같은 표 모양이고 `ops.provision_module('<모듈>', '<권역 코드>', '<이름>')` 이 만든다 (V7).
연관 데이터는 FK 로 잇는다 — 모듈 안 FK 는 커밋 때 검사(`DEFERRABLE INITIALLY DEFERRED`)라 한 배치 안의 순서는 상관없다.

```
tsdb.device (site, device_id) ◀── device_status_current (device_id) ◀── device_status_change
tsdb.device (site, device_id) ◀── scan (scan_id) ◀── actual_result   (hull_no, block_id, scan_id)
                                               ◀── artifact        (scan_id, artifact_type, segment_key)
```

| 표 | 키 | 부모 (FK) | 채우는 라우트 | 원천 |
|---|---|---|---|---|
| `device_status_current` | device_id | `tsdb.device` | status-current · upsert (더 새 시각만) | tsdb.status_history |
| `device_status_change` | (device_id, changed_at) | `device_status_current` | status-change · 바뀔 때만 | 〃 |
| `scan` | scan_id | `tsdb.device` | scan · upsert / scan-stub · 산출물이 먼저 오면 자리만 | actual · artifact |
| `actual_result` | (hull_no, block_id, scan_id) | `scan` | actual-result · COMPLETE 만 | tsdb.actual_history |
| `artifact` | (scan_id, artifact_type, segment_key) | `scan` | artifact · insert | tsdb.artifact_history |

- 각 표에 `src_event_time` · `src_received_at` · `applied_at` 을 둔다 → 지연 = `applied_at - src_received_at`
- 채널(MQTT 토픽)이 달라 **산출물이 스캔 START 보다 먼저** 올 수 있다. `scan-stub` 라우트가 `last_event_at = -infinity`,
  `last_event_type = PENDING` 인 부모 자리를 먼저 만들고, 뒤에 온 START 가 `newer-than` 을 통과해 제자리 값으로 덮는다
  (`first_event_at` 은 `| min`)
- 모듈 사이 · 레거시와의 연관(같은 호선 · 블록)은 FK 가 아니라 키 값(`hull_no` · `block_id`)으로 잇는다 — 주인이 다른 스키마끼리 FK 를 걸면 한쪽 배포가 다른 쪽을 막는다

### 04.3 계정 · publication · 슬롯

| 이름 | 종류 | 내용 |
|---|---|---|
| `tsdb_cdc_pub` | publication | `tsdb.device` · `tsdb.tag_catalog` + 하이퍼테이블 청크 (§05.2) |
| `svc_cdc_pub` | publication | `svc_{asm,oft,pnt,mch}.actual_result` + `ops.cdc_heartbeat` (V6 에서 스키마 단위 → 표 단위로 좁힘) |
| `zone_<모듈>` | 슬롯 (모듈 수만큼) | 판별 모듈이 첫 접속 때 생성 (pgoutput). 모듈을 걷어낼 때 `ops.drop_module()` 이 같이 지운다 |
| `rfc_provider` | 슬롯 1 | RFC Service 가 생성. 모듈 전부를 이 슬롯 하나로 받는다 (include 정규식 `svc_[a-z][a-z0-9]*\.actual_result`) |
| `ops.module` | 표 | 모듈 등록부 — 모듈 · 스키마 · 권역 코드. 판별 모듈은 기동 때 자기 등록을 확인한다 |
| `max_slot_wal_keep_size` | 설정 | 10GB — 멈춘 소비자가 디스크를 채우지 않게 (직전 설계 유지) |

---

## 05. CDC 설계 (R1)

### 05.1 방식

권역별 서비스 안에 Debezium Embedded(PostgreSQL connector, pgoutput)를 둔다. drawio 의 "dockerized java service" 와
직전 PoC `embedded-cdc`(Java) 를 기반으로 한다. 서비스마다 슬롯 하나 · 오프셋은 `ops.cdc_offset` 에 저장.

### 05.2 하이퍼테이블을 publication 에 싣는 법 — 후보

| 안 | 방법 | 장점 | 단점 |
|---|---|---|---|
| **A (권장)** | `FOR TABLES IN SCHEMA _timescaledb_internal` + 청크명 → 하이퍼테이블 역매핑 (Debezium TimescaleDb SMT 또는 자체 매퍼) | 새 청크 자동 편입, 원천 구조 그대로 | 압축 청크 · 연속집계 청크 제외 필터 필요, 스파이크 필요 (S1 · S2) |
| B | tsdb 이력을 하이퍼테이블이 아닌 일반(선언적 파티션) 테이블로 + `publish_via_partition_root` | CDC 가 평범해진다 | TimescaleDB 압축 · 보존 정책 포기, 3,680만 행/일을 직접 관리 |
| C | Provider 가 이력과 같은 트랜잭션으로 `tsdb.outbox`(일반 테이블)에 쓰고 그것만 CDC | 하이퍼테이블 문제 전부 회피 | WAL 이 2배, 2026-09-30 결정("Provider 는 이력만") 과 충돌 소지 |

A 를 1단계 스파이크로 확정하고, 실패하면 B → C 순으로 내려간다.

### 05.3 실측된 함정과 대응 (2026-09-17, timescaledb 2.29.2 / PG17)

| # | 함정 | 대응 |
|---|---|---|
| B1 | WAL relation 이 `_timescaledb_internal._hyper_N_M_chunk` 로 온다. 스냅샷은 부모명 | 역매핑 계층 + 캐시(미스 시 `_timescaledb_catalog` 조회) |
| B2 | 압축 시 원본 행이 이벤트 없이 사라지고 `*_compressed` INSERT 만 나간다 | 압축 청크 제외 + **압축 지연**(`compress_after` ≥ 소비자 최대 허용 지연, 기본 7일) |
| B3 | `drop_chunks` 는 DDL → 이벤트 없음 | RDB 는 이력 미러가 아니라 파생값이므로 영향 없음. 대조는 "보존 창 안" 기준 |
| B4 | `FOR ALL TABLES` 면 새 하이퍼테이블 · 연속집계 생성 불가 | `FOR ALL TABLES` 금지. 스키마 단위 publication 이 같은 제약을 받는지 S2 로 확인 |
| B6 | 초기 스냅샷 수십억 행 | `snapshot.mode = no_data`, 필요한 최신값만 서비스 기동 시 SELECT 로 백필 |
| B7 | 직전 실측 2,467 ev/s | 이번 피크 ≈ 426 ev/s(×5.8 여유) — 단, 슬롯 4개가 각자 전체 WAL 을 디코딩 |

### 05.4 권역 필터와 부하 구조

publication 은 권역으로 행을 거를 수 없다(청크 · 스키마 단위 publication 은 행 필터 불가).
**서비스 4개가 각자 tsdb 전체 WAL 을 디코딩하고 자기 권역만 남긴다.** 즉 HotDB 의 walsender CPU · 네트워크는
권역 수에 비례한다. 이게 R1-a 에서 재야 할 핵심 수치이고, 대안(공용 CDC 1개가 권역별 Kafka/NATS 로 팬아웃)은
실측 결과를 보고 판단한다 (§10 Q2).

---

## 06. 레거시 유입 · 유출 (R3 · R4) — 구현

| 구분 | RFC Service ① (SAP → erp) | DB Agent (Oracle → mes · lgs · geo) | RFC Service ② (svc → SAP) |
|---|---|---|---|
| 방식 | JDBC 폴링 (`poll-core`, HANA `ngdbc`) | JDBC 폴링 (`poll-core`, `ojdbc11`) | Embedded Debezium (슬롯 `rfc_provider`) |
| 증분 기준 | 워터마크 식 — 기본 `upd_date ∥ upd_time` (작업마다 지정) | 동일 | WAL LSN |
| 쓰기 | 배치 UPSERT (대상 PK). PK 없는 표는 `mode: replace` | 동일 | `RfcSender` — `jdbc`(SAP Z 표 INSERT) · `dry-run`, JCo 는 추가 예정 |
| 상태 | 레거시 DB `ops.poll_state` (작업별 워터마크 · 마지막 성공 · 오류) | 동일 | 오프셋 파일 + `ops.rfc_sent`(실적 · 판정당 한 번) |
| 계정 | `rfc_agent` (레거시 DB erp 만) | `db_agent` (레거시 DB mes · lgs · geo 만) | `rfc_provider` |
| 로컬 원천 | SAP 대역 `hotdb-sap-sim` (PostgreSQL, `erpsrc.*`) | Oracle 대역 `hotdb-oracle-sim` (FREEPDB1 — 소유자 `MES` · `LGS` · `GEO`, 읽기 계정 `LEGACY_READER`) | 같은 SAP 대역의 `erpsrc.zhotdb_actual_result` |
| 운영 원천 | `SAP_URL=jdbc:sap://<host>:<port>/?currentschema=<스키마>` | `ORACLE_URL` · `ORACLE_SCHEMA` | 같은 HANA 의 Z 표 |

- 워터마크는 모든 행을 쓴 뒤에만 저장한다. 같은 워터마크 값의 행은 매번 다시 읽는다(`>=`) — 같은 초에 바뀐 행을 놓치지 않으려고. UPSERT 라 결과는 같다
- 컬럼은 원천 · 대상 양쪽에 있는 것만 옮긴다. 한쪽 표에 컬럼이 늘어도 코드는 그대로다
- 작업은 RFC Service 3개(erp) + DB Agent 64개(Oracle 사본 mes 60 · lgs 3 · geo 1 전부 — `poll-jobs.yml` 은 `scripts/gen-sample-legacy.py` 생성). PK 없는 24표는 `mode: replace`. Oracle 대역은 소유자 MES · LGS · GEO 아래 같은 64표를 두고 읽기 계정 LEGACY_READER 가 읽는다 (FREEPDB1 에 다른 사용자는 두지 않는다). 실제 원천 표의 워터마크 컬럼 · 주기는 §10 Q8

## 07. 테스트 (R5)

| 층 | 도구 | 무엇을 | 단계 |
|---|---|---|---|
| 스키마 | Testcontainers (`timescale/timescaledb:2.29.2-pg17`) + JUnit 5 | init SQL 적용 · 하이퍼테이블 · CHECK · PK 충돌 · 계정 권한(남의 스키마 쓰기 거부) | 1 |
| CDC 스파이크 | 같은 컨테이너 + 슬롯 직접 읽기 | S1 청크 자동 편입 · S2 스키마 publication 과 새 하이퍼테이블 공존 · S3 압축 시 이벤트 · S4 청크명 역매핑 | 1 |
| 시드 · 발행기 | 태그 카탈로그 기반 생성기 (350대 · 3채널, 배율 ×1 ~ ×10) | Provider 없이 tsdb 에 직접 INSERT 해 CDC 부하만 분리 측정 | 1 |
| 서비스 단위 | JUnit + 가짜 이벤트 | 매핑 · 멱등 · 상태 전이 | 2 |
| 통합 E2E | compose (HotDB + 서비스 4) | tsdb INSERT → svc_* 반영 · 지연 · 건수 대조 | 2 |
| 장애 | 서비스 강제 종료 · 네트워크 단절 · DB 재기동 | 재접속 후 누락 0 · 중복 0 · 슬롯 WAL 보존 | 2 |
| 서버 to 서버 | HotDB 서버 1 + 서비스 서버 1~2 | 네트워크 경유 지연 · walsender 부하 | 2 (R1-b) |
| 대조 | `ops.reconcile` 잡 | 보존 창 안 tsdb ↔ svc_* 파생값 일치 | 2 |

합격 기준 (초안, 실측 후 확정):

| 항목 | 기준 |
|---|---|
| 지연 p99 (`applied_at - received_at`) | ≤ 2초 (×1 부하), ≤ 5초 (×5) |
| 누락 · 중복 | 0 (장애 시나리오 포함) |
| 슬롯 지연 | 정상 시 ≤ 16MB, 소비자 5분 정지 후 10분 안에 회복 |
| HotDB CPU | ×1 부하에서 평균 ≤ 30% |

---

## 08. 모니터링 (R1-a · R5)

Prometheus + Grafana. 대시보드는 HotDB · CDC 소비자 · 레거시 유입 3장.

| 대상 | 수집기 | 지표 |
|---|---|---|
| 호스트 (HotDB · OT 서버) | node_exporter | CPU · 메모리 · 디스크 IO · 네트워크 |
| 컨테이너 | cAdvisor | 컨테이너별 CPU · 메모리 |
| PostgreSQL | postgres_exporter + 사용자 쿼리 | 슬롯별 지연 bytes (`pg_replication_slots`), walsender 수 · 상태, WAL 생성률, 커넥션, 테이블 크기 |
| TimescaleDB | 사용자 쿼리 | 하이퍼테이블 · 청크 수 · 압축률 · 잡 실패 (`timescaledb_information.job_stats`) |
| CDC 소비자 (서비스) | Micrometer (`/actuator/prometheus`) | 이벤트/초, 적용 지연 히스토그램, 역매핑 캐시 미스, 실패 · 격리 건수, JVM 힙 · GC |
| 레거시 유입 | Micrometer | 폴링 주기별 건수 · 소요 · 워터마크 지연 |
| 대조 | `ops.reconcile` → exporter | 불일치 건수 |

경보 (초안): 슬롯 지연 > 1GB · 소비자 down 1분 · 지연 p99 > 5초 5분 지속 · `max_slot_wal_keep_size` 80% · 잡 실패.

---

## 09. 단계와 산출물

| 단계 | 내용 | 산출물 | 완료 기준 |
|---|---|---|---|
| **1. HotDB** ✅ | Flyway 마이그레이션(스키마 · 계정 · publication) · 레거시 DDL 생성 · 발행기 · 스키마 테스트 · CDC 스파이크 · 모니터링 | `src/hotdb/db` · `app/hotdb-migrate` · `app/field-simulator` · `monitoring` | 완료 2026-10-06 |
| **2. 판별 모듈 CDC** (진행) | `cdc-core` + 판별 모듈 N 개(등록부 · 관계형 RDB) · E2E · 장애 — **완료**. 서버 to 서버 부하 측정 · 실적 판별 로직 — 남음 | `app/cdc-core` · `app/zone-service` · V7 | §07 합격 기준 |
| **3. 레거시** ✅ (대역) | RFC Service(SAP 폴링 + 2단 CDC → SAP Z 표) · DB Agent(Oracle 폴링) — 로컬 대역으로 통합 테스트 통과. 운영 SAP · Oracle 접속, JCo, 실제 레거시 표 작업 목록 — 남음 | `app/poll-core` · `app/rfc-provider` · `app/db-agent` · V8 | 증분 누락 0 |

---

## 10. 결정 필요

| # | 질문 | 권장 | 영향 |
|---|---|---|---|
| ~~Q1~~ | tsdb CDC 방식 | **확정: A** (2026-10-06 스파이크 · 1단계 구현) | §04.1 · §05 |
| Q2 | 권역 서비스마다 슬롯 1개(요구 원문대로) vs 공용 CDC 1개 + 팬아웃 | 우선 원문대로 4슬롯, 부하 실측 후 재판단 | HotDB CPU · WAL |
| ~~Q3~~ | 레거시 스키마 이름 | **확정: 시험용으로 지어낸 표** (`erp` · `mes` · `lgs`) — 실제 이름 · 값은 저장소에 두지 않는다 | 보안 |
| Q4 | 서버 to 서버 테스트 장비 — 실서버 전 단계에 쓸 두 번째 호스트 | 확인 필요 | R1-b 일정 |
| Q5 | 도장(PLC) · 가공 필드 정의 | 정의서 전까지 모듈은 떠 있고 LiDAR 모양 대역 장비로만 확인 (`up.ps1 -AllZones`) | svc_pnt · svc_mch 라우트 |
| Q6 | tsdb 보존 기간 · 압축 지연 | 보존 90일, 압축 7일 후 | 디스크 · B2 |
| Q7 | RFC 함수 호출(JCo) — S-user 로 sapjco3 확보 여부와 호출할 RFC 함수(Z 모듈) | 확보 전까지 JDBC 로 Z 표 INSERT | `RfcSender` 구현 하나 |
| Q8 | 실제 레거시 폴링 범위 — 표별 워터마크 컬럼 · 주기 · PK 없는 표 처리 | PK 와 변경 일시 컬럼이 있는 표부터, PK 없는 큰 표는 replace 대신 원천 뷰로 키를 만든다 | 작업 목록 · 원천 부하 |
| Q9 | 모듈마다 판별 규칙 · 라우트가 다른가 | 같은 라우트로 시작, 다르면 `application-<모듈>.yml` 에 그 모듈 라우트만 | 판별 로직 (S7) |
| Q10 | 판별 모듈 앞에 "순차적으로 적재를 위한 큐" (그림 메모) — 무엇의 순서를 지키려는가 | 지금 모듈 안에서는 이미 순차 (슬롯 하나 · 엔진 단일 스레드 · WAL 순서대로 배치 커밋). 큐가 필요한 경우는 ① 한 모듈을 여러 인스턴스로 늘릴 때(장비 단위 순서 보장) ② 판별 로직이 느려 CDC 를 막을 때(받기 · 판별 분리) — 어느 쪽인지 확인 후 설계 | 모듈 구조 · 슬롯 수 · Kafka 미사용 결정과 충돌 여부 |
