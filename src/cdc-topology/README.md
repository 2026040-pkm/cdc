# cdc-topology — tsdb · rdb 를 CDC 로 어디까지 옮길 것인가

LiDAR 적재 DB(원천)에서 수신 DB 로 무엇을 CDC 로 옮길지, 다섯 가지 구성을 같은 기준으로 비교한다.

| # | 구성 | 한 줄 |
|---|---|---|
| ① | tsdb → cdc → rdb | 시계열 원문을 일반 PostgreSQL 로 |
| ② | tsdb + rdb → cdc → rdb | 원문 + 상태 표를 일반 PostgreSQL 로 |
| ③ | tsdb → cdc → tsdb | 시계열 원문을 다른 TimescaleDB 로 |
| ④ | tsdb + rdb → cdc → tsdb | 원문 + 상태 표를 TimescaleDB 로 (일반 표로 받는다) |
| ⑤ | tsdb + rdb → cdc → tsdb + rdb | 원천과 같은 모양(하이퍼테이블 + 일반 표)으로 통째로 |

여기서 **tsdb** 는 하이퍼테이블(`tsdb.lidar_status` · `lidar_scan_actual` · `lidar_scan_artifact` — 시간에 비례해 자라는 원문),
**rdb** 는 일반 표(`rdb.lidar_device_state` · `lidar_block_progress` · `lidar_block_artifact` · `lidar_tag_catalog` 등 —
행 수가 유계인 상태 · 등록부)다. 수신 쪽 rdb 는 일반 PostgreSQL, tsdb 는 TimescaleDB 인스턴스를 뜻한다.
지금 돌고 있는 `src/embedded-cdc-tsdb` 는 **rdb → cdc → tsdb 인스턴스(일반 표)** 로, ④ 에서 tsdb 를 뺀 모양이다.

## 00 결론

**⑤ 가 수신 측에 가장 많은 것을 주지만, tsdb 구간을 CDC(logical decoding)로 옮기는 안은 다섯 중 어느 것도
권하지 않는다.** 이 이미지(TimescaleDB 2.29.2 / PG17)에서 하이퍼테이블 행은 일반 publication 으로 나오지 않고,
나오게 하려면 원천에서 새 하이퍼테이블 · 연속 집계를 못 만들게 된다(아래 01절 Q1 · Q6). 압축 · 보존으로 원천에서 행이 사라져도
CDC 에는 아무것도 오지 않는다(Q2 · Q3).

그래서 권고는 목표와 수단을 나눈 **⑤ 의 변형**이다.

```
                  ┌─ rdb (상태 · 등록부) ── logical CDC (지금 embedded-cdc-tsdb) ──▶ 수신 rdb
원천 적재 ─────────┤
 (Kafka/NATS 소비)  └─ tsdb (원문) ── CDC 가 아니라 적재 분기: 같은 소스를 소비자 하나 더 ──▶ 수신 tsdb
```

tsdb 는 DB 를 거쳐 꺼내지 말고, DB 에 들어가기 **전**의 흐름(Kafka 토픽 · Engine NATS)에서 한 번 더 받는다.
2026-09-17 측정에서 이 방식이 실제로 성립함을 확인했다 — ISL Engine NATS 에 Db.Provider 를 하나 더 붙였더니
MQTT 직결과 같은 행 수(status 157,338행 · 도달률 93.7%)가 기존 경로에 영향 없이 들어왔다(`src/lidar-compare`).

| 순위 | 구성 | 판정 | 이유 한 줄 |
|---|---|---|---|
| 1 | ⑤ 의 변형 (rdb=CDC · tsdb=적재 분기) | **권고** | 수신이 원천과 같은 것을 갖되, 각 구간을 성립하는 수단으로 옮긴다 |
| 2 | ④ 에서 tsdb 를 뺀 것 (= 현재) | 유지 | 상태 · 등록부는 CDC 로 잘 간다. 수신에 이력이 없는 것만 한계 |
| 3 | ⑤ · ③ (tsdb 도 CDC) | 비권고 | 청크 이름 역매핑 · 원천 DDL 제약 · 압축/보존 불가시 — 파이프라인 재설계 규모 |
| 4 | ② | 비권고 | ③ 의 문제 + 수신 일반 PG 가 원문을 압축 · 보존 없이 무한히 받는다 |
| 5 | ① | 비권고 | ② 에서 쓸모 있는 rdb 까지 빠진 것 |

같은 목적을 **물리 복제(스트리밍 standby)** 로도 얻을 수 있다. 수신이 원천과 스키마 · 버전이 같고 읽기 전용이면
⑤ 의 가장 싼 답은 CDC 가 아니라 standby 다. 수신이 다른 스키마로 받거나(매핑 · 필터) 쓰기도 해야 할 때만 위 권고가 필요하다.

## 01 실기로 확인한 것 — 하이퍼테이블은 CDC 에 어떻게 보이나

`scripts/spike.ps1` 이 작은 TimescaleDB(2.29.2-pg17, tmpfs)를 띄워 `spike/hypertable-decoding.sql` 을 돌린다.
슬롯 셋(test_decoding · pgoutput+`FOR TABLE` · pgoutput+`FOR ALL TABLES`)을 같이 열고 단계마다 이벤트를 센다.
요약은 [results/2026-09-17-summary.md](results/2026-09-17-summary.md), 원자료는 같은 폴더의 txt 다.

| # | 질문 | 결과 | 영향받는 구성 |
|---|---|---|---|
| Q1 | `FOR TABLE <하이퍼테이블>` 로 행이 나오나 | **0건.** 청크에 든 행은 나오지 않는다. 새 청크도 안 따라온다 | ①~⑤ 의 tsdb 전부 |
| Q1b | `FOR ALL TABLES` 면 | 나온다. 단 청크 이름(`_hyper_1_N_chunk`)으로, TimescaleDB 카탈로그 변경이 섞여서 | 〃 |
| Q6 | `FOR ALL TABLES` 가 있을 때 새 하이퍼테이블 · 연속 집계 | **만들 수 없다** (`cannot create hypertable … because it is part of a publication`) | 〃 — 원천 운영을 CDC 가 막는다 |
| Q2 | 압축 | 원래 행은 **이벤트 없이 사라지고**, 압축 뭉치 INSERT(해석 불가)만 나온다 | ③ ④ ⑤ — 수신이 원천 모양을 따라가려면 |
| Q3 | 보존 (`drop_chunks`) | 데이터 행 삭제 **0건**. 카탈로그 DELETE 만 | 전부 — 수신은 보존 창을 따로 가져야 한다 |
| Q4 | 연속 집계 refresh | materialization 청크 INSERT 가 쏟아진다 (원문 700행 → 700건 · 카탈로그 변경 수십 건 별도) | ⑤ — 집계를 CDC 로 옮기면 이중 계산 |
| Q5 | 부모의 REPLICA IDENTITY 가 새 청크에 상속되나 | **상속된다** | 설정 부담은 없다 |
| C1 | 행당 WAL | 하이퍼테이블 INSERT 350 B · 상태 표 UPDATE(FULL) 280 B · (DEFAULT) 254 B | 비용은 구성보다 쓰기 빈도가 정한다 |

> 기존 `docs/cdc/timescaledb-cdc-impact.html` B2 는 "압축이 대량 DELETE 로 흘러가 target 을 지운다" 로 적었다.
> 이 버전에서는 DELETE 가 **안 나온다** — target 이 지워지지는 않지만, 원천에서 사라진 사실도 전달되지 않는다.
> Q6 은 그 문서에 없던 제약이다.

## 02 입력 — 누가 무엇을 얼마나 보내나

구성을 비교하려면 "원천에 무엇이 어떤 속도로 쓰이는가" 가 먼저다. 발행기(`util/mqtt-lidar-sim`) 기본값 · 350대 기준.

| 채널 | 송신 주체 · 토픽 | 주기 · 건수 | 원천 tsdb (원문) | 원천 rdb (상태) | rdb 변경 빈도 |
|---|---|---|---|---|---|
| status | LiDAR 장비 · `ot/device/{zone}/lidar/status` | 1초/대 · 1건 | `tsdb.lidar_status` 350행/초 | `rdb.lidar_device_state` 350행 고정 | **~304 UPDATE/초** (실측) |
| actual | LiDAR 장비 · `ot/sensor/{stage}/actual` | 1분/대 · 1건 | `tsdb.lidar_scan_actual` 5.8행/초 | `rdb.lidar_block_progress` 349행 고정 | 5.8 UPSERT/초 이하 |
| artifact | 추론 서버 · `ot/pipeline/{zone}/{shop}/{bay}/artifact` | 1분/대 · 12건 | `tsdb.lidar_scan_artifact` 70행/초 | `rdb.lidar_block_artifact` 349행 고정 | 70 UPSERT/초 이하 (배치마다 블록당 1) |
| 등록부 | ISL Engine · 태그 등록 | 거의 없음 | — | `rdb.lidar_tag_catalog` 2,310행 | 0 |
| 원장 | 적재 소비자 | 1초/배치 | — | `rdb.lidar_status_message` 증가 | 1 INSERT/초 |

필드 단위 카탈로그(장비 id · TagId · 값 · 시각이 경로마다 어디서 오는가)는 `src/lidar-compare/README.md` 의
"필드 카탈로그" 절이 원본이다. 여기서는 되풀이하지 않는다.

**크기** — 원문(tsdb)은 2026-09-17 측정에서 DB 하나가 10분에 128 MB(12 → 140 MB, 인덱스 포함 · 무압축) 늘었다.
하루 약 18 GB 다. 상태 표(rdb)는 전부 합쳐 3.9 MB 에서 멈춘다(`docs/cdc/lidar-cdc-db-flow.html`).

**이벤트 수** — 여기가 직관과 다르다. 상태 표로 접으면 저장은 170분의 1 이 되지만 **CDC 이벤트는 거의 안 줄어든다.**
`lidar_device_state` 350행에 UPDATE 이벤트 1,278,468건이 왔고, 같은 기간 원문은 1,291,778행이었다
(`docs/cdc/lidar-cdc-db-flow.html`). 소비자가 1초마다 장비 350대의 최신값을 덮어쓰기 때문이다. C1 의 행당 WAL 도 같은
자릿수다. 즉 **rdb 만 옮긴다고 CDC 가 가벼워지는 것이 아니라, 수신 측이 받는 모양이 가벼워지는 것**이다.

## 03 다섯 구성 비교

| | ① tsdb→rdb | ② tsdb+rdb→rdb | ③ tsdb→tsdb | ④ tsdb+rdb→tsdb | ⑤ tsdb+rdb→tsdb+rdb |
|---|---|---|---|---|---|
| 수신이 얻는 것 | 원문 이력 | 원문 이력 + 현재 상태 | 원문 이력 (압축 · 집계 가능) | 원문 이력 + 현재 상태 | 원천과 같은 모양 |
| tsdb 캡처 성립 | ✗ Q1 · Q6 | ✗ | ✗ | ✗ | ✗ |
| 원천에 주는 제약 | `FOR ALL TABLES` → 새 하이퍼테이블 · 집계 금지 (Q6) | 〃 | 〃 | 〃 | 〃 |
| 원천 압축 · 보존 전달 | 안 됨 (Q2 · Q3). 수신이 따로 보존해야 | 〃 | 〃 — 수신이 같은 정책을 따로 건다 | 〃 | 〃 |
| 수신 크기 (하루) | ~18 GB · 무압축 · 보존 없음 | ~18 GB + 3.9 MB | 압축하면 그 비율만큼 | 〃 | 〃 |
| CDC 이벤트 (초) | ~426 INSERT | ~426 INSERT + ~380 UPDATE | ~426 | ~806 | ~806 + 집계 refresh (Q4 를 캡처하면) |
| 처리 여유 (Debezium 단건 UPSERT 2,467/초 실측) | 5.8배 | 3.1배 | 5.8배 | 3.1배 | 3배 미만 |
| 코드 변경 | 청크명 → 논리명 역매핑 · 카탈로그 표 필터 · 복합키 매퍼 | ① + 현재 핸들러 | ① + 수신 하이퍼테이블 | ② + 수신 하이퍼테이블 | ④ + 집계 제외 규칙 |
| 판정 | 비권고 | 비권고 | 비권고 (목표는 타당) | 비권고 (tsdb 뺀 것 = 현재) | 목표는 최고, 수단은 분리 |

읽는 법:

- **①~⑤ 의 차이는 tsdb 에 대해서는 무의미하다.** 다섯 모두 "하이퍼테이블 행을 CDC 로 꺼낸다" 는 같은 벽(Q1 · Q6)에 먼저 부딪힌다.
  수신이 rdb 냐 tsdb 냐는 그 벽을 넘은 다음의 문제다.
- **수신을 일반 PostgreSQL(rdb)로 두는 ① ② 는 벽을 넘어도 손해다.** 원문이 하루 18 GB 씩 압축도 보존도 없이 쌓이고,
  원천의 보존(drop_chunks)은 전달되지 않으므로(Q3) 수신이 원천보다 무한히 커진다. 대사(건수 비교)는 상시 불일치가 된다.
- **수신을 TimescaleDB 로 두는 ③ ④ ⑤ 는 받은 뒤가 맞다.** 수신에 같은 청크 · 압축 · 보존을 따로 걸면 된다.
  문제는 받는 수단뿐이다.
- **rdb 는 어느 수신으로 보내도 잘 간다.** 현재 스택이 실측으로 보였다 — 표 6개 · LSN 순서 가드 · 지연 평균 0.65초(device_state).
  수신을 TimescaleDB 인스턴스로 둔 것(현재)은 이득도 손해도 없다. 일반 표로 받기 때문이다.

## 04 권고 구조의 세부

**rdb 구간 — logical CDC 유지 (현재 `src/embedded-cdc-tsdb`).** 바꿀 것이 하나 있다. 이벤트가 원문 속도로 나오는 원인은
소비자가 1초마다 350대 전부를 덮어쓰는 데 있다. 상태 표 UPSERT 에 "값이 바뀐 행만" 조건
(`WHERE (EXCLUDED.status, EXCLUDED.error_code, …) IS DISTINCT FROM (…)`)을 걸면 `last_heartbeat_at` 처럼 매초 바뀌는 컬럼을
어떻게 다룰지 정하는 만큼 이벤트가 준다. 하트비트를 상태 표에서 빼면 status 이벤트는 상태가 바뀔 때만 나온다.
**측정 전이다** — 줄어드는 폭은 발행기의 상태 전이 확률이 정한다.

**tsdb 구간 — 적재 분기.** 원천 DB 를 거치지 않고, 원천이 소비하는 같은 흐름을 수신도 소비한다.

| 분기 지점 | 수단 | 2026-09-17 근거 | 비고 |
|---|---|---|---|
| Kafka `ot.lidar.*` | 소비자 그룹 하나 더 (`lidar-ingest` 이미지 그대로, `PG_DSN` 만 수신) | A 경로가 이 이미지로 돈다 | Kafka Provider 가 태그당 최신값으로 합쳐 artifact 가 1/3 로 준다(A 도달률 33.7%). 원천과 같은 양이다 |
| ISL Engine NATS | Db.Provider 를 하나 더 (`ConnectionString` 만 수신) | C 경로 — status · artifact 가 MQTT 직결과 행 수까지 같음 | 원천을 Kafka 경로로 적재하고 있다면 수신이 원천보다 **더 많이** 갖는다. 같게 맞추려면 원천 적재도 같은 지점에서 한다 |

어느 쪽이든 **원천과 수신이 같은 입력을 독립적으로 적재**하게 되므로, 둘의 대사는 "같은 이벤트 시각 창의 건수" 로 한다
(`src/lidar-compare/scripts/verify.ps1` 이 이미 그 모양이다). CDC 의 LSN 연속성 가드 같은 장치는 없다 — 분기 소비자가 멈춘 동안의
변경은 Kafka 는 보존 기간 안에서 되감을 수 있고, NATS(일반 구독)는 잃는다. 이 차이가 분기 지점을 고르는 기준이다.

**⑤ 를 CDC 로 해야만 할 때** (수신이 원천 DB 만 볼 수 있고 흐름에 접근할 수 없는 경우):
하이퍼테이블 대신 **시간 워터마크 증분 복사**가 logical decoding 보다 낫다. 하이퍼테이블은 append-only 라
`WHERE received_at > :watermark` 로 새 행만 가져오면 된다. Q1 · Q6 과 무관하고 원천 DDL 을 막지 않으며, 압축된 청크에 늦게
들어간 행도 `received_at` 이 새로 찍히므로 잡힌다. 단 **지금 스키마에는 `received_at` 인덱스가 없다** — 청크는 `time` 으로
갈리므로 `received_at` 조건만 걸면 청크마다 전체를 훑는다(2026-09-17 `cmp-direct-pg` 에서 최근 10초 조회가 청크 하나 33만 행을
걸러 84 ms). `time > :watermark - 늦음 허용폭` 을 같이 걸어 청크를 거르거나 `(received_at)` 인덱스를 더한다(원천 쓰기 비용이 는다).
이 방식의 지연 · 원천 부하는 **측정 전이다.**

## 05 확인하지 않은 것

| 항목 | 왜 중요한가 | 확인 방법 |
|---|---|---|
| 청크명 역매핑의 실제 비용(Q3 of impact 문서) | ③~⑤ 를 CDC 로 하게 될 때 커넥터 메모리 · 새 청크 시점 지연 | Debezium 에 `FOR ALL TABLES` 슬롯을 붙여 6시간 청크 경계를 넘긴다 |
| 상태 표 "바뀐 행만" UPSERT 의 이벤트 감소 폭 | rdb CDC 의 실제 부하 | `ingest.py` UPSERT 조건 변경 → `cdc_events_total{table="lidar_device_state"}` 전후 비교 |
| 워터마크 증분 복사의 지연 · 원천 부하 | ⑤ 를 DB 만으로 해야 할 때의 대안 | 1초 주기 `COPY (SELECT … WHERE received_at > …)` 를 원천에 걸고 CPU · 지연 측정 |
| 수신 TimescaleDB 의 압축 비율 | 수신 크기 산정 (지금 18 GB/일은 무압축) | lidar_status 청크 하나 `compress_chunk` 전후 크기. 스파이크의 합성 데이터(0.60)는 행이 너무 적어 의미 없다 |
| 물리 standby 로 ⑤ 를 대체할 때 | 스키마 매핑이 필요 없다면 가장 싼 답 | PG17 standby + `sync_replication_slots` 로 rdb CDC 슬롯까지 승계되는지 |

## 재현

```powershell
.\scripts\spike.ps1         # 약 30초. results\hypertable-decoding-<시각>.txt 에 남기고 컨테이너를 내린다
.\scripts\spike.ps1 -Keep   # psql -h localhost -p 64432 -U postgres spike_run 으로 들여다본다
```

포트 64432 · 데이터는 tmpfs 라 내리면 남지 않는다. 다른 스택과 같이 떠 있어도 부하가 없다(행 수천 개).
