# timescaledb — ot-lidar-status → TimescaleDB 적재 + 모니터링

Kafka 토픽 `ot-lidar-status` 에 계속 발행되는 LiDAR 장비 상태를 PostgreSQL 17 + TimescaleDB 2.29 에
쌓고, 그 DB 와 적재 파이프라인을 Prometheus · Grafana 로 본다.

```
Aspire(InterSysLink) Kafka ──ot-lidar-status──▶ lidar-ingest ──▶ TimescaleDB(lidar)
                                                    │                 │
                                                 /metrics       postgres_exporter
                                                    └────▶ Prometheus ◀────┘
                                                                │
                                                             Grafana
```

## 기동 · 정지

```bash
scripts/up.sh        # 또는 scripts\up.ps1 — DB + 소비자 + 모니터링, Kafka 컨테이너를 tsdb-net 에 붙인다
scripts/verify.sh    # 오프셋 · 행 수 · 장비 상태 · 정책 잡 · 소비자 지표 한 번에
scripts/down.sh      # 정지 (데이터 유지).  -v 를 주면 볼륨까지 삭제 → init SQL 이 다시 돈다
```

| 서비스 | 주소 |
|---|---|
| TimescaleDB | `localhost:59432` · DB `lidar` · postgres / postgres |
| lidar-ingest | http://localhost:59080/metrics |
| Prometheus | http://localhost:59090 |
| Grafana | http://localhost:59300 · admin / admin (익명 Viewer 허용) |

Kafka 는 이 스택이 띄우지 않는다. Aspire 세션이 띄우는 `kafka-xxxxxxxx` 컨테이너를 쓴다.
**Aspire 세션을 다시 띄웠으면 `scripts/up.sh` 를 다시 실행한다** — 새 Kafka 컨테이너를 다시 붙여야 한다.

### 왜 Kafka 컨테이너를 이쪽 네트워크에 붙이는가

브로커는 자기 네트워크 안에서 `kafka:9093` 으로 광고된다. 호스트의 `127.0.0.1:9092` 는 Aspire 프록시라
podman VM 안 컨테이너에서는 보이지 않는다. 소비자를 Aspire 세션 네트워크에 넣어 보면 **Aspire 가 5초 안에
낯선 컨테이너를 떼어 낸다** (`podman events --filter type=network` 로 확인, 2026-09-09). 반대로 Kafka
컨테이너를 `tsdb-net` 에 alias `kafka` 로 붙이면 떼지 않는다. `up` 이 그 일을 하고 `down` 이 되돌린다.

## 메시지 → 표

레코드 하나 = 장비 약 210대의 상태 배열. 헤더에 `uniqueid` · `msg_timestamp` · `message_version` · `method_id` 가 붙는다.

```json
[{"content": {"timestamp": 1788936651940295100, "tid": "58", "pm_mode": false,
              "value": "{\"status\":\"ONLINE\",\"error_code\":\"\",\"scan_rate_pts_per_sec\":244448,
                        \"point_cloud_quality_score\":0.9641,\"temperature_c\":38.85,\"connectivity_rssi\":-38,
                        \"last_heartbeat_at\":\"2026-09-09T15:50:51.9402928+09:00\",
                        \"occurred_at\":\"2026-09-09T15:50:51.9402951+09:00\",
                        \"ingested_at\":\"2026-09-09T15:50:51.9402951+09:00\",
                        \"idempotency_key\":\"f4f3434ef3ce460e934acba181fc04c5\"}"}}, ...]
```

| 표 | 성격 | 내용 |
|---|---|---|
| `lidar_status` | 하이퍼테이블 · 청크 6h · 압축 3일 후 · 보존 31일 | 항목 하나 = 행 하나. `time` = `occurred_at`. `content.value` 의 열 개 필드 + `content_ts` · `pm_mode` · `kafka_offset` |
| `lidar_status_message` | 일반 테이블 | Kafka 레코드 하나 = 행 하나. 파티션·오프셋·헤더·항목 수·격리 수 |
| `lidar_device_state` | 일반 테이블 · 장비당 1행 | 장비별 최신 상태. exporter 와 대시보드가 하이퍼테이블 대신 이것을 읽는다 |
| `lidar_ingest_reject` | 일반 테이블 | 파싱 실패 항목의 원문과 사유 |
| `lidar_status_1m` | 연속 집계 | 장비별 1분: 온도·스캔 속도·품질·RSSI 평균, 상태별 건수 |
| `lidar_error_1m` | 연속 집계 | 오류 코드별 1분 건수·장비 수 |

멱등성: `(idempotency_key, time)` 유니크 인덱스 + `ON CONFLICT DO NOTHING`. Kafka 오프셋은 DB 커밋 뒤에만
커밋한다(at-least-once). 토픽 자체에도 같은 항목이 다시 실려 오는 경우가 관측됐다(레코드 50개 중 20~190행) —
대시보드의 "중복 스킵" 이 그것이고 유실이 아니다.

DDL 은 `infra/db/init/01-schema.sql`, 정책은 `02-policies.sql`. 스키마를 고쳤으면 `down.sh -v` 로 볼륨을 지워야 반영된다.

## 모니터링

| 대시보드 | 내용 |
|---|---|
| **OT LiDAR 적재 (Kafka → TimescaleDB)** | 적재 행/초 · Kafka 지연 · end-to-end 지연 · 배치 시간 · 중복/격리, 상태별 장비 수, 장비 최신 상태 표, 장비별 온도·스캔 속도·품질 추이(`$tid`), 오류 코드별 발생 |
| **TimescaleDB 상태 (lidar)** | DB·하이퍼테이블 크기, 청크·압축률, TPS, 튜플 삽입 vs 적재 행, 캐시 적중률, WAL, 연결·락·dead tuple, 체크포인트, 백그라운드 워커, 디스크, 정책 잡 실행 표 |

데이터소스는 둘이다. Prometheus 는 집계(상태별 대수 등) 만 들고, 장비 단위 추이는 TimescaleDB 직결로
연속 집계를 SQL 로 읽는다 — 장비 210대 × 필드를 Prometheus 레이블로 올리면 시계열이 터진다.

exporter 커스텀 쿼리(`infra/monitoring/exporter/queries-tsdb.yaml`): 하이퍼테이블 크기·청크·압축 통계,
정책 잡 상태, 장비 상태 집계, WAL, 백그라운드 워커. 5초 스크랩이라 하이퍼테이블 전체를 훑는 쿼리는 없다.

경보(`infra/monitoring/prometheus/rules/tsdb-alerts.yml`): 소비 정지 · Kafka 지연 · end-to-end 지연 · 격리 발생 ·
DB 오류, ERROR 장비 수 · 무응답 장비 · 데이터 신선도, DB down · 정책 잡 실패 · 디스크 · 연결 수 · 캐시 적중률.

대시보드 JSON 옆의 `.outline.txt` 는 `../embedded-cdc/scripts/dashboard-outline.js --write` 로 만든 개요다.
JSON 을 고쳤으면 같이 갱신한다.

## 소비자 (dev/ingest)

Python · confluent-kafka · psycopg3. 레코드 최대 50개 또는 1초마다 한 트랜잭션으로
`lidar_status` INSERT → 배치 안 장비별 최신값으로 `lidar_device_state` UPSERT(오래된 이벤트는 못 덮음) →
`lidar_status_message` INSERT → 격리 INSERT → COMMIT → Kafka 오프셋 커밋. DB 오류는 같은 배치를 물고
재시도하고, Kafka 오류(브로커 해석 실패 등)는 죽지 않고 재시도한다.

환경변수는 `kafka.env` 와 `dev/docker-compose.app.yml` 참고.

## 관측치 (2026-09-09)

- 레코드 간격 1~3초, 레코드당 ~210 항목 → 70~210 행/초. 배치 DB 쓰기 ~0.4초 / 10,000행.
- 장비 210대. 상태 분포 ONLINE 88% · IDLE 6% · WARNING 5% · ERROR 0.6%.
- 오류 코드: E-NET-0007, E-LDR-0101 / 0203 / 0311 / 0402.
