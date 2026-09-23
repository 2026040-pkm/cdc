# latest — 최종본

CDC 검토가 끝나고 **실제로 남은 것만** 모은 폴더다. 이 폴더 하나만 보고도 적재부터
복제까지 띄울 수 있다. 바깥을 가리키는 경로가 없다.

기준 커밋 `a7ad5e6` · 2026-09-21 · PostgreSQL 17.11 / TimescaleDB 2.29.2

```
LiDAR 350대 ──MQTT──▶ (ISL) ──▶ EES Kafka ──▶ timescaledb ──logical CDC──▶ embedded-cdc-tsdb
                                              (원천 · 적재)                  (수신)
```

## 무엇이 들어 있나

| | 무엇 | 역할 |
|---|---|---|
| `timescaledb/` | 적재 스택 | Kafka `ot.lidar.*` 를 받아 TimescaleDB 에 넣는다. **CDC 의 원천**이기도 하다 |
| `embedded-cdc-tsdb/` | CDC 스택 | 그 원천의 WAL 을 Debezium Embedded 로 읽어 수신 TimescaleDB 에 복제한다 |
| `shared/embedded-cdc-source-init/` | 공유 스키마 | CDC 원천 표(`car` · `computer` · `grade` · `member` · `cdc_heartbeat` · `cdc_user` · publication). 원천 DB 가 마운트해 `\i` 로 읽는다 |
| `scripts/` | 두 스택 묶음 | 순서를 지켜 한 번에 올리고 내린다 |

## 기동 · 정지

**순서가 있다.** `timescaledb` 가 네트워크 `tsdb-net` 을 만들고, `embedded-cdc-tsdb` 가
거기에 external 로 붙는다. 순서를 바꾸면 두 번째가 네트워크를 못 찾아 기동 자체가 막힌다.

```powershell
.\scripts\up.ps1           # 1) timescaledb  2) embedded-cdc-tsdb
.\scripts\down.ps1         # 역순으로 정지 (데이터 유지)
.\scripts\down.ps1 -v      # 볼륨까지 삭제 → init SQL 재실행 · CDC 슬롯·오프셋 정리
```

```bash
bash scripts/up.sh
bash scripts/down.sh       # [-v]
```

스택 하나만 다루려면 각 폴더의 `scripts/up` · `down` 을 직접 쓴다.

> **Kafka 는 이 스택 밖(Aspire 세션)에 있다.** `timescaledb/scripts/up` 이 Kafka 컨테이너를
> `tsdb-net` 에 alias `kafka` 로 붙여 준다. 브로커가 광고하는 내부 주소가 `kafka:9093` 이라
> **alias 이름은 반드시 `kafka`** 여야 한다. 컨테이너 이름은 `timescaledb/kafka.env` 에서 바꾼다.

## 포트

| | 주소 | 계정 |
|---|---|---|
| 원천 TimescaleDB | `localhost:59432` / `lidar` | `postgres:postgres` |
| lidar-ingest 지표 | http://localhost:59080/metrics | |
| Prometheus (적재) | http://localhost:59090 | |
| Grafana (적재) | http://localhost:59380 | `admin/admin` |
| 수신 TimescaleDB | `localhost:60433` / `targetdb` | `postgres:postgres` |
| cdc-service | http://localhost:60080/actuator/health | |
| Prometheus (CDC) | http://localhost:60090 | |
| Grafana (CDC) | http://localhost:60300 | `admin/admin` |

> Grafana 가 59300 이 아니라 **59380** 인 이유 — Windows 가 `59248~59347` 을 예약해 두어
> 호스트에서 열리지 않는다. 컨테이너는 멀쩡한데 브라우저만 안 열리면 이것부터 확인한다.

## 두 스택의 경계 — 무엇이 CDC 로 가고 무엇이 안 가나

원천 DB 는 스키마 둘로 나뉘어 있고, **그 선이 곧 CDC 의 경계**다.

| 스키마 | 무엇 | CDC |
|---|---|---|
| `rdb` | 일반 표 — 상태·등록부. 행 수가 유계다 | **간다.** publication `embedded_cdc_pub` |
| `tsdb` | 하이퍼테이블 + 연속 집계 — 시간에 비례해 자라는 원문 | **안 간다** |

하이퍼테이블을 뺀 것은 성능 문제가 아니라 **행 단위 CDC 가 성립하지 않아서**다. 청크가 주기마다
새 표로 생기고, 압축은 행을 내부 압축 청크로 옮기고, 보존은 청크째 DROP 한다 — 어느 것도
INSERT/UPDATE/DELETE 로 보이지 않는다. 2026-09-17 실측(TimescaleDB 2.29.2/PG17)에서:

- `FOR TABLE <하이퍼테이블>` 로는 행이 **0건** 나온다
- 압축은 **DELETE 도 TRUNCATE 도 남기지 않는다** — 원본 행이 이벤트 없이 사라진다
- `drop_chunks` 도 데이터 행 삭제 **0건**
- `FOR ALL TABLES` 로 넓히면 잡히긴 하지만 **원천에서 새 하이퍼테이블·연속 집계를 못 만들게 된다**

**스키마를 나눈 것이 그 선을 구조로 못 박은 것이다** — publication 에 하이퍼테이블이 실수로
끼어드는 것을 사람 주의가 아니라 구조가 막는다. 감시에도 심어 두어, 지표의 `captured` 라벨이
`rdb=yes` · `tsdb=no` 가 아닌 줄을 내면 뭔가 끼어든 것이다.

## 왜 compose 를 하나로 합치지 않았나

합칠 수 있을 것처럼 보이지만 합치지 않았다.

1. **두 스택은 별개 compose 프로젝트다** (`name: timescaledb`, `name: embedded-cdc-tsdb`).
   각자 네트워크·볼륨·컨테이너 이름을 갖는다.
2. **수신 스택은 원천의 네트워크를 external 로 참조한다.** compose 는 "다른 프로젝트가
   네트워크를 만들 때까지 기다리기" 를 표현하지 못한다. 그 순서는 스크립트가 지킨다.
3. **`include` 는 `-f` 여러 개와 다르다.** 가져온 서비스를 다시 선언해 덮어쓰려 하면 병합되지
   않고 `services.<이름> conflicts with imported resource` 로 기동이 막힌다.

그래서 `scripts/up` 은 compose 를 합치는 대신 **두 스택을 순서대로 호출**한다.

## src/ 와의 관계

이 폴더는 `src/timescaledb` · `src/embedded-cdc-tsdb` 의 **사본**이다. 원본도 그대로 있다.

- 앞으로 고칠 곳을 **한 곳으로 정하고** 쓴다. 두 곳을 각각 고치면 반드시 어긋난다.
- 사본을 만들며 바꾼 것은 **경로 하나뿐**이다 — 원천 DB 가 마운트하는 CDC 공유 스키마가
  `src/backup/embedded-cdc/...` 대신 `shared/embedded-cdc-source-init` 을 본다. 나머지
  설정·SQL·대시보드·경보는 원본과 같다.

## 여기 없는 것

| 무엇 | 어디에 | 왜 뺐나 |
|---|---|---|
| `cdc-topology` | `src/cdc-topology` | 하이퍼테이블 논리 복제 스파이크. 위 실측의 **근거**이지 돌리는 스택이 아니다 |
| `lidar-compare` | `src/lidar-compare` | 세 적재 경로 비교 실험. 결론을 내는 데 쓴 것이라 최종 구성에 속하지 않는다 |
| `embedded-cdc` · `embedded-cdc-go` · `cdc-custom` | git 이력 (`d184a21` 직전) | CDC 방식 비교용 세 구현. 검토가 끝나 저장소에서 내렸다 |

## 문서

| | |
|---|---|
| 태그 기준 RDB·TSDB 스키마 설계 (HotDBProvider 전환 제안) | [lidar-tag-db-schema.html](docs/lidar-tag-db-schema.html) |
| PG 설정 가이드 (필수·권장·금지) | Confluence `[PoC] CDC의 PG 설정 가이드` |
| 하이퍼테이블 CDC 영향 분석 | `docs/cdc/timescaledb-cdc-impact.html` |
| 지금 돌고 있는 것 전경 | `docs/cdc/current-lidar-cdc-overview.html` |
| 5안 비교와 권고 구성 | `src/cdc-topology/README.md` |

## 운영으로 넘기기 전에 정할 것

- **`max_slot_wal_keep_size`** — 지금 `-1`(상한 없음)이라 컨슈머가 오래 죽어 있으면 슬롯이
  붙잡은 WAL 이 **원천 디스크를 채울 때까지** 쌓인다. 권고는 디스크 여유의 20~30%.
  선행 작업은 운영 변경 속도 실측 하나다.
- **PG17 로 갈지** — 페일오버하면 복제 슬롯은 **반드시** 사라진다(승계해 주는
  `sync_replication_slots` 가 PG17 부터). 이중화를 쓸 거면 이게 곧 대응책이다.

자세한 근거와 나머지 미결 항목은 위 Confluence 문서에 있다.
