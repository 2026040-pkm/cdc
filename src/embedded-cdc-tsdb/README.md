# Embedded CDC (TimescaleDB target)

`../embedded-cdc` 와 **표 구성이 같고 수신 측 엔진만 다른** 판이다. Debezium Embedded Engine 을
내장한 같은 Spring Boot(Java 21) 서비스가 같은 원천을 읽고, 받는 쪽이 PostgreSQL 16 대신
**TimescaleDB 2.29 / PostgreSQL 17** 이다. 원천이 TimescaleDB 이므로 "같은 엔진으로 받을 때" 가
어떻게 다른지를 나란히 두고 보기 위한 스택이다.

```
lidar TimescaleDB ──logical replication(pgoutput)──▶ cdc-service ──JDBC──▶ Target TimescaleDB
  (src/timescaledb · WAL)                     (Debezium Embedded)        (멱등 UPSERT)
```

> **두 스택을 동시에 띄운다.** 같은 원천에 슬롯이 둘 붙으므로 커넥터 정체성을 전부 갈라 두었다.
>
> | | embedded-cdc | embedded-cdc-tsdb |
> |---|---|---|
> | 슬롯 | `embedded_cdc_slot` | `embedded_cdc_tsdb_slot` |
> | 커넥터 이름 / topic prefix | `embedded-cdc` / `embedded` | `embedded-cdc-tsdb` / `embedded-tsdb` |
> | heartbeat 행 (`cdc_heartbeat.pipeline`) | `embedded-cdc` | `embedded-cdc-tsdb` |
> | offset 볼륨 | `emb-cdc-offsets` | `emb-cdc-tsdb-offsets` |
> | target | PostgreSQL 16 · `localhost:56433` | TimescaleDB 2.29/PG17 · `localhost:60433` |
> | 포트 대역 | 56xxx | 60xxx |
>
> **publication(`embedded_cdc_pub`) 만 공유한다.** publication 은 "무엇을 캡처하는가" 의 정의일
> 뿐이고 하나를 여러 슬롯이 읽는 것은 정상이다. 원천의 `04-cdc-lidar.sql` 이 만든 것을 그대로 쓴다.
>
> ⚠️ 검증 테스트(`scripts/verify`)는 원천에 `verify_record` 표와 `verify_record_pub` 를 만든다.
> 그 이름을 두 스택이 공유하므로 **두 스택의 verify 를 동시에 돌리지 않는다.**

- **car** : source ↔ target 스키마 동일 → 1:1 복제
- **computer** : 스키마 다름 → 매핑(full_name/spec/price_krw) + 소프트 삭제 + LSN 순서 가드
- **lidar_device_state · lidar_status_message · lidar_ingest_reject · lidar_tag_catalog** : lidar 스택의 일반 표. 컬럼이 같아 컬럼 목록만으로 옮기는 passthrough 핸들러 + LSN 순서 가드. 원천 컬럼이 바뀌면 `LidarPassthroughHandlers` 와 `infra/db/target/init/02-lidar.sql` 을 같이 고친다 (`lidar_tag_catalog` 는 Kafka 의 숫자 tid 를 장비로 되돌리는 InterSysLink 등록부 사본이다)
- **모니터링** : Grafana(+Prometheus, postgres_exporter×2, podman-exporter) — 정합성·처리량·지연·slot lag·컨테이너 리소스.
  원천이 lidar DB 로 바뀌면서 **"원천 커밋 속도" 를 DB 전체로 재지 않는다** — 하이퍼테이블 적재가 섞여
  CDC 가 늘 뒤처지는 것처럼 보이기 때문이다. publication 에 든 표의 변경만 세는 `cdc_source_activity_*` 를
  따로 두고, DB 전체 값은 참고선으로만 겹친다. PoC 대시보드 13번 항목이 그 구분을 한 장으로 보여 준다.
  REPLICA IDENTITY·TOAST·컬럼 폭 지표도 publication 에 든 표만 본다 — 그러지 않으면 캡처하지 않는
  하이퍼테이블 뿌리가 "설정이 빠진 표" 로 잡힌다.
  Overview 아래 두 항목은 **옮기는 것 말고 있는 것 전부**를 낸다 — `DB 구성` 은 원천의 `rdb`·`tsdb` 스키마와
  수신 `targetdb` 의 `public` 스키마 표를
  캡처 여부·행 수·크기·청크까지 전부 세우고, `구독 토픽` 은 `ot.lidar.*` 세 토픽이 어디로 들어가고 그중
  무엇만 CDC 로 넘어오는지를 수신 DB 의 원장(`lidar_status_message`)·등록부(`lidar_tag_catalog`)에서 직접 읽는다.

> **원천은 lidar 스택(`../timescaledb`)의 TimescaleDB 다.** 그 DB 에 이 스택의 원천 스키마
> (`infra/db/source/init/01-schema.sql`)가 그대로 마운트되어 car · computer · grade · member ·
> cdc_heartbeat 가 만들어지고, lidar 표 네 개가 같은 publication 에 더해진다.
> 그래서 **lidar 스택을 먼저 올려야 한다** (`../timescaledb/scripts/up.sh`). `up` 이 확인한다.
>
> **캡처 대상은 전부 원천의 `rdb` 스키마에 있다.** 그 DB 는 스키마를 둘로 나눠 두었다 —
> `rdb` 는 행 단위 CDC 가 성립하는 일반 표, `tsdb` 는 하이퍼테이블과 연속 집계다.
> 그래서 `table-include-list` 가 `rdb.car` 처럼 수식돼 있고, 하이퍼테이블은 목록에 넣고 말고를
> 따질 필요 없이 애초에 다른 스키마에 있다.
>
> 하이퍼테이블 셋(`tsdb.lidar_status` · `tsdb.lidar_scan_actual` · `tsdb.lidar_scan_artifact`)은 캡처하지 않는다.
> 청크가 주기마다 새 테이블로 생기고 압축·보존이
> 내부 경로로 행을 옮기고 지워서 행 단위 CDC 가 성립하지 않는다 —
> [docs/timescaledb-cdc-impact.html](../../docs/timescaledb-cdc-impact.html) B안. 그 데이터는
> lidar 스택의 소비자가 직접 적재한다. 같은 문서의 ADR-01(센서 저장소는 CDC 원천과 분리)과 어긋나는
> 구성이라는 점을 알고 쓴다.
>
> 예전 원천 컨테이너(`infra/db/docker-compose.source.yml`)는 파일만 남아 있다. 되돌리려면
> 루트 compose 의 include 주석을 풀고 `dev/docker-compose.app.yml` 의 `SOURCE_DB_*` 를 바꾼다.

파이프라인 자체의 설명(핸들러·DLQ·LSN 가드·검증 시나리오)은 두 스택이 같다 —
**[../embedded-cdc/docs/architecture.html](../embedded-cdc/docs/architecture.html)** 참고.
이 폴더에는 문서를 두 벌 두지 않는다. 갈라지면 어느 쪽이 맞는지 알 수 없기 때문이다.

## 빠른 시작

docker 또는 podman 이 있으면 된다 (스크립트가 자동 감지).

```powershell
./scripts/up.ps1     # 전체 기동 (루트 docker-compose.yml 하나로 DB 2개 + 모니터링 + 앱)
./scripts/demo.ps1   # INSERT/UPDATE/DELETE 를 흘리고 source/target 비교
./scripts/load.ps1   # 5분마다 테이블당 INSERT 1000 / UPDATE 500 / DELETE 100 (지속 부하)
./scripts/verify.ps1 # 캡처 신뢰성 검증 테스트 V1~V6 (스택 기동 상태에서)
./scripts/down.ps1   # 정지 (-Wipe: 볼륨까지 삭제 → 다음 기동 시 snapshot 재실행)
```

macOS/Linux 는 `scripts/*.sh` 사용.

스크립트는 루트 `docker-compose.yml` 을 부를 뿐이라 아래와 같아도 된다.
네트워크·볼륨은 compose 가 만들고 지운다 — 미리 만들 필요 없다.

```bash
docker compose up -d --build   # 스택 루트에서
docker compose down -v
```

하위 compose 파일(`infra/db/*`, `infra/monitoring/*`, `dev/*`)은 루트가 `include` 로
끌어오는 조각이다. `-f` 로 하나씩 따로 올리면 파일마다 프로젝트가 갈려 같은
`container_name` 을 두고 충돌한다.

> **컨테이너 리소스 수집기는 엔진을 탄다.** 기본은 podman 소켓을 읽는
> `podman-exporter` 다. rootless podman 에는 `/var/lib/docker` 도 `docker.sock` 도 없고
> 컨테이너 cgroup 이 `user.slice` 아래로 들어가 cAdvisor 가 이름을 붙이지 못한다.
> docker 데몬 환경이면 `--profile cadvisor` 로 cAdvisor 를 대신 띄우고
> `infra/monitoring/prometheus/prometheus.yml` 의 cadvisor job 주석을 푼다.
> 대시보드는 `rules/container-resources.yml` 이 만드는 `cdc:container_*` 만 읽으므로
> 어느 쪽이든 같다.
>
> 소켓 uid 가 1000 이 아니면 `PODMAN_SOCK=/run/user/<uid>/podman/podman.sock` 로 덮어쓴다.

| 접속 | 주소 |
|---|---|
| Grafana | http://localhost:60300 (admin/admin) |
| Prometheus | http://localhost:60090 |
| cdc-service | http://localhost:60080/actuator/health |
| source DB | localhost:59432 · lidar · postgres/postgres (lidar 스택의 TimescaleDB) |
| target DB | localhost:60433 · targetdb · postgres/postgres (TimescaleDB 2.29 / PG17) |

## 검증 테스트

캡처 신뢰성 시나리오 6종(V1~V6)이 `dev/cdc-service/src/test` 에 구현돼 있다.
**스택이 떠 있는 상태에서** 실행한다 — 실제 source/target PostgreSQL 에 붙어 돌기 때문이다.
운영 테이블·`embedded_cdc_tsdb_slot` 은 건드리지 않고 `verify_*` 전용 테이블·publication·슬롯만 쓴다.

```powershell
./scripts/verify.ps1              # 전체 (약 3분)
./scripts/verify.ps1 -Tests "*V3*"  # 특정 시나리오만
```

| | 무엇을 확인하나 |
|---|---|
| V1 | INSERT/UPDATE/DELETE 지연, 변경 필드 식별, 1만 건 배치 처리량 |
| V2 | Provider 다운 구간의 변경분이 재기동 후 유실 없이 도착하는가 |
| V3 | 슬롯이 사라지면 조용히 넘어가지 않고 기동을 거부하는가, 재동기화로 복구되는가 |
| V4 | 오프셋 flush 전 중단으로 중복이 유입돼도 이중 반영되지 않는가 |
| V5 | TOAST 필드가 REPLICA IDENTITY 설정에 따라 어떻게 실려 오는가 |
| V6 | 건수·체크섬 대사가 유실을 검출하는가, heartbeat 로 슬롯이 전진하는가 |

산출물은 두 개다.

- `dev/cdc-service/build/verification/results.md` — 지연·처리량·WAL 보유량 등 계측치
- `dev/cdc-service/build/reports/tests/test/index.html` — JUnit 리포트

`JAVA_HOME` 이 없으면 스크립트가 `~/.jdks` 에서 JDK 21 을 찾아 쓴다.

## 대시보드 변경 추적

Grafana 대시보드는 JSON 파일이 원본이고, provisioning 이 10초마다 다시 읽는다.
문제는 이 JSON 이 3,000줄이 넘어 **패널에 쿼리 한 줄을 더해도 `git diff` 에서 묻힌다**는 것이다.
좌표·색·폰트가 값보다 자리를 많이 차지하기 때문이다.

그래서 "무엇을 그리는가"만 뽑은 개요 파일(`*.outline.txt`)을 JSON 옆에 같이 둔다.
리뷰는 이 파일의 diff 로 한다.

```bash
# JSON 을 고친 뒤 개요를 다시 만든다 (커밋 전 필수)
node scripts/dashboard-outline.js --write infra/monitoring/grafana/dashboards/*.json

# 개요가 JSON 보다 뒤처지지 않았는지 확인 (뒤처지면 exit 1)
node scripts/dashboard-outline.js --check infra/monitoring/grafana/dashboards/*.json

# 두 판을 직접 견주기 — 화면(라이브)과 파일이 갈라졌는지 볼 때도 쓴다
node scripts/dashboard-outline.js --diff 이전.json 이후.json
```

개요에는 패널 제목·타입·위치, 쿼리(refId·expr·legend), 단위·소수점, 임계값, 설명만 담는다.
즉 **바뀌면 화면이 달라지는 것**만 남기고 나머지는 버린다.

> `provider.yml` 이 `allowUiUpdates: true` 라 Grafana 화면에서도 편집·저장이 된다.
> 그렇게 저장한 내용은 Grafana DB 에만 남고 파일에는 반영되지 않으며, 파일이 다시 바뀌면 덮어써진다.
> **화면에서 고쳤으면 JSON 으로 내보내 파일에 반영할 것.** 갈라졌는지는 이렇게 확인한다.
>
> ```bash
> curl -s -u admin:admin http://localhost:60300/api/dashboards/uid/cdc-tsdb-poc \
>   | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>console.log(JSON.stringify(JSON.parse(s).dashboard)))" > live.json
> node scripts/dashboard-outline.js --diff live.json infra/monitoring/grafana/dashboards/embedded-cdc-tsdb-poc.json
> ```

## 폴더 구조

```
embedded-cdc-tsdb/
├── infra/
│   ├── db/            # source/target PostgreSQL compose (분리) + init SQL
│   └── monitoring/    # Prometheus + Grafana + postgres_exporter, 대시보드 프로비저닝
├── dev/
│   ├── cdc-service/   # Spring Boot + Debezium Embedded (Gradle, 멀티스테이지 Dockerfile)
│   └── docker-compose.app.yml
├── scripts/           # up / demo / load / verify / down (ps1 + sh)
└── (문서는 ../embedded-cdc/docs 하나만 둔다)
```

## 로컬 개발 (컨테이너 없이 앱만)

```powershell
# DB·모니터링은 컨테이너로 띄운 상태에서
cd dev/cdc-service
./gradlew bootRun    # localhost:60432/60433 으로 붙는다 (application.yml 기본값)
```
