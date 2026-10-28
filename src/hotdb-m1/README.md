# hotdb-m1 — Hot DB (M1-1.1 ~ 1.4)

통합안·정규화 30표 Hot DB를 compose로 띄운다. 빈 볼륨이면 `docker-entrypoint-initdb.d`가 스키마를 만들고, 명령 하나로 구축이 끝난다.
환경은 **TimescaleDB(tsdb · ts 하이퍼테이블)** 와 **WAL 논리 복제(CDC)** 가 둘 다 되게 맞췄다.

설계 근거는 「내업 · 필드 데이터 계보」 통합안 · 정규화 탭이다(https://claude.ai/artifact/VJADJkWmFYzKiwFqP9mjVQ).
cdc `src/hotdb/db/migration` V1~V20을 적용한 최종 상태를 기준선 한 파일로 모았다.
레거시 원천(SAP · ProSSYS)과 그 사본은 이 산출물 범위 밖이다.

```
hotdb-m1/
├── compose.yml              hotdb 하나 (TimescaleDB · logical WAL)
├── hotdb.sh / hotdb.ps1     up · clean · reset · verify · cdc-check · psql · down
└── hotdb/
    ├── initdb/010_hotdb_baseline.sql   통합안·정규화 30표 (cdc V1~V20 최종 상태, 한 트랜잭션)
    ├── verify.sql                      구축 · CDC 설정 확인
    ├── cdc_check.sql                   CDC 실제 동작 시험
    └── clean.sql                       데이터만 비우기
simulator/                   가공 레거시 리플레이 시뮬레이터 (M1-2.1, IPCS-304) — simulator/README.md
```

## Jira ↔ 통합안 · 정규화

| Jira | Hot DB 자리 |
|---|---|
| M1-1.1 가공 레거시 적재표 | `hot.l_work_unit`(DWG 앵커 · 가공 W/O) · `hot.l_event`(절점 시각) · `hot.l_cut_part`(부재). 원천 → l_* 적재는 RFC Service `poll-jobs-hot.yml` |
| M1-1.2 가공 판별 결과표 | `hot.p_progress` · `p_progress_block` · `p_action`, 절점 사전 `hot.m_code` CHECKPOINT(가공 10 + 사상 보류). Jira 문구의 "fab_result 별도 표"는 p_*로 대체 |
| M1-1.3 가공 기준정보 | `hot.m_hull` · `m_block` · `m_location`(SHOP · ZONE · JIG) · `m_lot` · `m_code` |
| M1-1.4 compose · DDL | 이 폴더 |

## 절차

```bash
./hotdb.sh up         # 기동 + verify. 볼륨이 비어 있으면 initdb 가 스키마를 만든다   (Windows: .\hotdb.ps1 up)
./hotdb.sh reset      # 볼륨 삭제 후 재구축 — 데이터 전부 삭제. 스키마(010_hotdb_baseline.sql)를 바꾼 뒤에도 이것
./hotdb.sh clean      # 데이터만 비움 — 스키마 · 사전 시드(m_code 13그룹 · 신호 사전 · 모듈) 유지
./hotdb.sh verify     # 엔진 · CDC 설정 · 하이퍼테이블 · publication · 계정 · 가공 절점 사전 · 적재 건수
./hotdb.sh cdc-check  # 시험 슬롯으로 tsdb · hot.p_progress 변경이 publication 으로 나오는지 확인
./hotdb.sh psql
./hotdb.sh down
```

- 접속은 `localhost:5432/hotdb`, 관리자는 hotdb/hotdb다 (`.env`의 `HOTDB_PORT` · `HOTDB_ADMIN_USER` · `HOTDB_ADMIN_PASSWORD`).
- 래퍼 없이 `docker compose up -d`만 해도 같은 결과가 나온다.
- 엔진은 docker를 먼저 찾고, 없으면 podman을 쓴다(`HOTDB_ENGINE`로 강제할 수 있다). 포트와 비밀번호는 `.env`에서 바꾼다.
- 기본 권한(`ALTER DEFAULT PRIVILEGES`)은 초기화를 돌린 관리자 기준으로 걸린다. 이후 표도 **관리자 계정으로** 만들어야 판별 모듈 · RFC Service 권한이 자동으로 붙는다. `postgres` 역할은 없다. cdc는 관리자가 postgres라 `FOR ROLE postgres`였는데, 그 부분만 다르다.
- 계정 비밀번호는 개발용으로 계정명과 같다. 서버에서는 `ALTER ROLE … PASSWORD`로 바꾼다.

### initdb 방식

- `010_hotdb_baseline.sql` **파일 하나만** `/docker-entrypoint-initdb.d`에 마운트한다. 폴더째 마운트하면 이미지 자체의 `000_install_timescaledb.sh` · `001_timescaledb_tune.sh`를 덮어쓴다.
- 볼륨이 비어 있을 때 한 번만 돈다. 한 트랜잭션이라 중간에 실패하면 아무것도 남지 않는다.
- 스키마를 바꾸려면 이 파일을 고치고 `reset`한다. M1의 데이터는 시뮬레이터 · 판별로 다시 만들 수 있다.
- healthcheck는 TCP(`pg_isready -h 127.0.0.1`)로 본다. 초기화 중의 임시 서버는 소켓만 열기 때문에, 소켓으로 보면 스키마가 생기기 전에 통과해 버린다.
- 데이터를 남겨야 하는 서버로 가면 증분 마이그레이션이 필요하다. 그때는 cdc처럼 Flyway를 쓴다.

## TimescaleDB · CDC 환경

| 항목 | 값 | 이유 |
|---|---|---|
| 이미지 | timescale/timescaledb:2.29.2-pg17 | `shared_preload_libraries=timescaledb` |
| `wal_level` | logical | 논리 복제 슬롯(pgoutput)으로 CDC |
| `max_wal_senders` · `max_replication_slots` | 16 · 16 | 판별 모듈 4개 + RFC Service 1개 = 슬롯 5개에 여유를 둔 값 |
| `max_slot_wal_keep_size` | 20GB | 멈춘 슬롯이 디스크를 다 채우지 않게 상한을 둔다 (cdc 실측: ×10 부하에서 10GB 초과) |
| `wal_sender_timeout` | 5min | 소비자가 적재 때문에 늦게 응답해도 바로 끊지 않는다 |

| publication | 싣는 표 | 소비자 |
|---|---|---|
| `tsdb_cdc_pub` | `tsdb.device` · `tag_catalog` · `ops.cdc_heartbeat` + **스키마 `_timescaledb_internal` 전체**(tsdb 하이퍼테이블 청크) | 판별 모듈 |
| `hot_cdc_pub` | `hot.p_progress` · `ops.cdc_heartbeat` | RFC Service → SAP |

- 하이퍼테이블 행은 부모 표가 아니라 청크에 들어간다. 그래서 청크가 사는 스키마를 통째로 싣는다. 새 청크도 자동으로 실린다.
- `ts.*` 청크는 `ts_chunks`에 따로 두어 tsdb publication에 섞이지 않게 했다.
- 압축은 7일 뒤에 건다. 압축된 행은 CDC로 나오지 않으므로 소비자가 따라잡을 여유를 두는 것이다.
- 복제 슬롯은 만들지 않는다. 소비자(Debezium)가 처음 붙을 때 자기 슬롯을 만든다.
- REPLICATION 권한이 있는 계정: `svc_mch` · `svc_asm` · `svc_oft` · `svc_pnt` · `rfc_service`.

| 계정 | 하는 일 |
|---|---|
| `rfc_service` | 레거시 원천 → `hot.m_*` · `l_*` 적재, `hot.p_progress` CDC → SAP |
| `svc_mch` · `svc_asm` · `svc_oft` · `svc_pnt` | 판별 모듈 4개 (svc_writer + svc_reader, REPLICATION) |
| `hotdb_provider` | tsdb 필드 원문 |
| `hotdb_monitor` | exporter |

## 검증 (2026-10-28, 로컬 podman)

1. **한 명령 구축**: `.\hotdb.ps1 reset`으로 빈 볼륨에서 initdb를 돌렸다.
   - 엔진: PG 17.11 · TimescaleDB 2.29.2 (preload) · wal_level logical
   - 객체: hot 28(+뷰 1) · ts 2 · tsdb 5 · ops 6 · 하이퍼테이블 5개 · publication 2개
2. **CDC 동작**: 계정 접속, 슬롯 생성, 실제 쓰기를 차례로 확인했다.
   - **복제 프로토콜 접속**: `svc_mch` · `rfc_service`가 TCP 복제 프로토콜(`replication=database`)로 붙어 `IDENTIFY_SYSTEM`과 `CREATE_REPLICATION_SLOT … LOGICAL pgoutput`이 됐다.
   - **쓰기 (`cdc-check`)**: 쓰기 권한도 함께 확인하려고 실제 계정으로 썼다. tsdb는 `hotdb_provider`, `hot.p_progress`는 `svc_mch`다.
   - `hot_cdc_pub` → `p_progress`만 나온다. `l_work_unit`은 실리지 않는데, 의도한 동작이다.
   - `tsdb_cdc_pub` → `_timescaledb_internal._hyper_1_N_chunk`(status_history 청크) · `device` · `tag_catalog`가 나온다.
3. **기준선 = cdc V1~V20**: V1~V20을 차례로 적용한 DB와 기준선 하나로 만든 DB를 비교했다.
   - `pg_dump -s` diff는 기본 권한 줄의 순서만 다르다.
   - 시드, 하이퍼테이블(청크 · 압축 · 보존), publication, 계정 · 멤버십은 같다.
4. **적재 작업 계약 시험**: inshoplocal 추출분을 원천으로 `poll-jobs-hot.yml` 가공 · 마스터 작업 14개를 PollJob 규칙(공유 열 · PK UPSERT)대로 돌렸고, 14개 모두 성공했다.
   - m_hull 4 · m_block 1,830 · m_location 261
   - l_work_unit 251,144 · l_event 167,843 · l_cut_part 80,588
5. **M1-1.3 조인**: 블록 조인율은 W/O 100% · 부재 92.0% · DWG 앵커 **83.3%**다.
   - **원인**: `hot.m_block` 적재가 W/O(YPWG411M)에서만 블록을 모은다. 빠진 169블록은 모두 YDEH050M(조립품)에 있다.
   - **시험**: YDEH050M 블록을 합치면 100%가 된다. 시험만 하고 롤백했다.

## 남은 일

- `poll-jobs-hot.yml` `hot.m_block`에 YDEH050M 블록 합치기 (M1-1.3 DoD 100%).
- ZONE(구역) 행 등록. `m_location`은 지금 SHOP · JIG만 있다.
