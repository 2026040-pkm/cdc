# fabsim — 가공 레거시 리플레이 시뮬레이터 (M1-2.1, IPCS-304)

inshoplocal 사본(2026-08~09 추출분)을 **시각 T 의 원천**으로 되돌려 놓고, RFC Service 의 가공 적재 작업 SQL 을 그대로 돌려 Hot DB `hot.*` 에 쓴다.
결과 행은 RFC Service 가 쓰는 것과 같은 표 · 같은 열 · 같은 키 · 같은 적재 추적 열(`src_job` · `polled_at` · `src_deleted_at`) · 같은 `ops.poll_state` 다.

```
simulator/
├── fabsim.py         prepare · run · samples · clean
├── web.py · web/     브라우저 제어 (시작 · 일시정지 · 한 걸음 · 멈춤 · 정리 · 샘플)
├── sim-tables.yml    원천 표별 "시각 T 의 모습" 규칙 (행이 생기는 시각 · 계획일 · 실적일 · 상태 코드 · 보간)
├── jobs-fab.yml      가공 적재 작업 16개 — cdc poll-jobs-hot.yml 에서 그대로 (SSY 만 PostgreSQL 문법)
├── requirements.txt
└── samples/          IF 별 입력 샘플 · 같은 시각의 hot 출력 샘플 (fabsim.py samples 가 만든다)
```

## 어떻게 도나

```
inshoplocal (사본, 마지막 상태 하나)                         Hot DB
  gp.* · am.*  ──①뜨기──▶  sim._표  ──②시각 T 뷰──▶  sim.표  ──③RFC 작업 SQL──▶  hot.l_* · hot.m_* · ops.poll_state
                (대상 호선만)        (T 이후 실적은 빈 값,       (gp. · am. → sim.,
                                       행은 생긴 뒤에만)         current_date → sim.today())
  시계 T 가 한 걸음(--step)씩 간다 → 걸음마다 폴링 한 주기 → 증분 작업은 워터마크 이상만, full 작업은 전부
```

1. **뜨기**: 대상 호선 행만 `sim._<표>` 로 한 번 복사한다(6065 기준 21표 5초). 호선 열이 없는 표(140M · 820M · STR01H · 214M · 211M)는 먼저 뜬 표에 이어진 행만 뜬다.
2. **시각 T 뷰**: `sim-tables.yml` 규칙대로 T 시점 원천을 흉내 낸다.
   - `appear`: 행이 원천에 생기는 시각 (예: 310M 등록일 · 140M 투입 시각 · 002M 모듬 시각). T 전에는 행이 없다.
   - `actual`: 실적일 (마킹 · 절단 완료 · 선별 · 불출 · 사상). T 를 지나기 전에는 SAP 처럼 `''`.
   - `hide`: 실적에 딸린 열 (시각 열 · 상태 코드). 예: 310M `PRCS_STUS_CODE` 는 착수 전 `''` → 착수 뒤 `'1'` → 완료 뒤 원래 값 `'2'`.
   - `interp`: W/O 누적 진척률은 착수 ~ 완료 사이를 직선 보간한다 (사본에는 마지막 값만 있다).
   - `MNT_DATE · MNT_TIME`: T 까지 보인 가장 늦은 시각으로 바꾼다. 그래서 RFC Service 의 **증분 작업(워터마크 = MNT_DATE‖MNT_TIME)이 고치지 않고 돈다**.
   - 날짜는 `--shift` 일만큼 옮길 수 있다. 7의 배수만 써서 요일을 지킨다 (주말에 실적이 줄어드는 리듬이 그대로 남는다).
3. **적재**: poll-core `PollJob` 과 같은 규칙으로 쓴다 — 원천 별칭 중 대상에 있는 열만 `CAST(? AS 대상형)` UPSERT, incremental 은 `ops.poll_state.watermark` 이상만 읽고(같은 값 다시 읽기 `>=`) 끝난 뒤 워터마크 저장, `mark-missing` full 작업은 이번에 안 온 행에 `src_deleted_at`.
   다른 점은 시각뿐이다 — `polled_at` · `ops.poll_state` 시각은 벽시계가 아니라 시뮬레이션 시각(T + shift)이다. full 작업은 값이 바뀐 행만 다시 쓰고 `polled_at` 은 한 번에 고친다(결과는 같고 빠르다).

## 쓰는 법

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt     # Linux: .venv/bin/pip
python fabsim.py prepare                                   # inshoplocal 에 sim 스키마(함수)
python fabsim.py run --profile cut --step 1d               # 6065 · 2023-09-01 ~ 2024-03-31 을 하루씩
python fabsim.py run --profile cut --step 1h --speed 3600  # 한 시간 = 1초로 (화면 시연)
python fabsim.py run --profile cut --resume                # 멈춘 곳에서 이어서 (Ctrl+C 로 멈춤)
python fabsim.py samples --profile cut --at 2023-11-20 --hot   # IF 별 입력 샘플 + hot 출력 → samples/
python fabsim.py clean                                     # 이 시뮬레이터가 쓴 hot 행 · 워터마크 지우기
```

### 웹으로 제어

```bash
python web.py                 # http://127.0.0.1:8765 — 이 PC 에서만 열린다 (--host 0.0.0.0 으로 열면 같은 망 어디서나)
```

내릴 때는 화면 오른쪽 위 "서버 종료"(또는 Ctrl+C). 리플레이가 도는 중이면 이번 주기를 마치고 위치를 저장한 뒤 내린다.

- 설정(프로필 · 구간 · 한 걸음 · 속도 · 날짜 옮기기)을 고르고 **처음부터 시작 / 이어서 시작**.
- 도는 중: **일시정지 · 계속 · 한 걸음**(일시정지 중 폴링 한 주기만) · **멈춤**, 속도는 도는 중에도 바꾼다.
- 화면: 다음 원천 시각(T) · 마지막 주기의 Hot DB 시각 · 진행률, 주기별 쓴 행 · 삭제 표시 막대, Hot DB 적재 현황(도면 앵커 · W/O · 사건 코드별 · 부재), 작업 상태(`ops.poll_state` 워터마크 · 오류), 로그.
- **준비 · 정리 · 샘플 뽑기**는 리플레이가 멈춰 있을 때만 된다. 샘플 파일은 화면에서 바로 열린다.
- CLI 와 같은 상태 파일(`.fabsim-<프로필>.json`)을 쓰므로, CLI 로 돌리다 멈춘 곳을 웹에서 "이어서 시작" 할 수 있다(반대도 된다). 같은 Hot DB 에 CLI 와 웹을 동시에 돌리지 않는다.

- 접속은 `FABSIM_SRC_DSN`(기본 `localhost:54433/inshoplocal`) · `FABSIM_HOT_DSN`(기본 `localhost:5432/hotdb`, 계정 `rfc_service`)으로 바꾼다.
- `--agent` 기본은 `rfc-sim` 이다. 실제 RFC Service(`rfc-service`)와 워터마크가 섞이지 않는다.
- 워터마크는 agent · 작업마다 하나다(RFC Service 와 같다). 프로필이나 구간을 바꿀 때는 `clean` 먼저 — 남아 있으면 `run` 이 멈추고 알려 준다.
- `--shift today` 는 구간 끝을 오늘 근처로 옮긴다. 화면의 "오늘 ±1개월 절단계획" 같은 필터를 시험할 때 쓴다.

| 프로필 | 호선 · 구간 (원천 시각) | 고른 이유 | 샘플 도면 |
|---|---|---|---|
| `cut` | 6065 · 2023-09-01 ~ 2024-03-31 | 사본에서 가공이 가장 몰린 구간 (절단 완료 월 2~3천, 모듬 월 1~1.7만) | 6065DS447CNE06 — 등록 10-25 → 불출 · 전처리 11-02 → 마킹 · 절단 11-09 → 모듬 11-09 → 사상 11-10~14 |
| `ssy` | 2514 · 2021-07-01 ~ 12-31 | ProSSYS 강재 이력(STY01H)이 있는 호선 — 6065 는 STY01H 에 도면이 없다 | 2514DS662CFG03 — 입고 09-13 → 불출 11-03 → 절단 12-01 |

## FAB-IF 별 적재표 레코드 매핑

| FAB-IF | 원천 표 (사본) | r2 화면 | 적재 작업 (jobs-fab.yml) | Hot DB 자리 | 리플레이에서 시간이 타는 것 |
|---|---|---|---|---|---|
| IF01 ProSSYS 강재 작업 이력 | AM.STR01H | SSY 스케줄작업실적 (크레인) | — (절점은 STY01H 로 판정) | — | END_DATE 에 행이 생긴다 |
| IF14 ROLL · 자재 마스터 | AM.STY01H(이력) · STY02C | SSY 입고 · 선별 · 불출작업실적 | `hot.l_event.ssy` | l_event STEEL_IN · PICK1 · PICK2 · STEEL_ISSUE (source SSY) | CHNG_DATE 에 행, ACPT · PICK · ISS 실적일 |
| IF02 SAP 불출 실적 | YPWC140M · YPWC111M | 전처리 CCA (불출일자 · 시각) | `hot.l_event.140m` · `cary_due` | l_event STEEL_ISSUE · PRETREAT (140M), EVENT CARY_DUE (111M) | 140M ISS_DATE 에 행, AUTO_MARK 뒤 MARK_STUS Y, 111M PICK_FIN · ISS_DT |
| IF04 ProMES 전처리 마킹 | (사본 없음) | CCA · CCB | IF02 의 140M AUTO_MARK 로 대신 | l_event PRETREAT | — |
| IF09 절단 · 마킹 | YPWC310M | 절단 CHC | `l_work_unit.dwg` · `l_event.310m` (+ 하루 1회 resync) | l_work_unit DWG 앵커, l_event MARK_DONE · CUT_DONE (310M) | RGSR 에 행, MARK · CUT_MFG_FD, 상태 '' → 1 → 2 |
| IF10 사상 · 모듬 실적 | YPWC820M · 821M | 사상 DAB · CLC | `hot.l_cut_part` | l_cut_part.grind_done_ts | 820M WORK_DATE, 821M 등록일에 행 |
| IF11 부재 전개 | YDEH090M · YPDG002M | 선별 (부재선별) | `hot.l_cut_part` | l_cut_part.dwg_no · group_ts | 002M 모듬 시각에 행 (090M 은 기준정보) |
| IF12 부재 · 조립품 기준 | YDEH060M · 050M | 자재/부재 현황 | `hot.l_cut_part` | l_cut_part 1행 = 부재 1개 | 060M 절단 실적 열만 |
| IF13 원자재 마스터 | YDEH081M | — | — (DWG attrs.steel_mat 예정) | — | 기준정보 |
| IF03 · 05 · 06 | 미확정 · 사본 없음 | — | — | — | — |
| (W/O) | YPWG411M · 214M · 211M | — | `l_work_unit.wo` · `l_event.wo_ct` (+ resync), DWG 시수 | l_work_unit WO, l_event CUT_DONE (WO_CT) | WO_ACTL_SD · FD, 진척률 보간 |
| (곡 · 불출 · 사외) | YPWC510M · YPDG100M · YCPS301M | — | `hot.l_cut_part` · `l_event.inbound` | bend_done_ts · issue_ts · inbound_ts, EVENT PART_INBOUND | BEND_MFG_FD · ISS_FD · 등록일 |

## 검증 (2026-10-28, 로컬)

- **리플레이 = 한 번에 적재**: `cut` 을 하루 걸음으로 2023-09-01 → 2023-10-31 까지 돌린 Hot DB 와, 같은 시각 T 에서 작업 SQL 을 한 번에 돌린 결과를 키 · 값으로 대조했다. 12개 작업(재동기화 제외) 모두 빠짐 0 · 남음 0 · 값 다름 0.
- **재동기화가 필요한 이유**: resync 작업을 빼면 도면 앵커 6,881 중 518(7.5%)의 시수가 틀린다. 도면 행의 워터마크(310M 수정 시각)가 안 움직였는데 조인 상대(411M W/O)가 나중에 생기기 때문이다 — RFC Service 의 하루 1회 resync(V20 F5)가 이것을 메운다.
- **SSY**: `ssy` 프로필 2021-09-01 → 11-30, `hot.l_event.ssy` 17,521행이 한 번에 적재한 결과와 같다.
- 속도: 6065 하루 걸음 한 번(16개 작업)에 3~15초.

## 한계

- 사본은 마지막 상태 하나라 **중간에 바뀌었다가 되돌아간 값**(재작업 · 취소 · 계획일 변경)은 재현하지 못한다. 계획일은 처음부터 마지막 값으로 보인다.
- 날짜만 있는 실적은 그날 0시에 보인다. 실제 입력 지연(SAP 배치 · 수기 입력)은 넣지 않았다.
- ProMES(IF04 · 06)와 설계 일정(IF05)은 사본에 없다. IF04 는 140M AUTO_MARK 로 대신한다.
- 6065 는 부재 마스터(060M)가 일부만 있어 l_cut_part 가 도면 일부에만 붙는다.
