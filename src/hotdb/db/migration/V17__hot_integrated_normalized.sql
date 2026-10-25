-- [통합안 · 정규화] Hot DB 를 「내업 · 필드 데이터 계보」 통합안 · 정규화 탭의 30표로 바꾼다 (2026-10-23).
--   근거: https://claude.ai/artifact/VJADJkWmFYzKiwFqP9mjVQ  통합안 · 정규화 탭 (v9 25표 + l_event + 정규화 신규 4표)
--
--   스키마
--     hot   : m_* 마스터 12 · l_* 레거시 참조 9 · s_* 스캔 3 · p_* 실적 · 판별 4  = 28
--     ts    : device_signal · sensor_event 하이퍼테이블 2  (청크는 ts_chunks 에 — tsdb 의 publication 에 섞이지 않게)
--     tsdb  : 그대로 (HotDB Provider 가 쓰는 필드 원문 3채널 — 파란 영역 밖). 판별 모듈이 여기서 hot · ts 로 옮긴다
--     svc   : 걷어낸다 (PoC 모듈 RDB — 시험 데이터뿐). 자리를 hot.s_* · hot.m_device_state · hot.p_* 가 이어받는다
--
--   쓰는 주체
--     판별 모듈(svc_<모듈>, svc_writer) : hot.m_device · m_device_state · s_* · p_* · ts.* 쓰기, 나머지 읽기
--     RFC Service(rfc_service)         : 레거시(SAP · 사본) → hot.m_hull · m_block · m_code · m_lot · l_* 적재, hot.p_progress CDC → SAP
--     Provider(hotdb_provider)         : tsdb 그대로 + ts.* 직접 쓸 수 있게 열어 둔다 (MQTT Agent 가 ts 로 바로 쓰는 날을 위해)
--
--   설계에서 더한 열 (계보 문서에 없는 것 — 구현 사정)
--     hot.m_device.zone               tsdb.device.zone 값. 어느 판별 모듈 장비인지 (m_location 이 비어 있어 location_id 로 못 가른다)
--     hot.m_device_state.src_received_at  Provider 수신 시각 — 반영 지연(ops.module_apply_lag)을 DB 시계로 재는 기준
--     hot.s_scan_session.zone         회차의 권역 — 판별 단계가 자기 모듈 회차만 집는 키
--     hot.s_scan_detection.judge_note 판별이 남긴 한 줄 (상태 · 이유)
--     ops.module.trade                모듈 ↔ 공종 코드(m_code TRADE: C 가공 · S 조립 · W 선행의장 · P 선행도장 = YPWG411M.FLAG)
--     geometry 열(m_location.geom · m_lot.geom)은 PostGIS 가 없어 jsonb(GeoJSON) 로 둔다
--
--   일부러 안 건 FK
--     레거시 적재 표(l_*)의 hull_no · block_no → m_block, l_wo_daily.wo_no → l_work_unit, l_cut_part.dwg_no → l_work_unit:
--     폴링 작업이 서로 독립으로 돌아 적재 순서를 보장할 수 없다. 논리 FK 로 두고 hot.v_l_orphans 뷰로 대조한다.
--     (정규화 점검 B1 — 복합 FK 로 적는다 — 는 뷰의 조인 키로 지켰다)
--
--   순서: 판별 모듈 · RFC Service 를 먼저 내리고 돌린다 (svc 를 지우고 publication 을 바꾼다).

-- ── 0. 옛 모듈 RDB 걷어내기 ──────────────────────────────────────────────
DROP SCHEMA IF EXISTS svc CASCADE;
DROP FUNCTION IF EXISTS ops.module_apply_lag();
DROP TABLE IF EXISTS ops.rfc_sent;
ALTER TABLE ops.module ADD COLUMN IF NOT EXISTS trade text;
UPDATE ops.module SET trade = CASE module WHEN 'mch' THEN 'C' WHEN 'asm' THEN 'S' WHEN 'oft' THEN 'W' WHEN 'pnt' THEN 'P' END;

CREATE SCHEMA hot;
CREATE SCHEMA ts;
CREATE SCHEMA ts_chunks;   -- ts 하이퍼테이블의 청크 — _timescaledb_internal(tsdb publication) 과 분리

GRANT USAGE ON SCHEMA hot, ts, ts_chunks TO svc_writer, svc_reader, rfc_service, hotdb_provider, hotdb_monitor;

-- ── 1. M 마스터 ──────────────────────────────────────────────────────────
CREATE TABLE hot.m_code (
  code_group  text NOT NULL,                     -- TRADE · ANCHOR_TYPE · CHECKPOINT · EVENT · TRADE_RULE · SOURCE_TABLE · SITE · EXCLUDE_REASON · CONF · DEVICE_CLASS · PART_ROLE · PAINT_ELEM · PAINT_STEP · MP · YPGS010M 그룹 …
  code        text NOT NULL,
  name        text,
  in_scope    boolean NOT NULL DEFAULT true,
  seq         integer,
  parent_code text,                              -- 절점 · 사건의 공종 (TRADE 코드)
  attrs       jsonb,                             -- 절점: basis MAT/DWG/PART · phase STOCK/CUT · weight · judge_source. 산정 규칙(TRADE_RULE) · 원천 표(SOURCE_TABLE)
  valid_from  date,
  valid_to    date,
  PRIMARY KEY (code_group, code)
);

CREATE TABLE hot.m_hull (
  hull_no   text PRIMARY KEY,                    -- PROJ_NO (411M 4자리)
  ship_type text,
  status    text,
  in_scope  boolean NOT NULL DEFAULT true
);

CREATE TABLE hot.m_location (
  location_id  text PRIMARY KEY,                 -- SHOP: 공장코드 / ZONE: 공장‖구역 / JIG: 정반코드
  level        text NOT NULL CHECK (level IN ('SHOP','ZONE','JIG')),
  parent_id    text REFERENCES hot.m_location (location_id),
  code         text,
  name         text,
  zone_type    text CHECK (zone_type IS NULL OR zone_type IN ('BAY','SHELTER')),
  geom         jsonb,                            -- GeoJSON 폴리곤 (PostGIS 없음)
  sensing_unit boolean,
  attrs        jsonb,
  valid_from   date,
  valid_to     date
);

CREATE TABLE hot.m_lot (
  lot_code     text PRIMARY KEY,                 -- BTS LANDNO
  middle_lot   text,
  lot_name     text,
  use_type     text,
  process_type text,
  indoor       boolean,
  area_m2      numeric,
  geom         jsonb,
  location_id  text REFERENCES hot.m_location (location_id)   -- 구역 ↔ 지번의 유일한 원본 (B3)
);
CREATE INDEX ON hot.m_lot (location_id);

CREATE TABLE hot.m_block (
  hull_no             text NOT NULL REFERENCES hot.m_hull (hull_no),
  block_no            text NOT NULL,
  block_type          text,
  weight_t            numeric,
  current_trade_stage text,
  bts_lot             text REFERENCES hot.m_lot (lot_code),
  bts_at              timestamptz,
  sensed_location_id  text REFERENCES hot.m_location (location_id),
  sensed_x            numeric,
  sensed_y            numeric,
  sensed_at           timestamptz,
  PRIMARY KEY (hull_no, block_no)
);

-- 장비 개체: 설치 정보만 (D1 · D2). 상태는 m_device_state
CREATE TABLE hot.m_device (
  device_id        text PRIMARY KEY,             -- 설비관리번호 = tsdb.device.device_id
  class_code       text NOT NULL,                -- m_code DEVICE_CLASS
  category         text NOT NULL CHECK (category IN ('SENSOR','ACTUATOR','COMPUTE','UTILITY')),
  parent_device_id text REFERENCES hot.m_device (device_id),
  location_id      text REFERENCES hot.m_location (location_id),
  shop_code        text,                         -- D9: 역정규화 유지
  bay_code         text,
  zone             text,                         -- (더한 열) tsdb.device.zone — 판별 모듈 분담
  x numeric, y numeric, z numeric, heading numeric, tilt numeric,
  attrs            jsonb,
  valid_from       date,
  valid_to         date,
  -- m_code 는 PK 가 (code_group, code) 라 그룹을 고정한 열(class_group)로 복합 FK 를 건다 (B4)
  class_group      text NOT NULL DEFAULT 'DEVICE_CLASS' CHECK (class_group = 'DEVICE_CLASS'),
  FOREIGN KEY (class_group, class_code) REFERENCES hot.m_code (code_group, code) DEFERRABLE INITIALLY DEFERRED,
  CHECK (location_id IS NULL OR shop_code IS NULL OR bay_code IS NULL OR location_id = shop_code || '‖' || bay_code)
);
CREATE INDEX ON hot.m_device (zone);

CREATE TABLE hot.m_device_state (
  device_id       text PRIMARY KEY REFERENCES hot.m_device (device_id) DEFERRABLE INITIALLY DEFERRED,
  last_seen       timestamptz,
  online          boolean,
  fault           boolean,
  fault_code      text,
  last_signals    jsonb,
  src_received_at timestamptz,                   -- (더한 열) Provider 수신 — 반영 지연 측정
  updated_at      timestamptz NOT NULL
);
CREATE INDEX ON hot.m_device_state (updated_at);

CREATE TABLE hot.m_signal_code (
  signal_code text PRIMARY KEY,
  kind        text NOT NULL CHECK (kind IN ('SIGNAL','EVENT')),
  name        text,
  data_type   text,
  unit        text,
  enum_values text[]
);

CREATE TABLE hot.m_signal_def (
  class_group      text NOT NULL DEFAULT 'DEVICE_CLASS' CHECK (class_group = 'DEVICE_CLASS'),
  class_code       text NOT NULL,                -- 공통은 ALL (D4)
  signal_code      text NOT NULL REFERENCES hot.m_signal_code (signal_code),
  sample_period_s  integer,
  adapter          text,
  address_template text,
  normal_min       numeric,
  normal_max       numeric,
  fault_rule_type  text,
  fault_params     jsonb,
  severity         text,
  valid_from       date,
  valid_to         date,
  PRIMARY KEY (class_code, signal_code),
  FOREIGN KEY (class_group, class_code) REFERENCES hot.m_code (code_group, code)
);

CREATE TABLE hot.m_signal_override (
  device_id    text NOT NULL REFERENCES hot.m_device (device_id),
  signal_code  text NOT NULL REFERENCES hot.m_signal_code (signal_code),
  address      text,
  normal_min   numeric,
  normal_max   numeric,
  fault_params jsonb,
  valid_from   date,
  valid_to     date,
  PRIMARY KEY (device_id, signal_code)
);

CREATE TABLE hot.m_file_store (
  store_id       text PRIMARY KEY,
  store_type     text CHECK (store_type IN ('RAW_PCD','ARTIFACT','CAD','IMAGE')),
  base_uri       text,
  path_rule      text,
  retention_days integer,
  owner          text
);

CREATE TABLE hot.m_cad_file (
  cad_file_id   text PRIMARY KEY,
  hull_no       text,
  block_no      text,
  cad_level     text,
  item_id       text,
  store_id      text REFERENCES hot.m_file_store (store_id),
  file_uri      text,
  fmt           text,
  rev_no        text,
  received_at   timestamptz,
  source_system text,
  valid         boolean NOT NULL DEFAULT true
);

-- ── 2. L 레거시 참조 ─────────────────────────────────────────────────────
-- 작업 단위 + 네 공종 앵커 1행. anchor_type 이 있는 행이 앵커다 (v_anchor 대체)
CREATE TABLE hot.l_work_unit (
  unit_id        text PRIMARY KEY,               -- WO: W/O 번호 · ORDER: AUFNR‖VORNR · ACTIVITY: ZZACT‖VORNR(송선) · DWG: 도면번호 · PBLOCK: 호선‖블록‖P
  level          text NOT NULL CHECK (level IN ('ACTIVITY','ORDER','WO','DWG','PBLOCK')),
  parent_id      text,                           -- 상위 단위 (논리 FK — 적재 순서 독립)
  act_no         text,
  hull_no        text,
  block_no       text,
  trade          text,                           -- m_code TRADE (C · S · W · P)
  mp             text,
  stage          text,
  wstg_code      text,
  location_id    text,
  plan_mh        numeric,                        -- 분
  acc_rate       numeric,
  plan_sd        date,
  plan_fd        date,
  actl_sd        date,
  actl_fd        date,
  attrs          jsonb,
  mnt_ts         timestamptz,
  site           text,                           -- m_code SITE (IN · OUT)
  excluded       boolean NOT NULL DEFAULT false,
  exclude_reason text,                           -- m_code EXCLUDE_REASON
  source_table   text,                           -- m_code SOURCE_TABLE
  ref_no         text,                           -- 가공 W/O 가 속한 도면
  step_code      text,                           -- CM · CT · BD · PR · DB · GR
  auto_dist      boolean NOT NULL DEFAULT false,
  anchor_type    text CHECK (anchor_type IS NULL OR anchor_type IN ('DWG','WSTG','GDO','PAINT_ORDER')),
  target_qty     numeric,
  target_basis   text,
  site_group     text NOT NULL DEFAULT 'SITE' CHECK (site_group = 'SITE'),
  excl_group     text NOT NULL DEFAULT 'EXCLUDE_REASON' CHECK (excl_group = 'EXCLUDE_REASON'),
  src_group      text NOT NULL DEFAULT 'SOURCE_TABLE' CHECK (src_group = 'SOURCE_TABLE'),
  FOREIGN KEY (site_group, site) REFERENCES hot.m_code (code_group, code),
  FOREIGN KEY (excl_group, exclude_reason) REFERENCES hot.m_code (code_group, code),
  FOREIGN KEY (src_group, source_table) REFERENCES hot.m_code (code_group, code)
);
CREATE INDEX ON hot.l_work_unit (hull_no, block_no);
CREATE INDEX ON hot.l_work_unit (anchor_type, hull_no, block_no) WHERE anchor_type IS NOT NULL;
CREATE INDEX ON hot.l_work_unit (ref_no) WHERE ref_no IS NOT NULL;
CREATE INDEX ON hot.l_work_unit (parent_id);
CREATE INDEX ON hot.l_work_unit (level, plan_sd);

-- 레거시 사건 1건 = 앵커 × 사건 × 원천 × 시각 (I6: 재발생은 새 행, I7: 그룹 고정 복합 FK)
CREATE TABLE hot.l_event (
  unit_id     text NOT NULL,                     -- 논리 FK → l_work_unit (적재 순서 독립)
  event_group text NOT NULL CHECK (event_group IN ('CHECKPOINT','EVENT')),
  event_code  text NOT NULL,
  source      text NOT NULL,                     -- 310M · WO_CT · 140M · SSY · 111M …
  event_ts    timestamptz NOT NULL,
  conf        text,                              -- m_code CONF
  src_value   text,
  mnt_ts      timestamptz,
  conf_group  text NOT NULL DEFAULT 'CONF' CHECK (conf_group = 'CONF'),
  PRIMARY KEY (unit_id, event_group, event_code, source, event_ts),
  FOREIGN KEY (event_group, event_code) REFERENCES hot.m_code (code_group, code),
  FOREIGN KEY (conf_group, conf) REFERENCES hot.m_code (code_group, code)
);
CREATE INDEX ON hot.l_event (mnt_ts);

CREATE TABLE hot.l_wo_daily (
  wo_no      text NOT NULL,                      -- 논리 FK → l_work_unit WO
  actl_date  date NOT NULL,
  org_code   text NOT NULL,
  daily_rate numeric,
  daily_mh   numeric,
  rgsr_ts    timestamptz,
  PRIMARY KEY (wo_no, actl_date, org_code)
);

CREATE TABLE hot.l_assy_node (
  hull_no        text NOT NULL,
  block_no       text NOT NULL,
  node_id        text NOT NULL,
  node_kind      text CHECK (node_kind IS NULL OR node_kind IN ('ASSY','PART','STD')),
  parent_node_id text,
  next_wstg      text,
  qty            integer,
  weight_kg      numeric,
  cad_file_id    text REFERENCES hot.m_cad_file (cad_file_id),
  plan_sd        date,
  part_clsf      text,
  series         text,
  design_wstg    text,
  prod_wstg      text,                           -- 논리 FK → l_work_unit.unit_id (WSTG 앵커)
  rule_applied   text,
  shape_key      text,
  PRIMARY KEY (hull_no, block_no, node_id)
);
CREATE INDEX ON hot.l_assy_node (prod_wstg);

CREATE TABLE hot.l_cut_part (
  part_no       text PRIMARY KEY,
  hull_no       text,
  block_no      text,
  dwg_no        text,                            -- 논리 FK → l_work_unit DWG
  dest_wstg     text,
  bend_flag     boolean,
  cut_done_ts   timestamptz,
  group_ts      timestamptz,                     -- 절점 9
  sort_ts       timestamptz,                     -- 보류 (355M 운영 추출 뒤)
  issue_ts      timestamptz,                     -- 절점 10
  inbound_ts    timestamptz,                     -- 사외 절단 부재 반입
  cut_site      text,
  weight_kg     numeric,
  cut_req_qty   integer,
  bend_done_ts  timestamptz,                     -- 절점 8
  grind_flag    boolean,                         -- 보류
  grind_done_ts timestamptz,                     -- 보류
  issue_src     text,                            -- '100M' · 'ASSY_WO'
  mnt_ts        timestamptz
);
CREATE INDEX ON hot.l_cut_part (dwg_no);
CREATE INDEX ON hot.l_cut_part (hull_no, block_no);

CREATE TABLE hot.l_outfit_item (
  rsnum      text NOT NULL,
  rspos      text NOT NULL,
  hull_no    text,
  block_no   text,
  matnr      text,
  class_code text,
  do_no      text,
  req_qty    numeric,
  inst_qty   numeric,
  inst_date  date,
  issue_ts   timestamptz,
  cad_file_id text REFERENCES hot.m_cad_file (cad_file_id),
  gdo_no     text,                               -- 논리 FK → l_work_unit (GDO = ORDER 앵커)
  part_role  text,                               -- m_code PART_ROLE
  temp_qty   numeric,
  fixed_qty  numeric,
  ready_date date,
  mnt_ts     timestamptz,
  PRIMARY KEY (rsnum, rspos)
);
CREATE INDEX ON hot.l_outfit_item (gdo_no);

CREATE TABLE hot.l_paint_element (
  anchor_id     text NOT NULL,                   -- 논리 FK → l_work_unit PBLOCK
  kind          text NOT NULL,                   -- C 전처리 · P 도장
  zone_code     text NOT NULL,
  elem_code     text NOT NULL,                   -- m_code PAINT_ELEM
  seq           text NOT NULL,
  wo_no         text,
  done_date     date,
  std_mh        numeric,
  insp_req_clsf text,
  mnt_ts        timestamptz,
  daily_cmpl    text,
  daily_ord_mh  numeric,
  wo_mh_split   numeric,
  step_code     text,
  actl_sd       date,
  cnfm_date     date,
  area_m2       numeric,
  PRIMARY KEY (anchor_id, kind, zone_code, elem_code, seq)
);

CREATE TABLE hot.l_bts_move (
  hull_no   text NOT NULL,
  block_no  text NOT NULL,
  move_ts   timestamptz NOT NULL,
  from_lot  text,
  to_lot    text,
  move_type text,
  PRIMARY KEY (hull_no, block_no, move_ts)
);

CREATE TABLE hot.l_daily_plan (
  plan_date     date NOT NULL,
  trade         text NOT NULL,
  anchor_type   text NOT NULL,
  anchor_id     text NOT NULL,                   -- 논리 FK → l_work_unit 앵커 행
  hull_no       text,
  block_no      text,
  location_id   text,
  org_code      text,
  priority      integer,
  source_system text,
  loaded_at     timestamptz,
  PRIMARY KEY (plan_date, trade, anchor_type, anchor_id)
);

-- 논리 FK 대조 뷰 — 적재가 끝난 뒤 고아 행을 본다 (B1: 복합 키로 조인)
CREATE VIEW hot.v_l_orphans AS
  SELECT 'l_work_unit.block' AS ref, unit_id AS key FROM hot.l_work_unit u
   WHERE block_no IS NOT NULL AND NOT EXISTS (SELECT 1 FROM hot.m_block b WHERE b.hull_no = u.hull_no AND b.block_no = u.block_no)
  UNION ALL
  SELECT 'l_event.unit', unit_id FROM hot.l_event e
   WHERE NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = e.unit_id)
  UNION ALL
  SELECT 'l_cut_part.dwg', part_no FROM hot.l_cut_part p
   WHERE dwg_no IS NOT NULL AND NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = p.dwg_no AND u.level = 'DWG')
  UNION ALL
  SELECT 'l_wo_daily.wo', wo_no FROM hot.l_wo_daily d
   WHERE NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = d.wo_no);

-- ── 3. ts 시계열 ─────────────────────────────────────────────────────────
CREATE TABLE ts.device_signal (
  time        timestamptz NOT NULL,
  device_id   text        NOT NULL,              -- m_device (하이퍼테이블이라 FK 는 두지 않는다)
  signal_code text        NOT NULL,              -- m_signal_code
  kind        text        NOT NULL CHECK (kind IN ('SIGNAL','EVENT')),
  value_num   numeric,
  value_text  text,
  quality     text,
  PRIMARY KEY (time, device_id, signal_code)
);
SELECT create_hypertable('ts.device_signal', 'time', chunk_time_interval => INTERVAL '7 days',
                         associated_schema_name => 'ts_chunks');

CREATE TABLE ts.sensor_event (
  time            timestamptz NOT NULL,
  event_id        text        NOT NULL,
  scan_id         text,
  object_key      text,
  zone            text,
  stage           text,
  location_id     text,
  hull_no         text,
  block_no        text,
  object_type     text,
  event_type      text,
  source_system   text,
  idempotency_key text,
  raw_payload     jsonb,
  PRIMARY KEY (time, event_id)
);
SELECT create_hypertable('ts.sensor_event', 'time', chunk_time_interval => INTERVAL '7 days',
                         associated_schema_name => 'ts_chunks');
CREATE INDEX ON ts.sensor_event (scan_id, time DESC);
SELECT add_retention_policy('ts.device_signal', drop_after => INTERVAL '90 days');
SELECT add_retention_policy('ts.sensor_event',  drop_after => INTERVAL '90 days');

-- ── 4. S 스캔 ────────────────────────────────────────────────────────────
CREATE TABLE hot.s_scan_session (
  scan_id           text PRIMARY KEY,
  zone              text,                        -- (더한 열) 판별 모듈 분담
  location_id       text REFERENCES hot.m_location (location_id),
  scan_ts           timestamptz,
  raw_store_id      text REFERENCES hot.m_file_store (store_id),
  raw_dir           text,
  artifact_store_id text REFERENCES hot.m_file_store (store_id),
  artifact_dir      text,
  artifacts         jsonb,
  model_ver         text,
  first_msg_ts      timestamptz,
  last_msg_ts       timestamptz,
  detection_cnt     integer,
  expected_cnt      integer,
  status            text CHECK (status IS NULL OR status IN ('OPEN','CLOSED','FAILED'))
);
CREATE INDEX ON hot.s_scan_session (zone, last_msg_ts);

CREATE TABLE hot.s_scan_device (
  scan_id   text NOT NULL REFERENCES hot.s_scan_session (scan_id) DEFERRABLE INITIALLY DEFERRED,
  device_id text NOT NULL REFERENCES hot.m_device (device_id) DEFERRABLE INITIALLY DEFERRED,
  role      text,
  PRIMARY KEY (scan_id, device_id)
);

CREATE TABLE hot.s_scan_detection (
  scan_id      text NOT NULL REFERENCES hot.s_scan_session (scan_id) DEFERRABLE INITIALLY DEFERRED,
  object_key   text NOT NULL,
  event_id     text,                             -- ts.sensor_event 1:1 (하이퍼테이블이라 FK 없음)
  received_ts  timestamptz,
  object_type  text,
  hull_no      text,
  block_no     text,
  item_id      text,
  anchor_type  text,
  anchor_id    text REFERENCES hot.l_work_unit (unit_id),   -- 판별이 채운다 (P2)
  cad_file_id  text REFERENCES hot.m_cad_file (cad_file_id),
  match_conf   numeric,
  pose         jsonb,
  bbox         jsonb,
  progress_est numeric,
  count_est    integer,
  presence     boolean,
  stage_hint   text,
  event_type   text,
  segment_uri  text,
  model_ver    text,
  ocr_text     text,
  judged       boolean NOT NULL DEFAULT false,
  judged_ts    timestamptz,
  judge_note   text,                             -- (더한 열)
  PRIMARY KEY (scan_id, object_key)
);
CREATE INDEX s_scan_detection_pending ON hot.s_scan_detection (received_ts) WHERE NOT judged;
CREATE INDEX ON hot.s_scan_detection (hull_no, block_no);

-- ── 5. P 실적 · 판별 ─────────────────────────────────────────────────────
CREATE TABLE hot.p_action (
  action_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  action_type text NOT NULL CHECK (action_type IN ('ALERT','KEYIN')),
  hull_no     text,
  block_no    text,
  anchor_id   text REFERENCES hot.l_work_unit (unit_id),
  sub_type    text,                              -- UNMATCHED · ORDER_REVERSED · GAP · LOW_CONFIDENCE · CARRY_OVERDUE · NO_PLAN …
  detail      jsonb,
  value_pct   numeric,
  cp_code     text,
  reason_code text,
  user_id     text,
  raised_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
  status      text NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','RESOLVED','DISMISSED')),
  resolved_by text,
  resolved_at timestamptz
);
CREATE INDEX ON hot.p_action (hull_no, block_no, status);

CREATE TABLE hot.p_progress (
  anchor_id       text NOT NULL REFERENCES hot.l_work_unit (unit_id),
  as_of           date NOT NULL,
  hull_no         text,
  block_no        text,
  anchor_type     text,
  state           text NOT NULL CHECK (state IN ('notDue','inProgress','done','confirmed','held')),
  auto_pct        numeric,
  legacy_pct      numeric,
  gap_pct         numeric,
  planned_qty     numeric,
  done_qty        numeric,
  last_cp         text,
  cp_history      jsonb,
  evidence        jsonb,
  location_id     text REFERENCES hot.m_location (location_id),
  first_detect_ts timestamptz,
  last_detect_ts  timestamptz,
  confirmed_by    bigint REFERENCES hot.p_action (action_id),   -- P5: 형 맞춤
  weight_mh       numeric,
  site            text,
  excluded        boolean NOT NULL DEFAULT false,
  exclude_reason  text,
  gap_alert       boolean NOT NULL DEFAULT false,
  match_state     text,
  match_pool      text,
  match_wo_no     text,
  match_lead      text,
  cp_plan         jsonb,
  late_cp_cnt     integer,
  cp_stats        jsonb,
  judged_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (anchor_id, as_of)                 -- P1: 호선 · 블록은 PK 에서 뺀다
);
CREATE INDEX ON hot.p_progress (hull_no, block_no, as_of);
CREATE INDEX ON hot.p_progress (as_of, anchor_type);

CREATE TABLE hot.p_progress_block (
  hull_no            text NOT NULL,
  block_no           text NOT NULL,
  trade              text NOT NULL,              -- m_code TRADE
  stage              text NOT NULL,
  as_of              date NOT NULL,
  auto_pct           numeric,
  legacy_pct         numeric,
  weight_basis       text,
  anchor_cnt         integer,
  open_action_cnt    integer,
  bts_lot            text REFERENCES hot.m_lot (lot_code),
  sensed_location_id text REFERENCES hot.m_location (location_id),
  wip_state          text CHECK (wip_state IS NULL OR wip_state IN ('JUST_IN','WORKING','DONE','WAITING','NOT_WIP')),
  first_detect_ts    timestamptz,
  last_detect_ts     timestamptz,
  updated_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (hull_no, block_no, trade, stage, as_of)
);

CREATE TABLE hot.p_item_state (
  hull_no       text NOT NULL,
  block_no      text NOT NULL,
  item_type     text NOT NULL CHECK (item_type IN ('ASSY','PART','OUTFIT_ITEM','ROLL')),
  item_id       text NOT NULL,
  anchor_id     text REFERENCES hot.l_work_unit (unit_id),
  state         text,
  detect_ts     timestamptz,
  legacy_ts     timestamptz,
  scan_id       text,
  object_key    text,
  work_stage    text,
  sub_total     integer,
  sub_judged    integer,
  issue_path    text,
  mirror_stage  text,
  sync_state    text,
  state_history jsonb,
  PRIMARY KEY (hull_no, block_no, item_type, item_id),
  FOREIGN KEY (scan_id, object_key) REFERENCES hot.s_scan_detection (scan_id, object_key)   -- P4: 복합 FK
);

-- ── 6. ops — Hot DB 쪽 폴링 상태 · SAP 송신 기록 · 반영 지연 ───────────────
-- RFC Service 가 레거시 → hot.l_* 적재 작업의 워터마크를 여기 남긴다 (레거시 DB 의 ops.poll_state 와 같은 모양)
CREATE TABLE ops.poll_state (
  agent       text NOT NULL,
  job         text NOT NULL,
  watermark   text,
  last_run_at timestamptz,
  last_ok_at  timestamptz,
  last_rows   bigint,
  last_error  text,
  PRIMARY KEY (agent, job)
);

-- p_progress 한 행(앵커 × 기준일 × 상태)당 SAP 로 한 번
CREATE TABLE ops.rfc_progress_sent (
  anchor_id     text        NOT NULL,
  as_of         date        NOT NULL,
  state         text        NOT NULL,
  src_judged_at timestamptz NOT NULL,            -- 판별 시각 (DB 시계) — sent_at 과 빼면 2단 지연
  src_lsn       bigint,
  sent_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (anchor_id, as_of, state)
);
CREATE INDEX ON ops.rfc_progress_sent (sent_at);

-- 모듈별 반영 지연 (DB 시계) — m_device_state 로 잰다. 등록된 모듈은 데이터가 없어도 한 줄
CREATE FUNCTION ops.module_apply_lag()
RETURNS TABLE (zone text, rows_1m bigint, p50_seconds double precision, p99_seconds double precision,
               max_seconds double precision)
LANGUAGE sql STABLE AS $fn$
  SELECT m.module, count(l.lag),
         coalesce(percentile_cont(0.5)  WITHIN GROUP (ORDER BY l.lag), 0),
         coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY l.lag), 0),
         coalesce(max(l.lag), 0)
    FROM ops.module m
    LEFT JOIN LATERAL (
      SELECT extract(epoch FROM s.updated_at - s.src_received_at)::float8 AS lag
        FROM hot.m_device_state s JOIN hot.m_device d ON d.device_id = s.device_id
       WHERE d.zone = m.zone_code AND s.updated_at > now() - interval '1 minute') l ON true
   GROUP BY m.module
$fn$;

-- 모듈 걷어내기 — V11 판은 svc 표를 지웠다. hot 은 모듈 열이 없으므로 그 모듈 권역 장비의 상태 · 등록부 · 슬롯 · 체크포인트만 걷는다
CREATE OR REPLACE FUNCTION ops.drop_module(p_module text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE r text := 'svc_' || p_module;
BEGIN
  PERFORM pg_drop_replication_slot(slot_name) FROM pg_replication_slots
   WHERE slot_name = 'zone_' || p_module AND NOT active;
  DELETE FROM ops.cdc_checkpoint WHERE pipeline = 'zone-' || p_module;
  DELETE FROM hot.m_device_state s USING hot.m_device d, ops.module m
   WHERE s.device_id = d.device_id AND d.zone = m.zone_code AND m.module = p_module;
  DELETE FROM ops.module WHERE module = p_module;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
    EXECUTE format('DROP OWNED BY %I', r);
    EXECUTE format('DROP ROLE %I', r);
  END IF;
END
$fn$;

-- ── 7. 권한 ──────────────────────────────────────────────────────────────
-- 읽기: 모두
GRANT SELECT ON ALL TABLES IN SCHEMA hot, ts TO svc_reader, rfc_service, hotdb_monitor, hotdb_provider;
-- 판별 모듈: 장비 · 상태 · 스캔 · 실적 · 시계열
GRANT INSERT, UPDATE, DELETE ON hot.m_device, hot.m_device_state, hot.s_scan_session, hot.s_scan_device, hot.s_scan_detection,
      hot.p_progress, hot.p_progress_block, hot.p_item_state, hot.p_action, ts.device_signal, ts.sensor_event TO svc_writer;
GRANT USAGE ON SEQUENCE hot.p_action_action_id_seq TO svc_writer;
-- RFC Service: 레거시 적재 + 폴링 상태 + 송신 기록 (p_progress 는 CDC 로 읽기만)
GRANT INSERT, UPDATE, DELETE ON hot.m_hull, hot.m_block, hot.m_code, hot.m_location, hot.m_lot, hot.l_work_unit, hot.l_event,
      hot.l_wo_daily, hot.l_assy_node, hot.l_cut_part, hot.l_outfit_item, hot.l_paint_element, hot.l_bts_move, hot.l_daily_plan
      TO rfc_service;
GRANT SELECT, INSERT, UPDATE ON ops.poll_state TO rfc_service;
GRANT SELECT, INSERT ON ops.rfc_progress_sent TO rfc_service;
GRANT SELECT ON ops.poll_state, ops.rfc_progress_sent TO hotdb_monitor;
-- Provider: ts 직접 적재를 열어 둔다
GRANT INSERT ON ts.device_signal, ts.sensor_event TO hotdb_provider;
-- 청크에는 하이퍼테이블 권한이 복사된다. 청크 스키마의 기본 권한도 맞춘다
ALTER DEFAULT PRIVILEGES IN SCHEMA ts_chunks GRANT SELECT ON TABLES TO svc_reader, rfc_service, hotdb_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA ts_chunks GRANT INSERT ON TABLES TO svc_writer, hotdb_provider;

-- ── 8. publication — SAP 로 가는 것은 실적(p_progress)뿐 ──────────────────
ALTER PUBLICATION svc_cdc_pub RENAME TO hot_cdc_pub;
ALTER PUBLICATION hot_cdc_pub SET TABLE ops.cdc_heartbeat, hot.p_progress;

-- ── 9. 사전 (m_code) — 레거시가 아니라 설계 · 회의 결정에서 온다 ──────────
INSERT INTO hot.m_code (code_group, code, name, seq) VALUES
  ('TRADE', 'C', '가공', 1), ('TRADE', 'S', '조립', 2), ('TRADE', 'W', '선행의장', 3), ('TRADE', 'P', '선행도장', 4),
  ('ANCHOR_TYPE', 'DWG', '절단 도면', 1), ('ANCHOR_TYPE', 'WSTG', '조립 송선', 2),
  ('ANCHOR_TYPE', 'GDO', '의장 GDO', 3), ('ANCHOR_TYPE', 'PAINT_ORDER', '도장 블록', 4),
  ('SITE', 'IN', '사내', 1), ('SITE', 'OUT', '사외', 2),
  ('CONF', 'HIGH', '높음', 1), ('CONF', 'MID', '중간', 2), ('CONF', 'LOW', '낮음 — 완료 사실만', 3),
  ('EXCLUDE_REASON', 'DWG_D', '도면 상태 D', 1), ('EXCLUDE_REASON', 'CAS_PAS', 'CAS · PAS 조립 W/O (조립으로 본다)', 2),
  ('EXCLUDE_REASON', 'TBAR', 'T-BAR 계열 (T · X · K · Y)', 3), ('EXCLUDE_REASON', 'SSYW', '선판계 SSYW', 4),
  ('EXCLUDE_REASON', 'AUTO_DIST', '자동분배 W/O (413 대상 아님)', 5),
  ('SOURCE_TABLE', 'YPWG411M', '작업오더 마스터', 1), ('SOURCE_TABLE', 'YPWC310M', '도면별 절단 · 마킹', 2),
  ('SOURCE_TABLE', 'ZPPT200', '오더 공정 속성 (의장)', 3), ('SOURCE_TABLE', 'YDEH060M', '부재 마스터', 4),
  ('SOURCE_TABLE', 'YPWP720M', '도장 미세공정 요소', 5), ('SOURCE_TABLE', 'ZCOT001', '공사 기준정보', 6),
  ('SOURCE_TABLE', 'ZPST0630', '외주 · 정산 네트워크 (액티비티)', 7),
  ('DEVICE_CLASS', 'ALL', '공통', 0), ('DEVICE_CLASS', 'LIDAR', '라이다', 1), ('DEVICE_CLASS', 'PANTILT', '팬틸트', 2),
  ('DEVICE_CLASS', 'EDGE_PC', '엣지 PC', 3), ('DEVICE_CLASS', 'UTILITY', '제습기 · 판넬', 4),
  ('PART_ROLE', 'SUPPORT', '지지류', 1), ('PART_ROLE', 'BODY', '본체', 2),
  ('PAINT_STEP', 'SP', 'S/P', 1), ('PAINT_STEP', 'TUP', 'T/UP', 2), ('PAINT_STEP', 'FINAL', 'FINAL', 3);

-- 가공 절점 10개 (+ 사상 보류). attrs.basis = 분모 단위, phase = 구간, weight = 가중, judge_source = 시각 열
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs) VALUES
  ('CHECKPOINT', 'STEEL_IN',    '강재 입고',       true,  1, 'C', '{"basis":"MAT","phase":"STOCK","weight":"WEIGHT","judge_source":"l_event:STEEL_IN","note":"세 층 — 확보 STEEL_SECURED · 반입예정 경과 CARY_DUE · 도착 STEEL_IN. 도착은 운영 ProSSYS 재추출 뒤"}'),
  ('CHECKPOINT', 'PICK1',       '1차 선별',        true,  2, 'C', '{"basis":"MAT","phase":"STOCK","weight":"WEIGHT","judge_source":"l_event:PICK1","skip_if":"EMERGENCY"}'),
  ('CHECKPOINT', 'PICK2',       '2차 선별',        true,  3, 'C', '{"basis":"MAT","phase":"STOCK","weight":"WEIGHT","judge_source":"l_event:PICK2","skip_if":"EMERGENCY"}'),
  ('CHECKPOINT', 'STEEL_ISSUE', '강재 불출',       true,  4, 'C', '{"basis":"MAT","phase":"STOCK","weight":"WEIGHT","judge_source":"l_event:STEEL_ISSUE","note":"= 전처리 라인 투입 = 가공 입고. K73 재입고분 차감"}'),
  ('CHECKPOINT', 'PRETREAT',    '전처리',          true,  5, 'C', '{"basis":"DWG","phase":"CUT","weight":"MH","judge_source":"l_event:PRETREAT","note":"도면의 강판 전부 Y"}'),
  ('CHECKPOINT', 'MARK_DONE',   '마킹 완료 (절단 착수)', true, 6, 'C', '{"basis":"DWG","phase":"CUT","weight":"MH","judge_source":"l_event:MARK_DONE"}'),
  ('CHECKPOINT', 'CUT_DONE',    '절단 완료',       true,  7, 'C', '{"basis":"DWG","phase":"CUT","weight":"MH","judge_source":"l_event:CUT_DONE","done":true,"note":"사내 WO_CT · 사외 310M 두 원천, 합치지 않는다"}'),
  ('CHECKPOINT', 'BEND_DONE',   '곡가공',          true,  8, 'C', '{"basis":"PART","phase":"CUT","weight":"COUNT","judge_source":"l_cut_part:bend_done_ts","target":"bend_flag","note":"벤딩 대상 중 완료 — 화면은 보조 줄"}'),
  ('CHECKPOINT', 'GRIND',       '사상',            false, 85, 'C', '{"basis":"PART","phase":"CUT","weight":"COUNT","judge_source":"l_cut_part:grind_done_ts","target":"grind_flag","note":"보류 — 모듬판 단위 일괄, 넣는다면 키는 모듬번호"}'),
  ('CHECKPOINT', 'GROUP',       '모둠 선별',       true,  9, 'C', '{"basis":"PART","phase":"CUT","weight":"COUNT","judge_source":"l_cut_part:group_ts","note":"송선 기준 · 분모 = 모듬 대상 부재"}'),
  ('CHECKPOINT', 'PART_ISSUE',  '최종(부재) 불출', true, 10, 'C', '{"basis":"PART","phase":"CUT","weight":"COUNT","judge_source":"l_cut_part:issue_ts","note":"대체: 송선 조립 W/O 착수일"}'),
  -- 조립 후보 5 · 의장 후보 5 · 도장 2 (절점 날짜는 l_event 에 쌓이고, 판별은 아직 센서 쪽)
  ('CHECKPOINT', 'ASSY_ISSUED',   '발행 (411 등록일)', true, 1, 'S', '{"basis":"WSTG","judge_source":"l_event:ASSY_ISSUED"}'),
  ('CHECKPOINT', 'ASSY_STARTED',  '착수',          true, 2, 'S', '{"basis":"WSTG","judge_source":"l_work_unit:actl_sd"}'),
  ('CHECKPOINT', 'ASSY_DONE',     '완료',          true, 3, 'S', '{"basis":"WSTG","judge_source":"l_work_unit:actl_fd","done":true}'),
  ('CHECKPOINT', 'ASSY_INSPECTED','검사 (YPMA100M)', true, 4, 'S', '{"basis":"WSTG","judge_source":"l_event:ASSY_INSPECTED"}'),
  ('CHECKPOINT', 'ASSY_OUT',      '반출 (검사장)',  true, 5, 'S', '{"basis":"WSTG","judge_source":"l_event:ASSY_OUT"}'),
  ('CHECKPOINT', 'OFT_IN',        '반입 (P00/W00)', true, 1, 'W', '{"basis":"GDO"}'),
  ('CHECKPOINT', 'OFT_SUPPORT',   '지지류',        true, 2, 'W', '{"basis":"GDO"}'),
  ('CHECKPOINT', 'OFT_BODY',      '본체',          true, 3, 'W', '{"basis":"GDO"}'),
  ('CHECKPOINT', 'OFT_FULL',      '100%',          true, 4, 'W', '{"basis":"GDO","done":true}'),
  ('CHECKPOINT', 'OFT_OUT',       '반출',          true, 5, 'W', '{"basis":"GDO"}'),
  ('CHECKPOINT', 'PNT_IN',        'BTS 반입',      true, 1, 'P', '{"basis":"PAINT_ORDER"}'),
  ('CHECKPOINT', 'PNT_OUT',       'BTS 반출',      true, 2, 'P', '{"basis":"PAINT_ORDER","done":true}');

-- 절점이 아닌 사건 (EVENT 그룹)
INSERT INTO hot.m_code (code_group, code, name, seq, parent_code, attrs) VALUES
  ('EVENT', 'STEEL_SECURED', '강재 확보 (입고 1층)', 1, 'C', '{"source":"ZPPT005_1.AEFLG","denominator":"YDEH081M"}'),
  ('EVENT', 'CARY_DUE',      '반입예정 경과 (입고 2층)', 2, 'C', '{"source":"YPWC111M.CARY_DUE_DATE","alert":"CARRY_OVERDUE"}'),
  ('EVENT', 'STEEL_REISSUE', '겹장 재입고 · 재불출 (K73 · R)', 3, 'C', '{"adjust":"STEEL_ISSUE"}'),
  ('EVENT', 'DWG_CONFIRMED', '도면 확정', 4, 'C', '{"source":"ZPPT005.CRAT_IND"}'),
  ('EVENT', 'PART_INBOUND',  '사외 절단 부재 반입 (YCPS301M)', 5, 'C', '{"source":"YCPS301M"}');

-- 공종별 산정 규칙 (TRADE_RULE) — 판별 단계가 읽는다
INSERT INTO hot.m_code (code_group, code, name, attrs) VALUES
  ('TRADE_RULE', 'C', '가공 — 절점 도달이 곧 실적. 도면 완료율 시수 가중',
   '{"pct_basis":"DWG_MH","state_rule":"CP_REACHED","done_cp":"CUT_DONE","weight_col":"plan_mh","legacy":"POST","gap_rule":"NONE","back_judge":false,"list_window_days":30,"site_out_skips":["STEEL_IN","PICK1","PICK2","STEEL_ISSUE"]}'),
  ('TRADE_RULE', 'S', '조립 — 인식 개수 ÷ 목표 개수. 보조 Σ413 ÷ ΣSTD_MH',
   '{"pct_basis":"COUNT","state_rule":"DETECT","legacy":"STEP","gap_rule":"RECORD_ONLY_THEN_ALERT_3D","held_if_no_wo":true,"min_conf":0.9}'),
  ('TRADE_RULE', 'W', '선행의장 — 설치 품목 ÷ 소요 품목',
   '{"pct_basis":"ITEMS","state_rule":"ITEMS_ALL","legacy":"SETTLEMENT","gap_rule":"ALERT_10","min_conf":0.9}'),
  ('TRADE_RULE', 'P', '선행도장 — Σ413 ÷ Σ212 (공정당 1회)',
   '{"pct_basis":"MH_212","state_rule":"CONTINUOUS","legacy":"CONTINUOUS","gap_rule":"NONE","min_conf":0.9}');

-- 신호 사전 — 태그 status 채널의 필드를 그대로 코드로 둔다
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit) VALUES
  ('status',       'SIGNAL', '장비 상태',      'enum', NULL),
  ('heartbeat',    'SIGNAL', '하트비트',       'text', NULL),
  ('scan_rate',    'SIGNAL', '스캔 속도',      'num',  'pts/s'),
  ('temperature',  'SIGNAL', '온도',           'num',  '°C'),
  ('rssi',         'SIGNAL', '연결 신호 세기', 'num',  'dBm'),
  ('fov_mode',     'SIGNAL', '시야각 모드',    'text', NULL),
  ('error_code',   'SIGNAL', '오류 코드',      'text', NULL),
  ('ONLINE',       'EVENT',  '온라인 전환',    NULL,   NULL),
  ('OFFLINE',      'EVENT',  '오프라인 전환',  NULL,   NULL),
  ('FAULT_ON',     'EVENT',  '고장 발생',      NULL,   NULL),
  ('FAULT_OFF',    'EVENT',  '고장 해제',      NULL,   NULL);
INSERT INTO hot.m_signal_def (class_code, signal_code, sample_period_s, adapter, address_template, fault_rule_type, fault_params, severity) VALUES
  ('ALL',   'heartbeat',   1,  'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', 'HEARTBEAT_TIMEOUT', '{"timeout_s":30}', 'HIGH'),
  ('LIDAR', 'status',      1,  'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', 'ENUM', '{"fault_values":["ERROR"]}', 'HIGH'),
  ('LIDAR', 'scan_rate',   1,  'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', 'RANGE', '{"min":100000}', 'MID'),
  ('LIDAR', 'temperature', 1,  'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', 'RANGE', '{"max":70}', 'MID'),
  ('LIDAR', 'rssi',        1,  'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', 'RANGE', '{"min":-85}', 'LOW');
