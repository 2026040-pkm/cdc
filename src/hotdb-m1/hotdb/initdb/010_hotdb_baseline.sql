-- =============================================================================
-- Hot DB 기준선 — 통합안 · 정규화 30표 (M1-1.4, IPCS-303)
--
-- cdc 저장소 src/hotdb/db/migration V1 ~ V20 을 차례로 적용한 최종 상태를 한 파일로 모았다.
-- 빈 DB 에서 이 파일 하나로 같은 모양이 된다 (2026-10-28 생성, 원본과 스키마 diff 0 확인).
--
-- 적용: 컨테이너 /docker-entrypoint-initdb.d 에서 볼륨이 비어 있을 때 한 번 돈다 (이미지의 000_ · 001_ 스크립트 뒤).
-- 스키마를 바꾸면 이 파일을 고치고 reset(볼륨 삭제 후 재기동)한다 — 기존 볼륨에는 다시 돌지 않는다.
-- 한 트랜잭션이라 중간에 실패하면 아무것도 남지 않는다.
-- 설계 근거: 「내업 · 필드 데이터 계보」 통합안 · 정규화 탭  https://claude.ai/artifact/VJADJkWmFYzKiwFqP9mjVQ
--
--   hot   m_* 마스터 12 · l_* 레거시 참조 9 · s_* 스캔 3 · p_* 실적 · 판별 4  (+ v_l_orphans)
--   ts    device_signal · sensor_event 하이퍼테이블 (청크는 ts_chunks)
--   tsdb  HotDB Provider 필드 원문 (device · tag_catalog · *_history 하이퍼테이블 3)
--   ops   CDC 운영 · 폴링 상태 · SAP 송신 기록 · 모듈
--
-- M1 가공에서 쓰는 곳
--   1.1 레거시 적재  : hot.l_work_unit (DWG 앵커 · 가공 W/O) · hot.l_event (절점 시각) · hot.l_cut_part (부재)
--                     RFC Service poll-jobs-hot.yml 이 레거시 원천 → l_* 로 적재
--   1.2 판별 결과    : hot.p_progress · p_progress_block · p_action, 절점 사전 hot.m_code CHECKPOINT (가공 10)
--   1.3 기준정보     : hot.m_hull · m_block · m_location(SHOP · ZONE) · m_lot · m_code
--
-- 관리자 계정은 compose 의 POSTGRES_USER(기본 hotdb). 기본 권한(ALTER DEFAULT PRIVILEGES)은 이 파일을 돌리는 관리자 기준으로 걸린다
-- — 이후 표도 같은 관리자로 만들어야 판별 모듈 · RFC Service 권한이 자동으로 붙는다.
-- 비밀번호는 로컬 개발용(계정명과 같음). 서버에서는 ALTER ROLE … PASSWORD 로 바꾼다.
-- =============================================================================
\set ON_ERROR_STOP on
BEGIN;

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── 계정 ────────────────────────────────────────────────────────────────
--   hotdb_provider : tsdb 쓰기 (MQTT → Provider)
--   svc_<모듈>     : 판별 모듈 4 (mch 가공 · asm 조립 · oft 의장 · pnt 도장) = svc_writer + svc_reader, CDC 슬롯
--   rfc_service    : 레거시 원천 → hot.m_* · l_* 적재, hot.p_progress CDC → SAP
--   hotdb_monitor  : postgres_exporter
DO $$
DECLARE r text;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'svc_reader') THEN CREATE ROLE svc_reader NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'svc_writer') THEN CREATE ROLE svc_writer NOLOGIN; END IF;
  FOREACH r IN ARRAY ARRAY['hotdb_provider', 'hotdb_monitor'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', r, r); END IF;
  END LOOP;
  FOREACH r IN ARRAY ARRAY['rfc_service', 'svc_asm', 'svc_oft', 'svc_pnt', 'svc_mch'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN EXECUTE format('CREATE ROLE %I LOGIN REPLICATION PASSWORD %L', r, r); END IF;
  END LOOP;
END $$;
GRANT svc_reader, svc_writer TO svc_asm, svc_oft, svc_pnt, svc_mch;
GRANT pg_monitor TO hotdb_monitor;

-- ── 스키마 · 표 · 함수 · 권한 (pg_dump -s, V1~V20 적용 결과) ────────────────
--
-- PostgreSQL database dump
--



SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: hot; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA hot;


--
-- Name: ops; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ops;


--
-- Name: ts; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ts;


--
-- Name: ts_chunks; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ts_chunks;


--
-- Name: tsdb; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA tsdb;


--
-- Name: l_bts_move_to_block(); Type: FUNCTION; Schema: hot; Owner: -
--

CREATE FUNCTION hot.l_bts_move_to_block() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  UPDATE hot.m_block b SET bts_lot = NEW.to_lot, bts_at = NEW.move_ts
   WHERE b.hull_no = NEW.hull_no AND b.block_no = NEW.block_no
     AND (b.bts_at IS NULL OR b.bts_at <= NEW.move_ts);
  RETURN NULL;
END $$;


--
-- Name: m_block_bts_fill(); Type: FUNCTION; Schema: hot; Owner: -
--

CREATE FUNCTION hot.m_block_bts_fill() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.bts_lot IS NULL THEN
    SELECT to_lot, move_ts INTO NEW.bts_lot, NEW.bts_at FROM hot.l_bts_move
     WHERE hull_no = NEW.hull_no AND block_no = NEW.block_no ORDER BY move_ts DESC LIMIT 1;
  END IF;
  RETURN NEW;
END $$;


--
-- Name: drop_module(text); Type: FUNCTION; Schema: ops; Owner: -
--

CREATE FUNCTION ops.drop_module(p_module text) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: module_apply_lag(); Type: FUNCTION; Schema: ops; Owner: -
--

CREATE FUNCTION ops.module_apply_lag() RETURNS TABLE(zone text, rows_1m bigint, p50_seconds double precision, p99_seconds double precision, max_seconds double precision)
    LANGUAGE sql STABLE
    AS $$
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
$$;


--
-- Name: provision_module(text, text, text); Type: FUNCTION; Schema: ops; Owner: -
--

CREATE FUNCTION ops.provision_module(p_module text, p_zone_code text, p_display text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE r text := 'svc_' || p_module;
BEGIN
  IF p_module !~ '^[a-z][a-z0-9]{1,15}$' THEN
    RAISE EXCEPTION '모듈 이름은 영소문자로 시작하는 2~16자 [a-z0-9]: %', p_module;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
    EXECUTE format('CREATE ROLE %I LOGIN REPLICATION PASSWORD %L', r, r);
  END IF;
  EXECUTE format('GRANT svc_reader, svc_writer TO %I', r);
  INSERT INTO ops.module (module, zone_code, display_name) VALUES (p_module, p_zone_code, p_display)
  ON CONFLICT (module) DO UPDATE SET zone_code = EXCLUDED.zone_code, display_name = EXCLUDED.display_name;
END
$_$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: l_assy_node; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_assy_node (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    node_id text NOT NULL,
    node_kind text,
    parent_node_id text,
    next_wstg text,
    qty integer,
    weight_kg numeric,
    cad_file_id text,
    plan_sd date,
    part_clsf text,
    series text,
    design_wstg text,
    prod_wstg text,
    rule_applied text,
    shape_key text,
    src_job text,
    polled_at timestamp with time zone,
    src_deleted_at timestamp with time zone,
    CONSTRAINT l_assy_node_node_kind_check CHECK (((node_kind IS NULL) OR (node_kind = ANY (ARRAY['ASSY'::text, 'PART'::text, 'STD'::text]))))
);


--
-- Name: COLUMN l_assy_node.src_deleted_at; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.l_assy_node.src_deleted_at IS '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20';


--
-- Name: l_bts_move; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_bts_move (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    move_ts timestamp with time zone NOT NULL,
    from_lot text,
    to_lot text,
    move_type text,
    src_job text,
    polled_at timestamp with time zone,
    src_deleted_at timestamp with time zone
);


--
-- Name: COLUMN l_bts_move.src_deleted_at; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.l_bts_move.src_deleted_at IS '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20';


--
-- Name: l_cut_part; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_cut_part (
    part_no text NOT NULL,
    hull_no text,
    block_no text,
    dwg_no text,
    dest_wstg text,
    bend_flag boolean,
    cut_done_ts timestamp with time zone,
    group_ts timestamp with time zone,
    sort_ts timestamp with time zone,
    issue_ts timestamp with time zone,
    inbound_ts timestamp with time zone,
    cut_site text,
    weight_kg numeric,
    cut_req_qty integer,
    bend_done_ts timestamp with time zone,
    grind_flag boolean,
    grind_done_ts timestamp with time zone,
    issue_src text,
    mnt_ts timestamp with time zone,
    src_job text,
    polled_at timestamp with time zone,
    src_deleted_at timestamp with time zone
);


--
-- Name: COLUMN l_cut_part.src_deleted_at; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.l_cut_part.src_deleted_at IS '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20';


--
-- Name: l_daily_plan; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_daily_plan (
    plan_date date NOT NULL,
    trade text NOT NULL,
    anchor_type text NOT NULL,
    anchor_id text NOT NULL,
    hull_no text,
    block_no text,
    location_id text,
    org_code text,
    priority integer,
    source_system text,
    loaded_at timestamp with time zone
);


--
-- Name: l_event; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_event (
    unit_id text NOT NULL,
    event_group text NOT NULL,
    event_code text NOT NULL,
    source text NOT NULL,
    event_ts timestamp with time zone NOT NULL,
    conf text,
    src_value text,
    mnt_ts timestamp with time zone,
    conf_group text DEFAULT 'CONF'::text NOT NULL,
    src_job text,
    polled_at timestamp with time zone,
    src_deleted_at timestamp with time zone,
    CONSTRAINT l_event_conf_group_check CHECK ((conf_group = 'CONF'::text)),
    CONSTRAINT l_event_event_group_check CHECK ((event_group = ANY (ARRAY['CHECKPOINT'::text, 'EVENT'::text])))
);


--
-- Name: COLUMN l_event.src_deleted_at; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.l_event.src_deleted_at IS '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20';


--
-- Name: l_outfit_item; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_outfit_item (
    rsnum text NOT NULL,
    rspos text NOT NULL,
    hull_no text,
    block_no text,
    matnr text,
    class_code text,
    do_no text,
    req_qty numeric,
    inst_qty numeric,
    inst_date date,
    issue_ts timestamp with time zone,
    cad_file_id text,
    gdo_no text,
    part_role text,
    temp_qty numeric,
    fixed_qty numeric,
    ready_date date,
    mnt_ts timestamp with time zone,
    src_job text,
    polled_at timestamp with time zone,
    src_deleted_at timestamp with time zone
);


--
-- Name: COLUMN l_outfit_item.src_deleted_at; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.l_outfit_item.src_deleted_at IS '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20';


--
-- Name: l_paint_element; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_paint_element (
    anchor_id text NOT NULL,
    kind text NOT NULL,
    zone_code text NOT NULL,
    elem_code text NOT NULL,
    seq text NOT NULL,
    wo_no text,
    done_date date,
    std_mh numeric,
    insp_req_clsf text,
    mnt_ts timestamp with time zone,
    daily_cmpl text,
    daily_ord_mh numeric,
    wo_mh_split numeric,
    step_code text,
    actl_sd date,
    cnfm_date date,
    area_m2 numeric,
    src_job text,
    polled_at timestamp with time zone,
    src_deleted_at timestamp with time zone
);


--
-- Name: COLUMN l_paint_element.src_deleted_at; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.l_paint_element.src_deleted_at IS '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20';


--
-- Name: l_wo_daily; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_wo_daily (
    wo_no text NOT NULL,
    actl_date date NOT NULL,
    org_code text NOT NULL,
    daily_rate numeric,
    daily_mh numeric,
    rgsr_ts timestamp with time zone,
    src_job text,
    polled_at timestamp with time zone,
    src_deleted_at timestamp with time zone
);


--
-- Name: COLUMN l_wo_daily.src_deleted_at; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.l_wo_daily.src_deleted_at IS '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20';


--
-- Name: l_work_unit; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.l_work_unit (
    unit_id text NOT NULL,
    level text NOT NULL,
    parent_id text,
    act_no text,
    hull_no text,
    block_no text,
    trade text,
    mp text,
    stage text,
    wstg_code text,
    location_id text,
    plan_mh numeric,
    acc_rate numeric,
    plan_sd date,
    plan_fd date,
    actl_sd date,
    actl_fd date,
    attrs jsonb,
    mnt_ts timestamp with time zone,
    site text,
    excluded boolean DEFAULT false NOT NULL,
    exclude_reason text,
    source_table text,
    ref_no text,
    step_code text,
    auto_dist boolean DEFAULT false NOT NULL,
    anchor_type text,
    target_qty numeric,
    target_basis text,
    site_group text DEFAULT 'SITE'::text NOT NULL,
    excl_group text DEFAULT 'EXCLUDE_REASON'::text NOT NULL,
    src_group text DEFAULT 'SOURCE_TABLE'::text NOT NULL,
    src_job text,
    polled_at timestamp with time zone,
    src_deleted_at timestamp with time zone,
    CONSTRAINT l_work_unit_anchor_type_check CHECK (((anchor_type IS NULL) OR (anchor_type = ANY (ARRAY['DWG'::text, 'WSTG'::text, 'GDO'::text, 'PAINT_ORDER'::text])))),
    CONSTRAINT l_work_unit_excl_group_check CHECK ((excl_group = 'EXCLUDE_REASON'::text)),
    CONSTRAINT l_work_unit_level_check CHECK ((level = ANY (ARRAY['ACTIVITY'::text, 'ORDER'::text, 'WO'::text, 'DWG'::text, 'PBLOCK'::text]))),
    CONSTRAINT l_work_unit_site_group_check CHECK ((site_group = 'SITE'::text)),
    CONSTRAINT l_work_unit_src_group_check CHECK ((src_group = 'SOURCE_TABLE'::text))
);


--
-- Name: COLUMN l_work_unit.src_deleted_at; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.l_work_unit.src_deleted_at IS '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20';


--
-- Name: m_block; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_block (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    block_type text,
    weight_t numeric,
    current_trade_stage text,
    bts_lot text,
    bts_at timestamp with time zone,
    sensed_location_id text,
    sensed_x numeric,
    sensed_y numeric,
    sensed_at timestamp with time zone
);


--
-- Name: m_cad_file; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_cad_file (
    cad_file_id text NOT NULL,
    hull_no text,
    block_no text,
    cad_level text,
    item_id text,
    store_id text,
    file_uri text,
    fmt text,
    rev_no text,
    received_at timestamp with time zone,
    source_system text,
    valid boolean DEFAULT true NOT NULL
);


--
-- Name: m_code; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_code (
    code_group text NOT NULL,
    code text NOT NULL,
    name text,
    in_scope boolean DEFAULT true NOT NULL,
    seq integer,
    parent_code text,
    attrs jsonb,
    valid_from date,
    valid_to date
);


--
-- Name: m_device; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_device (
    device_id text NOT NULL,
    class_code text NOT NULL,
    category text NOT NULL,
    parent_device_id text,
    location_id text,
    shop_code text,
    bay_code text,
    zone text,
    x numeric,
    y numeric,
    z numeric,
    heading numeric,
    tilt numeric,
    attrs jsonb,
    valid_from date,
    valid_to date,
    class_group text DEFAULT 'DEVICE_CLASS'::text NOT NULL,
    CONSTRAINT m_device_category_check CHECK ((category = ANY (ARRAY['SENSOR'::text, 'ACTUATOR'::text, 'COMPUTE'::text, 'UTILITY'::text]))),
    CONSTRAINT m_device_check CHECK (((location_id IS NULL) OR (shop_code IS NULL) OR (bay_code IS NULL) OR (location_id = ((shop_code || '‖'::text) || bay_code)))),
    CONSTRAINT m_device_class_group_check CHECK ((class_group = 'DEVICE_CLASS'::text))
);


--
-- Name: m_device_state; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_device_state (
    device_id text NOT NULL,
    last_seen timestamp with time zone,
    online boolean,
    fault boolean,
    fault_code text,
    last_signals jsonb,
    src_received_at timestamp with time zone,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: m_file_store; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_file_store (
    store_id text NOT NULL,
    store_type text,
    base_uri text,
    path_rule text,
    retention_days integer,
    owner text,
    CONSTRAINT m_file_store_store_type_check CHECK ((store_type = ANY (ARRAY['RAW_PCD'::text, 'ARTIFACT'::text, 'CAD'::text, 'IMAGE'::text])))
);


--
-- Name: m_hull; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_hull (
    hull_no text NOT NULL,
    ship_type text,
    status text,
    in_scope boolean DEFAULT true NOT NULL
);


--
-- Name: m_location; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_location (
    location_id text NOT NULL,
    level text NOT NULL,
    parent_id text,
    code text,
    name text,
    zone_type text,
    geom jsonb,
    sensing_unit boolean,
    attrs jsonb,
    valid_from date,
    valid_to date,
    CONSTRAINT m_location_level_check CHECK ((level = ANY (ARRAY['SHOP'::text, 'ZONE'::text, 'JIG'::text]))),
    CONSTRAINT m_location_zone_type_check CHECK (((zone_type IS NULL) OR (zone_type = ANY (ARRAY['BAY'::text, 'SHELTER'::text]))))
);


--
-- Name: m_lot; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_lot (
    lot_code text NOT NULL,
    middle_lot text,
    lot_name text,
    use_type text,
    process_type text,
    indoor boolean,
    area_m2 numeric,
    geom jsonb,
    location_id text
);


--
-- Name: m_signal_code; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_signal_code (
    signal_code text NOT NULL,
    kind text NOT NULL,
    name text,
    data_type text,
    unit text,
    enum_values text[],
    CONSTRAINT m_signal_code_kind_check CHECK ((kind = ANY (ARRAY['SIGNAL'::text, 'EVENT'::text])))
);


--
-- Name: m_signal_def; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_signal_def (
    class_group text DEFAULT 'DEVICE_CLASS'::text NOT NULL,
    class_code text NOT NULL,
    signal_code text NOT NULL,
    sample_period_s integer,
    adapter text,
    address_template text,
    normal_min numeric,
    normal_max numeric,
    fault_rule_type text,
    fault_params jsonb,
    severity text,
    valid_from date,
    valid_to date,
    CONSTRAINT m_signal_def_class_group_check CHECK ((class_group = 'DEVICE_CLASS'::text))
);


--
-- Name: m_signal_override; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.m_signal_override (
    device_id text NOT NULL,
    signal_code text NOT NULL,
    address text,
    normal_min numeric,
    normal_max numeric,
    fault_params jsonb,
    valid_from date,
    valid_to date
);


--
-- Name: p_action; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.p_action (
    action_id bigint NOT NULL,
    action_type text NOT NULL,
    hull_no text,
    block_no text,
    anchor_id text,
    sub_type text,
    detail jsonb,
    value_pct numeric,
    cp_code text,
    reason_code text,
    user_id text,
    raised_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    status text DEFAULT 'OPEN'::text NOT NULL,
    resolved_by text,
    resolved_at timestamp with time zone,
    CONSTRAINT p_action_action_type_check CHECK ((action_type = ANY (ARRAY['ALERT'::text, 'KEYIN'::text]))),
    CONSTRAINT p_action_status_check CHECK ((status = ANY (ARRAY['OPEN'::text, 'RESOLVED'::text, 'DISMISSED'::text])))
);


--
-- Name: p_action_action_id_seq; Type: SEQUENCE; Schema: hot; Owner: -
--

ALTER TABLE hot.p_action ALTER COLUMN action_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME hot.p_action_action_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: p_item_state; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.p_item_state (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    item_type text NOT NULL,
    item_id text NOT NULL,
    anchor_id text,
    state text,
    detect_ts timestamp with time zone,
    legacy_ts timestamp with time zone,
    scan_id text,
    object_key text,
    work_stage text,
    sub_total integer,
    sub_judged integer,
    issue_path text,
    mirror_stage text,
    sync_state text,
    state_history jsonb,
    CONSTRAINT p_item_state_item_type_check CHECK ((item_type = ANY (ARRAY['ASSY'::text, 'PART'::text, 'OUTFIT_ITEM'::text, 'ROLL'::text])))
);


--
-- Name: p_progress; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.p_progress (
    anchor_id text NOT NULL,
    as_of date NOT NULL,
    hull_no text,
    block_no text,
    anchor_type text,
    state text NOT NULL,
    auto_pct numeric,
    legacy_pct numeric,
    gap_pct numeric,
    planned_qty numeric,
    done_qty numeric,
    last_cp text,
    cp_history jsonb,
    evidence jsonb,
    location_id text,
    first_detect_ts timestamp with time zone,
    last_detect_ts timestamp with time zone,
    confirmed_by bigint,
    weight_mh numeric,
    site text,
    excluded boolean DEFAULT false NOT NULL,
    exclude_reason text,
    gap_alert boolean DEFAULT false NOT NULL,
    match_state text,
    match_pool text,
    match_wo_no text,
    match_lead text,
    cp_plan jsonb,
    late_cp_cnt integer,
    cp_stats jsonb,
    judged_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT p_progress_state_check CHECK ((state = ANY (ARRAY['notDue'::text, 'inProgress'::text, 'done'::text, 'confirmed'::text, 'held'::text])))
);


--
-- Name: p_progress_block; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.p_progress_block (
    hull_no text NOT NULL,
    block_no text NOT NULL,
    trade text NOT NULL,
    stage text NOT NULL,
    as_of date NOT NULL,
    auto_pct numeric,
    legacy_pct numeric,
    weight_basis text,
    anchor_cnt integer,
    open_action_cnt integer,
    bts_lot text,
    sensed_location_id text,
    wip_state text,
    first_detect_ts timestamp with time zone,
    last_detect_ts timestamp with time zone,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT p_progress_block_wip_state_check CHECK (((wip_state IS NULL) OR (wip_state = ANY (ARRAY['JUST_IN'::text, 'WORKING'::text, 'DONE'::text, 'WAITING'::text, 'NOT_WIP'::text]))))
);


--
-- Name: s_scan_detection; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.s_scan_detection (
    scan_id text NOT NULL,
    object_key text NOT NULL,
    event_id text,
    received_ts timestamp with time zone,
    object_type text,
    hull_no text,
    block_no text,
    item_id text,
    anchor_type text,
    anchor_id text,
    cad_file_id text,
    match_conf numeric,
    pose jsonb,
    bbox jsonb,
    progress_est numeric,
    count_est integer,
    presence boolean,
    stage_hint text,
    event_type text,
    segment_uri text,
    model_ver text,
    ocr_text text,
    judged boolean DEFAULT false NOT NULL,
    judged_ts timestamp with time zone,
    judge_note text,
    match_attempts integer DEFAULT 0 NOT NULL,
    match_wait_until timestamp with time zone
);


--
-- Name: COLUMN s_scan_detection.match_attempts; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.s_scan_detection.match_attempts IS '앵커 찾기 횟수 (0 = 아직). V20';


--
-- Name: COLUMN s_scan_detection.match_wait_until; Type: COMMENT; Schema: hot; Owner: -
--

COMMENT ON COLUMN hot.s_scan_detection.match_wait_until IS '앵커를 기다리는 끝 시각 — judge_note WAIT_ANCHOR 일 때. 지나면 UNMATCHED 확정. V20';


--
-- Name: s_scan_device; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.s_scan_device (
    scan_id text NOT NULL,
    device_id text NOT NULL,
    role text
);


--
-- Name: s_scan_session; Type: TABLE; Schema: hot; Owner: -
--

CREATE TABLE hot.s_scan_session (
    scan_id text NOT NULL,
    zone text,
    location_id text,
    scan_ts timestamp with time zone,
    raw_store_id text,
    raw_dir text,
    artifact_store_id text,
    artifact_dir text,
    artifacts jsonb,
    model_ver text,
    first_msg_ts timestamp with time zone,
    last_msg_ts timestamp with time zone,
    detection_cnt integer,
    expected_cnt integer,
    status text,
    CONSTRAINT s_scan_session_status_check CHECK (((status IS NULL) OR (status = ANY (ARRAY['OPEN'::text, 'CLOSED'::text, 'FAILED'::text]))))
);


--
-- Name: v_l_orphans; Type: VIEW; Schema: hot; Owner: -
--

CREATE VIEW hot.v_l_orphans AS
 SELECT 'l_work_unit.block'::text AS ref,
    u.unit_id AS key
   FROM hot.l_work_unit u
  WHERE ((u.block_no IS NOT NULL) AND (u.src_deleted_at IS NULL) AND (NOT (EXISTS ( SELECT 1
           FROM hot.m_block b
          WHERE ((b.hull_no = u.hull_no) AND (b.block_no = u.block_no))))))
UNION ALL
 SELECT 'l_event.unit'::text AS ref,
    e.unit_id AS key
   FROM hot.l_event e
  WHERE ((e.src_deleted_at IS NULL) AND (NOT (EXISTS ( SELECT 1
           FROM hot.l_work_unit u
          WHERE (u.unit_id = e.unit_id)))))
UNION ALL
 SELECT 'l_cut_part.dwg'::text AS ref,
    p.part_no AS key
   FROM hot.l_cut_part p
  WHERE ((p.dwg_no IS NOT NULL) AND (p.src_deleted_at IS NULL) AND (NOT (EXISTS ( SELECT 1
           FROM hot.l_work_unit u
          WHERE ((u.unit_id = p.dwg_no) AND (u.level = 'DWG'::text))))))
UNION ALL
 SELECT 'l_wo_daily.wo'::text AS ref,
    d.wo_no AS key
   FROM hot.l_wo_daily d
  WHERE ((d.src_deleted_at IS NULL) AND (NOT (EXISTS ( SELECT 1
           FROM hot.l_work_unit u
          WHERE (u.unit_id = d.wo_no)))))
UNION ALL
 SELECT 'm_block.bts_lot'::text AS ref,
    ((b.hull_no || '‖'::text) || b.block_no) AS key
   FROM hot.m_block b
  WHERE ((b.bts_lot IS NOT NULL) AND (NOT (EXISTS ( SELECT 1
           FROM hot.m_lot l
          WHERE (l.lot_code = b.bts_lot)))));


--
-- Name: cdc_checkpoint; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.cdc_checkpoint (
    pipeline text NOT NULL,
    slot_name text NOT NULL,
    last_applied_lsn pg_lsn NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: cdc_dead_letter; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.cdc_dead_letter (
    id bigint NOT NULL,
    pipeline text NOT NULL,
    route text NOT NULL,
    source text NOT NULL,
    lsn bigint,
    payload jsonb NOT NULL,
    error text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    status text DEFAULT 'PENDING'::text NOT NULL,
    attempts integer DEFAULT 1 NOT NULL,
    resolution text,
    last_error text,
    resolved_at timestamp with time zone,
    CONSTRAINT cdc_dead_letter_status_check CHECK ((status = ANY (ARRAY['PENDING'::text, 'RETRY_REQUESTED'::text, 'RESOLVED'::text, 'DISCARDED'::text])))
);


--
-- Name: cdc_dead_letter_id_seq; Type: SEQUENCE; Schema: ops; Owner: -
--

ALTER TABLE ops.cdc_dead_letter ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME ops.cdc_dead_letter_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: cdc_heartbeat; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.cdc_heartbeat (
    pipeline text NOT NULL,
    beat_at timestamp with time zone NOT NULL
);


--
-- Name: module; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.module (
    module text NOT NULL,
    zone_code text NOT NULL,
    display_name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    trade text,
    CONSTRAINT module_module_check CHECK ((module ~ '^[a-z][a-z0-9]{1,15}$'::text))
);


--
-- Name: poll_state; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.poll_state (
    agent text NOT NULL,
    job text NOT NULL,
    watermark text,
    last_run_at timestamp with time zone,
    last_ok_at timestamp with time zone,
    last_rows bigint,
    last_error text
);


--
-- Name: rfc_progress_sent; Type: TABLE; Schema: ops; Owner: -
--

CREATE TABLE ops.rfc_progress_sent (
    anchor_id text NOT NULL,
    as_of date NOT NULL,
    state text NOT NULL,
    src_judged_at timestamp with time zone NOT NULL,
    src_lsn bigint,
    sent_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    payload_hash text NOT NULL
);


--
-- Name: COLUMN rfc_progress_sent.payload_hash; Type: COMMENT; Schema: ops; Owner: -
--

COMMENT ON COLUMN ops.rfc_progress_sent.payload_hash IS 'state · auto_pct · legacy_pct · last_cp 의 해시 — 바뀌면 다시 보낸다. V20';


--
-- Name: device_signal; Type: TABLE; Schema: ts; Owner: -
--

CREATE TABLE ts.device_signal (
    "time" timestamp with time zone NOT NULL,
    device_id text NOT NULL,
    signal_code text NOT NULL,
    kind text NOT NULL,
    value_num numeric,
    value_text text,
    quality text,
    CONSTRAINT device_signal_kind_check CHECK ((kind = ANY (ARRAY['SIGNAL'::text, 'EVENT'::text])))
);


--
-- Name: sensor_event; Type: TABLE; Schema: ts; Owner: -
--

CREATE TABLE ts.sensor_event (
    "time" timestamp with time zone NOT NULL,
    event_id text NOT NULL,
    scan_id text,
    object_key text,
    zone text,
    stage text,
    location_id text,
    hull_no text,
    block_no text,
    object_type text,
    event_type text,
    source_system text,
    idempotency_key text,
    raw_payload jsonb
);


--
-- Name: actual_history; Type: TABLE; Schema: tsdb; Owner: -
--

CREATE TABLE tsdb.actual_history (
    event_time timestamp with time zone NOT NULL,
    tag_key bigint NOT NULL,
    scan_id uuid NOT NULL,
    event_type text NOT NULL,
    hull_no text NOT NULL,
    block_id text NOT NULL,
    scanned_at timestamp with time zone,
    block_progress_rate real NOT NULL,
    match_confidence real,
    reference_cad_id text,
    model_version text,
    source_time_text text NOT NULL,
    source_ingested_at timestamp with time zone,
    received_at timestamp with time zone NOT NULL,
    CONSTRAINT actual_history_block_progress_rate_check CHECK (((block_progress_rate >= (0)::double precision) AND (block_progress_rate <= (100)::double precision))),
    CONSTRAINT actual_history_event_type_check CHECK ((event_type = ANY (ARRAY['START'::text, 'PROGRESS'::text, 'COMPLETE'::text]))),
    CONSTRAINT actual_history_match_confidence_check CHECK (((match_confidence >= (0)::double precision) AND (match_confidence <= (1)::double precision)))
);


--
-- Name: artifact_history; Type: TABLE; Schema: tsdb; Owner: -
--

CREATE TABLE tsdb.artifact_history (
    event_time timestamp with time zone NOT NULL,
    tag_key bigint NOT NULL,
    scan_id uuid NOT NULL,
    artifact_type text NOT NULL,
    segment_key text DEFAULT ''::text NOT NULL,
    hull_no text NOT NULL,
    block_id text NOT NULL,
    storage_uri text,
    file_size_bytes bigint,
    checksum text,
    transformation_matrix real[],
    produced_by_device_id text,
    model_version text,
    source_time_text text NOT NULL,
    source_ingested_at timestamp with time zone,
    received_at timestamp with time zone NOT NULL,
    CONSTRAINT artifact_history_artifact_type_check CHECK ((artifact_type = ANY (ARRAY['REGISTERED_PCD'::text, 'TRANSFORMATION_MATRIX'::text, 'SEGMENTED_PCD'::text]))),
    CONSTRAINT artifact_history_check CHECK (((artifact_type = 'SEGMENTED_PCD'::text) = (segment_key <> ''::text))),
    CONSTRAINT artifact_history_check1 CHECK (
CASE
    WHEN (artifact_type = 'TRANSFORMATION_MATRIX'::text) THEN ((cardinality(transformation_matrix) = 16) AND (storage_uri IS NULL))
    ELSE ((transformation_matrix IS NULL) AND (storage_uri <> ''::text))
END),
    CONSTRAINT artifact_history_file_size_bytes_check CHECK ((file_size_bytes >= 0))
);


--
-- Name: device; Type: TABLE; Schema: tsdb; Owner: -
--

CREATE TABLE tsdb.device (
    site text NOT NULL,
    device_id text NOT NULL,
    device_role text NOT NULL,
    zone text NOT NULL,
    shop text,
    bay text,
    active boolean DEFAULT true NOT NULL
);


--
-- Name: status_history; Type: TABLE; Schema: tsdb; Owner: -
--

CREATE TABLE tsdb.status_history (
    event_time timestamp with time zone NOT NULL,
    tag_key bigint NOT NULL,
    status text NOT NULL,
    error_code text,
    last_heartbeat_at timestamp with time zone,
    scan_rate_pts_per_sec integer,
    temperature_c real,
    connectivity_rssi smallint,
    fov_mode text,
    source_time_text text NOT NULL,
    source_ingested_at timestamp with time zone,
    received_at timestamp with time zone NOT NULL,
    CONSTRAINT status_history_scan_rate_pts_per_sec_check CHECK ((scan_rate_pts_per_sec >= 0)),
    CONSTRAINT status_history_status_check CHECK ((status = ANY (ARRAY['ONLINE'::text, 'CALIBRATING'::text, 'ERROR'::text, 'OFFLINE'::text])))
);


--
-- Name: tag_catalog; Type: TABLE; Schema: tsdb; Owner: -
--

CREATE TABLE tsdb.tag_catalog (
    tag_key bigint NOT NULL,
    edge_group_id text NOT NULL,
    tag_id character varying(100) NOT NULL,
    mqtt_topic text NOT NULL,
    source_id text NOT NULL,
    site text NOT NULL,
    device_id text NOT NULL,
    channel text NOT NULL,
    artifact_type text,
    active boolean DEFAULT true NOT NULL,
    CONSTRAINT tag_catalog_artifact_type_check CHECK ((artifact_type = ANY (ARRAY['REGISTERED_PCD'::text, 'TRANSFORMATION_MATRIX'::text, 'SEGMENTED_PCD'::text]))),
    CONSTRAINT tag_catalog_channel_check CHECK ((channel = ANY (ARRAY['status'::text, 'actual'::text, 'artifact'::text]))),
    CONSTRAINT tag_catalog_check CHECK (((channel = 'artifact'::text) = (artifact_type IS NOT NULL)))
);


--
-- Name: tag_catalog_tag_key_seq; Type: SEQUENCE; Schema: tsdb; Owner: -
--

ALTER TABLE tsdb.tag_catalog ALTER COLUMN tag_key ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME tsdb.tag_catalog_tag_key_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: l_assy_node l_assy_node_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_assy_node
    ADD CONSTRAINT l_assy_node_pkey PRIMARY KEY (hull_no, block_no, node_id);


--
-- Name: l_bts_move l_bts_move_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_bts_move
    ADD CONSTRAINT l_bts_move_pkey PRIMARY KEY (hull_no, block_no, move_ts);


--
-- Name: l_cut_part l_cut_part_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_cut_part
    ADD CONSTRAINT l_cut_part_pkey PRIMARY KEY (part_no);


--
-- Name: l_daily_plan l_daily_plan_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_daily_plan
    ADD CONSTRAINT l_daily_plan_pkey PRIMARY KEY (plan_date, trade, anchor_type, anchor_id);


--
-- Name: l_event l_event_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_event
    ADD CONSTRAINT l_event_pkey PRIMARY KEY (unit_id, event_group, event_code, source, event_ts);


--
-- Name: l_outfit_item l_outfit_item_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_outfit_item
    ADD CONSTRAINT l_outfit_item_pkey PRIMARY KEY (rsnum, rspos);


--
-- Name: l_paint_element l_paint_element_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_paint_element
    ADD CONSTRAINT l_paint_element_pkey PRIMARY KEY (anchor_id, kind, zone_code, elem_code, seq);


--
-- Name: l_wo_daily l_wo_daily_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_wo_daily
    ADD CONSTRAINT l_wo_daily_pkey PRIMARY KEY (wo_no, actl_date, org_code);


--
-- Name: l_work_unit l_work_unit_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_work_unit
    ADD CONSTRAINT l_work_unit_pkey PRIMARY KEY (unit_id);


--
-- Name: m_block m_block_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_block
    ADD CONSTRAINT m_block_pkey PRIMARY KEY (hull_no, block_no);


--
-- Name: m_cad_file m_cad_file_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_cad_file
    ADD CONSTRAINT m_cad_file_pkey PRIMARY KEY (cad_file_id);


--
-- Name: m_code m_code_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_code
    ADD CONSTRAINT m_code_pkey PRIMARY KEY (code_group, code);


--
-- Name: m_device m_device_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_device
    ADD CONSTRAINT m_device_pkey PRIMARY KEY (device_id);


--
-- Name: m_device_state m_device_state_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_device_state
    ADD CONSTRAINT m_device_state_pkey PRIMARY KEY (device_id);


--
-- Name: m_file_store m_file_store_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_file_store
    ADD CONSTRAINT m_file_store_pkey PRIMARY KEY (store_id);


--
-- Name: m_hull m_hull_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_hull
    ADD CONSTRAINT m_hull_pkey PRIMARY KEY (hull_no);


--
-- Name: m_location m_location_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_location
    ADD CONSTRAINT m_location_pkey PRIMARY KEY (location_id);


--
-- Name: m_lot m_lot_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_lot
    ADD CONSTRAINT m_lot_pkey PRIMARY KEY (lot_code);


--
-- Name: m_signal_code m_signal_code_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_signal_code
    ADD CONSTRAINT m_signal_code_pkey PRIMARY KEY (signal_code);


--
-- Name: m_signal_def m_signal_def_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_signal_def
    ADD CONSTRAINT m_signal_def_pkey PRIMARY KEY (class_code, signal_code);


--
-- Name: m_signal_override m_signal_override_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_signal_override
    ADD CONSTRAINT m_signal_override_pkey PRIMARY KEY (device_id, signal_code);


--
-- Name: p_action p_action_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_action
    ADD CONSTRAINT p_action_pkey PRIMARY KEY (action_id);


--
-- Name: p_item_state p_item_state_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_item_state
    ADD CONSTRAINT p_item_state_pkey PRIMARY KEY (hull_no, block_no, item_type, item_id);


--
-- Name: p_progress_block p_progress_block_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_progress_block
    ADD CONSTRAINT p_progress_block_pkey PRIMARY KEY (hull_no, block_no, trade, stage, as_of);


--
-- Name: p_progress p_progress_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_progress
    ADD CONSTRAINT p_progress_pkey PRIMARY KEY (anchor_id, as_of);


--
-- Name: s_scan_detection s_scan_detection_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_detection
    ADD CONSTRAINT s_scan_detection_pkey PRIMARY KEY (scan_id, object_key);


--
-- Name: s_scan_device s_scan_device_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_device
    ADD CONSTRAINT s_scan_device_pkey PRIMARY KEY (scan_id, device_id);


--
-- Name: s_scan_session s_scan_session_pkey; Type: CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_session
    ADD CONSTRAINT s_scan_session_pkey PRIMARY KEY (scan_id);


--
-- Name: cdc_checkpoint cdc_checkpoint_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.cdc_checkpoint
    ADD CONSTRAINT cdc_checkpoint_pkey PRIMARY KEY (pipeline);


--
-- Name: cdc_dead_letter cdc_dead_letter_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.cdc_dead_letter
    ADD CONSTRAINT cdc_dead_letter_pkey PRIMARY KEY (id);


--
-- Name: cdc_heartbeat cdc_heartbeat_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.cdc_heartbeat
    ADD CONSTRAINT cdc_heartbeat_pkey PRIMARY KEY (pipeline);


--
-- Name: module module_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.module
    ADD CONSTRAINT module_pkey PRIMARY KEY (module);


--
-- Name: module module_zone_code_key; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.module
    ADD CONSTRAINT module_zone_code_key UNIQUE (zone_code);


--
-- Name: poll_state poll_state_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.poll_state
    ADD CONSTRAINT poll_state_pkey PRIMARY KEY (agent, job);


--
-- Name: rfc_progress_sent rfc_progress_sent_pkey; Type: CONSTRAINT; Schema: ops; Owner: -
--

ALTER TABLE ONLY ops.rfc_progress_sent
    ADD CONSTRAINT rfc_progress_sent_pkey PRIMARY KEY (anchor_id, as_of, payload_hash);


--
-- Name: device_signal device_signal_pkey; Type: CONSTRAINT; Schema: ts; Owner: -
--

ALTER TABLE ONLY ts.device_signal
    ADD CONSTRAINT device_signal_pkey PRIMARY KEY ("time", device_id, signal_code);


--
-- Name: sensor_event sensor_event_pkey; Type: CONSTRAINT; Schema: ts; Owner: -
--

ALTER TABLE ONLY ts.sensor_event
    ADD CONSTRAINT sensor_event_pkey PRIMARY KEY ("time", event_id);


--
-- Name: actual_history actual_history_pkey; Type: CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.actual_history
    ADD CONSTRAINT actual_history_pkey PRIMARY KEY (tag_key, event_time, scan_id, event_type);


--
-- Name: artifact_history artifact_history_pkey; Type: CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.artifact_history
    ADD CONSTRAINT artifact_history_pkey PRIMARY KEY (tag_key, event_time, scan_id, artifact_type, segment_key);


--
-- Name: device device_pkey; Type: CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.device
    ADD CONSTRAINT device_pkey PRIMARY KEY (site, device_id);


--
-- Name: status_history status_history_pkey; Type: CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.status_history
    ADD CONSTRAINT status_history_pkey PRIMARY KEY (tag_key, event_time);


--
-- Name: tag_catalog tag_catalog_edge_group_id_tag_id_key; Type: CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.tag_catalog
    ADD CONSTRAINT tag_catalog_edge_group_id_tag_id_key UNIQUE (edge_group_id, tag_id);


--
-- Name: tag_catalog tag_catalog_pkey; Type: CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.tag_catalog
    ADD CONSTRAINT tag_catalog_pkey PRIMARY KEY (tag_key);


--
-- Name: l_assy_node_deleted_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_assy_node_deleted_idx ON hot.l_assy_node USING btree (src_deleted_at) WHERE (src_deleted_at IS NOT NULL);


--
-- Name: l_assy_node_polled_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_assy_node_polled_idx ON hot.l_assy_node USING btree (src_job, polled_at) WHERE (src_deleted_at IS NULL);


--
-- Name: l_assy_node_prod_wstg_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_assy_node_prod_wstg_idx ON hot.l_assy_node USING btree (prod_wstg);


--
-- Name: l_bts_move_deleted_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_bts_move_deleted_idx ON hot.l_bts_move USING btree (src_deleted_at) WHERE (src_deleted_at IS NOT NULL);


--
-- Name: l_bts_move_polled_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_bts_move_polled_idx ON hot.l_bts_move USING btree (src_job, polled_at) WHERE (src_deleted_at IS NULL);


--
-- Name: l_cut_part_deleted_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_cut_part_deleted_idx ON hot.l_cut_part USING btree (src_deleted_at) WHERE (src_deleted_at IS NOT NULL);


--
-- Name: l_cut_part_dwg_no_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_cut_part_dwg_no_idx ON hot.l_cut_part USING btree (dwg_no);


--
-- Name: l_cut_part_hull_no_block_no_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_cut_part_hull_no_block_no_idx ON hot.l_cut_part USING btree (hull_no, block_no);


--
-- Name: l_cut_part_polled_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_cut_part_polled_idx ON hot.l_cut_part USING btree (src_job, polled_at) WHERE (src_deleted_at IS NULL);


--
-- Name: l_event_deleted_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_event_deleted_idx ON hot.l_event USING btree (src_deleted_at) WHERE (src_deleted_at IS NOT NULL);


--
-- Name: l_event_mnt_ts_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_event_mnt_ts_idx ON hot.l_event USING btree (mnt_ts);


--
-- Name: l_event_polled_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_event_polled_idx ON hot.l_event USING btree (src_job, polled_at) WHERE (src_deleted_at IS NULL);


--
-- Name: l_outfit_item_deleted_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_outfit_item_deleted_idx ON hot.l_outfit_item USING btree (src_deleted_at) WHERE (src_deleted_at IS NOT NULL);


--
-- Name: l_outfit_item_gdo_no_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_outfit_item_gdo_no_idx ON hot.l_outfit_item USING btree (gdo_no);


--
-- Name: l_outfit_item_polled_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_outfit_item_polled_idx ON hot.l_outfit_item USING btree (src_job, polled_at) WHERE (src_deleted_at IS NULL);


--
-- Name: l_paint_element_deleted_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_paint_element_deleted_idx ON hot.l_paint_element USING btree (src_deleted_at) WHERE (src_deleted_at IS NOT NULL);


--
-- Name: l_paint_element_polled_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_paint_element_polled_idx ON hot.l_paint_element USING btree (src_job, polled_at) WHERE (src_deleted_at IS NULL);


--
-- Name: l_wo_daily_deleted_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_wo_daily_deleted_idx ON hot.l_wo_daily USING btree (src_deleted_at) WHERE (src_deleted_at IS NOT NULL);


--
-- Name: l_wo_daily_polled_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_wo_daily_polled_idx ON hot.l_wo_daily USING btree (src_job, polled_at) WHERE (src_deleted_at IS NULL);


--
-- Name: l_work_unit_act_no_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_work_unit_act_no_idx ON hot.l_work_unit USING btree (act_no) WHERE (level = 'WO'::text);


--
-- Name: l_work_unit_anchor_type_hull_no_block_no_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_work_unit_anchor_type_hull_no_block_no_idx ON hot.l_work_unit USING btree (anchor_type, hull_no, block_no) WHERE (anchor_type IS NOT NULL);


--
-- Name: l_work_unit_deleted_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_work_unit_deleted_idx ON hot.l_work_unit USING btree (src_deleted_at) WHERE (src_deleted_at IS NOT NULL);


--
-- Name: l_work_unit_hull_no_block_no_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_work_unit_hull_no_block_no_idx ON hot.l_work_unit USING btree (hull_no, block_no);


--
-- Name: l_work_unit_level_plan_sd_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_work_unit_level_plan_sd_idx ON hot.l_work_unit USING btree (level, plan_sd);


--
-- Name: l_work_unit_parent_id_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_work_unit_parent_id_idx ON hot.l_work_unit USING btree (parent_id);


--
-- Name: l_work_unit_polled_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_work_unit_polled_idx ON hot.l_work_unit USING btree (src_job, polled_at) WHERE (src_deleted_at IS NULL);


--
-- Name: l_work_unit_ref_no_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX l_work_unit_ref_no_idx ON hot.l_work_unit USING btree (ref_no) WHERE (ref_no IS NOT NULL);


--
-- Name: m_device_state_updated_at_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX m_device_state_updated_at_idx ON hot.m_device_state USING btree (updated_at);


--
-- Name: m_device_zone_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX m_device_zone_idx ON hot.m_device USING btree (zone);


--
-- Name: m_lot_location_id_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX m_lot_location_id_idx ON hot.m_lot USING btree (location_id);


--
-- Name: p_action_hull_no_block_no_status_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX p_action_hull_no_block_no_status_idx ON hot.p_action USING btree (hull_no, block_no, status);


--
-- Name: p_action_open_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX p_action_open_idx ON hot.p_action USING btree (hull_no, block_no) WHERE (status = 'OPEN'::text);


--
-- Name: p_progress_as_of_anchor_type_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX p_progress_as_of_anchor_type_idx ON hot.p_progress USING btree (as_of, anchor_type);


--
-- Name: p_progress_hull_no_block_no_as_of_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX p_progress_hull_no_block_no_as_of_idx ON hot.p_progress USING btree (hull_no, block_no, as_of);


--
-- Name: s_scan_detection_hull_no_block_no_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX s_scan_detection_hull_no_block_no_idx ON hot.s_scan_detection USING btree (hull_no, block_no);


--
-- Name: s_scan_detection_pending; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX s_scan_detection_pending ON hot.s_scan_detection USING btree (received_ts) WHERE (NOT judged);


--
-- Name: s_scan_detection_wait_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX s_scan_detection_wait_idx ON hot.s_scan_detection USING btree (hull_no, block_no) WHERE (judge_note = 'WAIT_ANCHOR'::text);


--
-- Name: s_scan_session_zone_last_msg_ts_idx; Type: INDEX; Schema: hot; Owner: -
--

CREATE INDEX s_scan_session_zone_last_msg_ts_idx ON hot.s_scan_session USING btree (zone, last_msg_ts);


--
-- Name: cdc_dead_letter_pipeline_created_at_idx; Type: INDEX; Schema: ops; Owner: -
--

CREATE INDEX cdc_dead_letter_pipeline_created_at_idx ON ops.cdc_dead_letter USING btree (pipeline, created_at);


--
-- Name: cdc_dead_letter_pipeline_status_id_idx; Type: INDEX; Schema: ops; Owner: -
--

CREATE INDEX cdc_dead_letter_pipeline_status_id_idx ON ops.cdc_dead_letter USING btree (pipeline, status, id);


--
-- Name: rfc_progress_sent_sent_at_idx; Type: INDEX; Schema: ops; Owner: -
--

CREATE INDEX rfc_progress_sent_sent_at_idx ON ops.rfc_progress_sent USING btree (sent_at);


--
-- Name: device_signal_time_idx; Type: INDEX; Schema: ts; Owner: -
--

CREATE INDEX device_signal_time_idx ON ts.device_signal USING btree ("time" DESC);


--
-- Name: sensor_event_scan_id_time_idx; Type: INDEX; Schema: ts; Owner: -
--

CREATE INDEX sensor_event_scan_id_time_idx ON ts.sensor_event USING btree (scan_id, "time" DESC);


--
-- Name: sensor_event_time_idx; Type: INDEX; Schema: ts; Owner: -
--

CREATE INDEX sensor_event_time_idx ON ts.sensor_event USING btree ("time" DESC);


--
-- Name: actual_history_event_time_idx; Type: INDEX; Schema: tsdb; Owner: -
--

CREATE INDEX actual_history_event_time_idx ON tsdb.actual_history USING btree (event_time DESC);


--
-- Name: actual_history_scan_idx; Type: INDEX; Schema: tsdb; Owner: -
--

CREATE INDEX actual_history_scan_idx ON tsdb.actual_history USING btree (scan_id);


--
-- Name: artifact_history_event_time_idx; Type: INDEX; Schema: tsdb; Owner: -
--

CREATE INDEX artifact_history_event_time_idx ON tsdb.artifact_history USING btree (event_time DESC);


--
-- Name: artifact_history_scan_idx; Type: INDEX; Schema: tsdb; Owner: -
--

CREATE INDEX artifact_history_scan_idx ON tsdb.artifact_history USING btree (scan_id);


--
-- Name: status_history_event_time_idx; Type: INDEX; Schema: tsdb; Owner: -
--

CREATE INDEX status_history_event_time_idx ON tsdb.status_history USING btree (event_time DESC);


--
-- Name: l_bts_move l_bts_move_to_block; Type: TRIGGER; Schema: hot; Owner: -
--

CREATE TRIGGER l_bts_move_to_block AFTER INSERT OR UPDATE ON hot.l_bts_move FOR EACH ROW EXECUTE FUNCTION hot.l_bts_move_to_block();


--
-- Name: m_block m_block_bts_fill; Type: TRIGGER; Schema: hot; Owner: -
--

CREATE TRIGGER m_block_bts_fill BEFORE INSERT ON hot.m_block FOR EACH ROW EXECUTE FUNCTION hot.m_block_bts_fill();


--
-- Name: l_assy_node l_assy_node_cad_file_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_assy_node
    ADD CONSTRAINT l_assy_node_cad_file_id_fkey FOREIGN KEY (cad_file_id) REFERENCES hot.m_cad_file(cad_file_id);


--
-- Name: l_event l_event_conf_group_conf_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_event
    ADD CONSTRAINT l_event_conf_group_conf_fkey FOREIGN KEY (conf_group, conf) REFERENCES hot.m_code(code_group, code);


--
-- Name: l_event l_event_event_group_event_code_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_event
    ADD CONSTRAINT l_event_event_group_event_code_fkey FOREIGN KEY (event_group, event_code) REFERENCES hot.m_code(code_group, code);


--
-- Name: l_outfit_item l_outfit_item_cad_file_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_outfit_item
    ADD CONSTRAINT l_outfit_item_cad_file_id_fkey FOREIGN KEY (cad_file_id) REFERENCES hot.m_cad_file(cad_file_id);


--
-- Name: l_work_unit l_work_unit_excl_group_exclude_reason_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_work_unit
    ADD CONSTRAINT l_work_unit_excl_group_exclude_reason_fkey FOREIGN KEY (excl_group, exclude_reason) REFERENCES hot.m_code(code_group, code);


--
-- Name: l_work_unit l_work_unit_site_group_site_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_work_unit
    ADD CONSTRAINT l_work_unit_site_group_site_fkey FOREIGN KEY (site_group, site) REFERENCES hot.m_code(code_group, code);


--
-- Name: l_work_unit l_work_unit_src_group_source_table_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.l_work_unit
    ADD CONSTRAINT l_work_unit_src_group_source_table_fkey FOREIGN KEY (src_group, source_table) REFERENCES hot.m_code(code_group, code);


--
-- Name: m_block m_block_hull_no_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_block
    ADD CONSTRAINT m_block_hull_no_fkey FOREIGN KEY (hull_no) REFERENCES hot.m_hull(hull_no);


--
-- Name: m_block m_block_sensed_location_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_block
    ADD CONSTRAINT m_block_sensed_location_id_fkey FOREIGN KEY (sensed_location_id) REFERENCES hot.m_location(location_id);


--
-- Name: m_cad_file m_cad_file_store_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_cad_file
    ADD CONSTRAINT m_cad_file_store_id_fkey FOREIGN KEY (store_id) REFERENCES hot.m_file_store(store_id);


--
-- Name: m_device m_device_class_group_class_code_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_device
    ADD CONSTRAINT m_device_class_group_class_code_fkey FOREIGN KEY (class_group, class_code) REFERENCES hot.m_code(code_group, code) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: m_device m_device_location_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_device
    ADD CONSTRAINT m_device_location_id_fkey FOREIGN KEY (location_id) REFERENCES hot.m_location(location_id);


--
-- Name: m_device m_device_parent_device_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_device
    ADD CONSTRAINT m_device_parent_device_id_fkey FOREIGN KEY (parent_device_id) REFERENCES hot.m_device(device_id);


--
-- Name: m_device_state m_device_state_device_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_device_state
    ADD CONSTRAINT m_device_state_device_id_fkey FOREIGN KEY (device_id) REFERENCES hot.m_device(device_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: m_location m_location_parent_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_location
    ADD CONSTRAINT m_location_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES hot.m_location(location_id);


--
-- Name: m_lot m_lot_location_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_lot
    ADD CONSTRAINT m_lot_location_id_fkey FOREIGN KEY (location_id) REFERENCES hot.m_location(location_id);


--
-- Name: m_signal_def m_signal_def_class_group_class_code_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_signal_def
    ADD CONSTRAINT m_signal_def_class_group_class_code_fkey FOREIGN KEY (class_group, class_code) REFERENCES hot.m_code(code_group, code);


--
-- Name: m_signal_def m_signal_def_signal_code_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_signal_def
    ADD CONSTRAINT m_signal_def_signal_code_fkey FOREIGN KEY (signal_code) REFERENCES hot.m_signal_code(signal_code);


--
-- Name: m_signal_override m_signal_override_device_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_signal_override
    ADD CONSTRAINT m_signal_override_device_id_fkey FOREIGN KEY (device_id) REFERENCES hot.m_device(device_id);


--
-- Name: m_signal_override m_signal_override_signal_code_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.m_signal_override
    ADD CONSTRAINT m_signal_override_signal_code_fkey FOREIGN KEY (signal_code) REFERENCES hot.m_signal_code(signal_code);


--
-- Name: p_action p_action_anchor_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_action
    ADD CONSTRAINT p_action_anchor_id_fkey FOREIGN KEY (anchor_id) REFERENCES hot.l_work_unit(unit_id);


--
-- Name: p_item_state p_item_state_anchor_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_item_state
    ADD CONSTRAINT p_item_state_anchor_id_fkey FOREIGN KEY (anchor_id) REFERENCES hot.l_work_unit(unit_id);


--
-- Name: p_item_state p_item_state_scan_id_object_key_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_item_state
    ADD CONSTRAINT p_item_state_scan_id_object_key_fkey FOREIGN KEY (scan_id, object_key) REFERENCES hot.s_scan_detection(scan_id, object_key);


--
-- Name: p_progress p_progress_anchor_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_progress
    ADD CONSTRAINT p_progress_anchor_id_fkey FOREIGN KEY (anchor_id) REFERENCES hot.l_work_unit(unit_id);


--
-- Name: p_progress_block p_progress_block_sensed_location_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_progress_block
    ADD CONSTRAINT p_progress_block_sensed_location_id_fkey FOREIGN KEY (sensed_location_id) REFERENCES hot.m_location(location_id);


--
-- Name: p_progress p_progress_confirmed_by_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_progress
    ADD CONSTRAINT p_progress_confirmed_by_fkey FOREIGN KEY (confirmed_by) REFERENCES hot.p_action(action_id);


--
-- Name: p_progress p_progress_location_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.p_progress
    ADD CONSTRAINT p_progress_location_id_fkey FOREIGN KEY (location_id) REFERENCES hot.m_location(location_id);


--
-- Name: s_scan_detection s_scan_detection_anchor_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_detection
    ADD CONSTRAINT s_scan_detection_anchor_id_fkey FOREIGN KEY (anchor_id) REFERENCES hot.l_work_unit(unit_id);


--
-- Name: s_scan_detection s_scan_detection_cad_file_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_detection
    ADD CONSTRAINT s_scan_detection_cad_file_id_fkey FOREIGN KEY (cad_file_id) REFERENCES hot.m_cad_file(cad_file_id);


--
-- Name: s_scan_detection s_scan_detection_scan_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_detection
    ADD CONSTRAINT s_scan_detection_scan_id_fkey FOREIGN KEY (scan_id) REFERENCES hot.s_scan_session(scan_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: s_scan_device s_scan_device_device_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_device
    ADD CONSTRAINT s_scan_device_device_id_fkey FOREIGN KEY (device_id) REFERENCES hot.m_device(device_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: s_scan_device s_scan_device_scan_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_device
    ADD CONSTRAINT s_scan_device_scan_id_fkey FOREIGN KEY (scan_id) REFERENCES hot.s_scan_session(scan_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: s_scan_session s_scan_session_artifact_store_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_session
    ADD CONSTRAINT s_scan_session_artifact_store_id_fkey FOREIGN KEY (artifact_store_id) REFERENCES hot.m_file_store(store_id);


--
-- Name: s_scan_session s_scan_session_location_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_session
    ADD CONSTRAINT s_scan_session_location_id_fkey FOREIGN KEY (location_id) REFERENCES hot.m_location(location_id);


--
-- Name: s_scan_session s_scan_session_raw_store_id_fkey; Type: FK CONSTRAINT; Schema: hot; Owner: -
--

ALTER TABLE ONLY hot.s_scan_session
    ADD CONSTRAINT s_scan_session_raw_store_id_fkey FOREIGN KEY (raw_store_id) REFERENCES hot.m_file_store(store_id);


--
-- Name: actual_history actual_history_tag_key_fkey; Type: FK CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.actual_history
    ADD CONSTRAINT actual_history_tag_key_fkey FOREIGN KEY (tag_key) REFERENCES tsdb.tag_catalog(tag_key);


--
-- Name: artifact_history artifact_history_tag_key_fkey; Type: FK CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.artifact_history
    ADD CONSTRAINT artifact_history_tag_key_fkey FOREIGN KEY (tag_key) REFERENCES tsdb.tag_catalog(tag_key);


--
-- Name: status_history status_history_tag_key_fkey; Type: FK CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.status_history
    ADD CONSTRAINT status_history_tag_key_fkey FOREIGN KEY (tag_key) REFERENCES tsdb.tag_catalog(tag_key);


--
-- Name: tag_catalog tag_catalog_site_device_id_fkey; Type: FK CONSTRAINT; Schema: tsdb; Owner: -
--

ALTER TABLE ONLY tsdb.tag_catalog
    ADD CONSTRAINT tag_catalog_site_device_id_fkey FOREIGN KEY (site, device_id) REFERENCES tsdb.device(site, device_id);


--
-- Name: SCHEMA hot; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA hot TO svc_writer;
GRANT USAGE ON SCHEMA hot TO svc_reader;
GRANT USAGE ON SCHEMA hot TO rfc_service;
GRANT USAGE ON SCHEMA hot TO hotdb_provider;
GRANT USAGE ON SCHEMA hot TO hotdb_monitor;


--
-- Name: SCHEMA ops; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA ops TO svc_reader;
GRANT USAGE ON SCHEMA ops TO rfc_service;
GRANT USAGE ON SCHEMA ops TO hotdb_monitor;


--
-- Name: SCHEMA ts; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA ts TO svc_writer;
GRANT USAGE ON SCHEMA ts TO svc_reader;
GRANT USAGE ON SCHEMA ts TO rfc_service;
GRANT USAGE ON SCHEMA ts TO hotdb_provider;
GRANT USAGE ON SCHEMA ts TO hotdb_monitor;


--
-- Name: SCHEMA ts_chunks; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA ts_chunks TO svc_writer;
GRANT USAGE ON SCHEMA ts_chunks TO svc_reader;
GRANT USAGE ON SCHEMA ts_chunks TO rfc_service;
GRANT USAGE ON SCHEMA ts_chunks TO hotdb_provider;
GRANT USAGE ON SCHEMA ts_chunks TO hotdb_monitor;


--
-- Name: SCHEMA tsdb; Type: ACL; Schema: -; Owner: -
--

GRANT USAGE ON SCHEMA tsdb TO hotdb_provider;
GRANT USAGE ON SCHEMA tsdb TO svc_reader;
GRANT USAGE ON SCHEMA tsdb TO hotdb_monitor;


--
-- Name: TABLE l_assy_node; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_assy_node TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_assy_node TO rfc_service;
GRANT SELECT ON TABLE hot.l_assy_node TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_assy_node TO hotdb_provider;


--
-- Name: TABLE l_bts_move; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_bts_move TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_bts_move TO rfc_service;
GRANT SELECT ON TABLE hot.l_bts_move TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_bts_move TO hotdb_provider;


--
-- Name: TABLE l_cut_part; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_cut_part TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_cut_part TO rfc_service;
GRANT SELECT ON TABLE hot.l_cut_part TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_cut_part TO hotdb_provider;


--
-- Name: TABLE l_daily_plan; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_daily_plan TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_daily_plan TO rfc_service;
GRANT SELECT ON TABLE hot.l_daily_plan TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_daily_plan TO hotdb_provider;


--
-- Name: TABLE l_event; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_event TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_event TO rfc_service;
GRANT SELECT ON TABLE hot.l_event TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_event TO hotdb_provider;


--
-- Name: TABLE l_outfit_item; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_outfit_item TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_outfit_item TO rfc_service;
GRANT SELECT ON TABLE hot.l_outfit_item TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_outfit_item TO hotdb_provider;


--
-- Name: TABLE l_paint_element; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_paint_element TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_paint_element TO rfc_service;
GRANT SELECT ON TABLE hot.l_paint_element TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_paint_element TO hotdb_provider;


--
-- Name: TABLE l_wo_daily; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_wo_daily TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_wo_daily TO rfc_service;
GRANT SELECT ON TABLE hot.l_wo_daily TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_wo_daily TO hotdb_provider;


--
-- Name: TABLE l_work_unit; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.l_work_unit TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.l_work_unit TO rfc_service;
GRANT SELECT ON TABLE hot.l_work_unit TO hotdb_monitor;
GRANT SELECT ON TABLE hot.l_work_unit TO hotdb_provider;


--
-- Name: TABLE m_block; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_block TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.m_block TO rfc_service;
GRANT SELECT ON TABLE hot.m_block TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_block TO hotdb_provider;


--
-- Name: TABLE m_cad_file; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_cad_file TO svc_reader;
GRANT SELECT ON TABLE hot.m_cad_file TO rfc_service;
GRANT SELECT ON TABLE hot.m_cad_file TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_cad_file TO hotdb_provider;


--
-- Name: TABLE m_code; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_code TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.m_code TO rfc_service;
GRANT SELECT ON TABLE hot.m_code TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_code TO hotdb_provider;


--
-- Name: TABLE m_device; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_device TO svc_reader;
GRANT SELECT ON TABLE hot.m_device TO rfc_service;
GRANT SELECT ON TABLE hot.m_device TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_device TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.m_device TO svc_writer;


--
-- Name: TABLE m_device_state; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_device_state TO svc_reader;
GRANT SELECT ON TABLE hot.m_device_state TO rfc_service;
GRANT SELECT ON TABLE hot.m_device_state TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_device_state TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.m_device_state TO svc_writer;


--
-- Name: TABLE m_file_store; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_file_store TO svc_reader;
GRANT SELECT ON TABLE hot.m_file_store TO rfc_service;
GRANT SELECT ON TABLE hot.m_file_store TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_file_store TO hotdb_provider;


--
-- Name: TABLE m_hull; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_hull TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.m_hull TO rfc_service;
GRANT SELECT ON TABLE hot.m_hull TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_hull TO hotdb_provider;


--
-- Name: TABLE m_location; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_location TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.m_location TO rfc_service;
GRANT SELECT ON TABLE hot.m_location TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_location TO hotdb_provider;


--
-- Name: TABLE m_lot; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_lot TO svc_reader;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE hot.m_lot TO rfc_service;
GRANT SELECT ON TABLE hot.m_lot TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_lot TO hotdb_provider;


--
-- Name: TABLE m_signal_code; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_signal_code TO svc_reader;
GRANT SELECT ON TABLE hot.m_signal_code TO rfc_service;
GRANT SELECT ON TABLE hot.m_signal_code TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_signal_code TO hotdb_provider;


--
-- Name: TABLE m_signal_def; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_signal_def TO svc_reader;
GRANT SELECT ON TABLE hot.m_signal_def TO rfc_service;
GRANT SELECT ON TABLE hot.m_signal_def TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_signal_def TO hotdb_provider;


--
-- Name: TABLE m_signal_override; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.m_signal_override TO svc_reader;
GRANT SELECT ON TABLE hot.m_signal_override TO rfc_service;
GRANT SELECT ON TABLE hot.m_signal_override TO hotdb_monitor;
GRANT SELECT ON TABLE hot.m_signal_override TO hotdb_provider;


--
-- Name: TABLE p_action; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.p_action TO svc_reader;
GRANT SELECT ON TABLE hot.p_action TO rfc_service;
GRANT SELECT ON TABLE hot.p_action TO hotdb_monitor;
GRANT SELECT ON TABLE hot.p_action TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.p_action TO svc_writer;


--
-- Name: SEQUENCE p_action_action_id_seq; Type: ACL; Schema: hot; Owner: -
--

GRANT USAGE ON SEQUENCE hot.p_action_action_id_seq TO svc_writer;


--
-- Name: TABLE p_item_state; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.p_item_state TO svc_reader;
GRANT SELECT ON TABLE hot.p_item_state TO rfc_service;
GRANT SELECT ON TABLE hot.p_item_state TO hotdb_monitor;
GRANT SELECT ON TABLE hot.p_item_state TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.p_item_state TO svc_writer;


--
-- Name: TABLE p_progress; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.p_progress TO svc_reader;
GRANT SELECT ON TABLE hot.p_progress TO rfc_service;
GRANT SELECT ON TABLE hot.p_progress TO hotdb_monitor;
GRANT SELECT ON TABLE hot.p_progress TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.p_progress TO svc_writer;


--
-- Name: TABLE p_progress_block; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.p_progress_block TO svc_reader;
GRANT SELECT ON TABLE hot.p_progress_block TO rfc_service;
GRANT SELECT ON TABLE hot.p_progress_block TO hotdb_monitor;
GRANT SELECT ON TABLE hot.p_progress_block TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.p_progress_block TO svc_writer;


--
-- Name: TABLE s_scan_detection; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.s_scan_detection TO svc_reader;
GRANT SELECT ON TABLE hot.s_scan_detection TO rfc_service;
GRANT SELECT ON TABLE hot.s_scan_detection TO hotdb_monitor;
GRANT SELECT ON TABLE hot.s_scan_detection TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.s_scan_detection TO svc_writer;


--
-- Name: TABLE s_scan_device; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.s_scan_device TO svc_reader;
GRANT SELECT ON TABLE hot.s_scan_device TO rfc_service;
GRANT SELECT ON TABLE hot.s_scan_device TO hotdb_monitor;
GRANT SELECT ON TABLE hot.s_scan_device TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.s_scan_device TO svc_writer;


--
-- Name: TABLE s_scan_session; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.s_scan_session TO svc_reader;
GRANT SELECT ON TABLE hot.s_scan_session TO rfc_service;
GRANT SELECT ON TABLE hot.s_scan_session TO hotdb_monitor;
GRANT SELECT ON TABLE hot.s_scan_session TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE hot.s_scan_session TO svc_writer;


--
-- Name: TABLE v_l_orphans; Type: ACL; Schema: hot; Owner: -
--

GRANT SELECT ON TABLE hot.v_l_orphans TO svc_reader;
GRANT SELECT ON TABLE hot.v_l_orphans TO rfc_service;
GRANT SELECT ON TABLE hot.v_l_orphans TO hotdb_monitor;
GRANT SELECT ON TABLE hot.v_l_orphans TO hotdb_provider;


--
-- Name: TABLE cdc_checkpoint; Type: ACL; Schema: ops; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE ops.cdc_checkpoint TO svc_reader;
GRANT SELECT,INSERT,UPDATE ON TABLE ops.cdc_checkpoint TO rfc_service;
GRANT SELECT ON TABLE ops.cdc_checkpoint TO hotdb_monitor;


--
-- Name: TABLE cdc_dead_letter; Type: ACL; Schema: ops; Owner: -
--

GRANT SELECT,INSERT ON TABLE ops.cdc_dead_letter TO svc_reader;
GRANT SELECT,INSERT ON TABLE ops.cdc_dead_letter TO rfc_service;
GRANT SELECT ON TABLE ops.cdc_dead_letter TO hotdb_monitor;


--
-- Name: COLUMN cdc_dead_letter.status; Type: ACL; Schema: ops; Owner: -
--

GRANT UPDATE(status) ON TABLE ops.cdc_dead_letter TO svc_reader;
GRANT UPDATE(status) ON TABLE ops.cdc_dead_letter TO rfc_service;


--
-- Name: COLUMN cdc_dead_letter.attempts; Type: ACL; Schema: ops; Owner: -
--

GRANT UPDATE(attempts) ON TABLE ops.cdc_dead_letter TO svc_reader;
GRANT UPDATE(attempts) ON TABLE ops.cdc_dead_letter TO rfc_service;


--
-- Name: COLUMN cdc_dead_letter.resolution; Type: ACL; Schema: ops; Owner: -
--

GRANT UPDATE(resolution) ON TABLE ops.cdc_dead_letter TO svc_reader;
GRANT UPDATE(resolution) ON TABLE ops.cdc_dead_letter TO rfc_service;


--
-- Name: COLUMN cdc_dead_letter.last_error; Type: ACL; Schema: ops; Owner: -
--

GRANT UPDATE(last_error) ON TABLE ops.cdc_dead_letter TO svc_reader;
GRANT UPDATE(last_error) ON TABLE ops.cdc_dead_letter TO rfc_service;


--
-- Name: COLUMN cdc_dead_letter.resolved_at; Type: ACL; Schema: ops; Owner: -
--

GRANT UPDATE(resolved_at) ON TABLE ops.cdc_dead_letter TO svc_reader;
GRANT UPDATE(resolved_at) ON TABLE ops.cdc_dead_letter TO rfc_service;


--
-- Name: SEQUENCE cdc_dead_letter_id_seq; Type: ACL; Schema: ops; Owner: -
--

GRANT USAGE ON SEQUENCE ops.cdc_dead_letter_id_seq TO svc_reader;
GRANT USAGE ON SEQUENCE ops.cdc_dead_letter_id_seq TO rfc_service;


--
-- Name: TABLE cdc_heartbeat; Type: ACL; Schema: ops; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE ops.cdc_heartbeat TO svc_reader;
GRANT SELECT,INSERT,UPDATE ON TABLE ops.cdc_heartbeat TO rfc_service;
GRANT SELECT ON TABLE ops.cdc_heartbeat TO hotdb_monitor;


--
-- Name: TABLE module; Type: ACL; Schema: ops; Owner: -
--

GRANT SELECT ON TABLE ops.module TO svc_reader;
GRANT SELECT ON TABLE ops.module TO rfc_service;
GRANT SELECT ON TABLE ops.module TO hotdb_monitor;


--
-- Name: TABLE poll_state; Type: ACL; Schema: ops; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE ops.poll_state TO rfc_service;
GRANT SELECT ON TABLE ops.poll_state TO hotdb_monitor;


--
-- Name: TABLE rfc_progress_sent; Type: ACL; Schema: ops; Owner: -
--

GRANT SELECT,INSERT ON TABLE ops.rfc_progress_sent TO rfc_service;
GRANT SELECT ON TABLE ops.rfc_progress_sent TO hotdb_monitor;


--
-- Name: TABLE device_signal; Type: ACL; Schema: ts; Owner: -
--

GRANT SELECT ON TABLE ts.device_signal TO svc_reader;
GRANT SELECT ON TABLE ts.device_signal TO rfc_service;
GRANT SELECT ON TABLE ts.device_signal TO hotdb_monitor;
GRANT SELECT,INSERT ON TABLE ts.device_signal TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE ts.device_signal TO svc_writer;


--
-- Name: TABLE sensor_event; Type: ACL; Schema: ts; Owner: -
--

GRANT SELECT ON TABLE ts.sensor_event TO svc_reader;
GRANT SELECT ON TABLE ts.sensor_event TO rfc_service;
GRANT SELECT ON TABLE ts.sensor_event TO hotdb_monitor;
GRANT SELECT,INSERT ON TABLE ts.sensor_event TO hotdb_provider;
GRANT INSERT,DELETE,UPDATE ON TABLE ts.sensor_event TO svc_writer;


--
-- Name: TABLE actual_history; Type: ACL; Schema: tsdb; Owner: -
--

GRANT SELECT,INSERT ON TABLE tsdb.actual_history TO hotdb_provider;
GRANT SELECT ON TABLE tsdb.actual_history TO svc_reader;
GRANT SELECT ON TABLE tsdb.actual_history TO hotdb_monitor;


--
-- Name: TABLE artifact_history; Type: ACL; Schema: tsdb; Owner: -
--

GRANT SELECT,INSERT ON TABLE tsdb.artifact_history TO hotdb_provider;
GRANT SELECT ON TABLE tsdb.artifact_history TO svc_reader;
GRANT SELECT ON TABLE tsdb.artifact_history TO hotdb_monitor;


--
-- Name: TABLE device; Type: ACL; Schema: tsdb; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE tsdb.device TO hotdb_provider;
GRANT SELECT ON TABLE tsdb.device TO svc_reader;
GRANT SELECT ON TABLE tsdb.device TO hotdb_monitor;


--
-- Name: TABLE status_history; Type: ACL; Schema: tsdb; Owner: -
--

GRANT SELECT,INSERT ON TABLE tsdb.status_history TO hotdb_provider;
GRANT SELECT ON TABLE tsdb.status_history TO svc_reader;
GRANT SELECT ON TABLE tsdb.status_history TO hotdb_monitor;


--
-- Name: TABLE tag_catalog; Type: ACL; Schema: tsdb; Owner: -
--

GRANT SELECT,INSERT,UPDATE ON TABLE tsdb.tag_catalog TO hotdb_provider;
GRANT SELECT ON TABLE tsdb.tag_catalog TO svc_reader;
GRANT SELECT ON TABLE tsdb.tag_catalog TO hotdb_monitor;


--
-- Name: SEQUENCE tag_catalog_tag_key_seq; Type: ACL; Schema: tsdb; Owner: -
--

GRANT USAGE ON SEQUENCE tsdb.tag_catalog_tag_key_seq TO hotdb_provider;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: ts_chunks; Owner: -
--

ALTER DEFAULT PRIVILEGES IN SCHEMA ts_chunks GRANT INSERT ON TABLES TO hotdb_provider;
ALTER DEFAULT PRIVILEGES IN SCHEMA ts_chunks GRANT SELECT ON TABLES TO svc_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ts_chunks GRANT SELECT ON TABLES TO rfc_service;
ALTER DEFAULT PRIVILEGES IN SCHEMA ts_chunks GRANT SELECT ON TABLES TO hotdb_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA ts_chunks GRANT INSERT ON TABLES TO svc_writer;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: tsdb; Owner: -
--

ALTER DEFAULT PRIVILEGES IN SCHEMA tsdb GRANT SELECT,INSERT ON TABLES TO hotdb_provider;
ALTER DEFAULT PRIVILEGES IN SCHEMA tsdb GRANT SELECT ON TABLES TO svc_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA tsdb GRANT SELECT ON TABLES TO hotdb_monitor;


--
-- PostgreSQL database dump complete
--



-- ── TimescaleDB — 하이퍼테이블 · 압축 · 보존 (V2 · V12 · V13 · V16 · V17 최종값) ──
SELECT pg_catalog.set_config('search_path', 'public', false);   -- pg_dump 본문이 비워 둔 search_path 복구 (timescaledb 함수)
-- tsdb 청크는 _timescaledb_internal 에 둔다 (tsdb_cdc_pub 이 그 스키마째로 싣는다)
SELECT create_hypertable('tsdb.status_history',   by_range('event_time', INTERVAL '7 days'), create_default_indexes => false);
SELECT create_hypertable('tsdb.actual_history',   by_range('event_time', INTERVAL '7 days'), create_default_indexes => false);
SELECT create_hypertable('tsdb.artifact_history', by_range('event_time', INTERVAL '7 days'), create_default_indexes => false);
ALTER TABLE tsdb.status_history   SET (timescaledb.compress, timescaledb.compress_segmentby = 'tag_key', timescaledb.compress_orderby = 'event_time DESC');
ALTER TABLE tsdb.actual_history   SET (timescaledb.compress, timescaledb.compress_segmentby = 'tag_key', timescaledb.compress_orderby = 'event_time DESC');
ALTER TABLE tsdb.artifact_history SET (timescaledb.compress, timescaledb.compress_segmentby = 'tag_key', timescaledb.compress_orderby = 'event_time DESC');
-- 압축은 CDC 소비자가 따라잡을 여유(7일)를 두고 건다 — 압축된 행은 CDC 로 나오지 않는다
SELECT remove_compression_policy('tsdb.status_history',   if_exists => true);
SELECT remove_compression_policy('tsdb.actual_history',   if_exists => true);
SELECT remove_compression_policy('tsdb.artifact_history', if_exists => true);
SELECT add_compression_policy('tsdb.status_history',   compress_after => INTERVAL '7 days', schedule_interval => INTERVAL '3 hours');  -- 원래 청크 6시간 시절 주기 유지
SELECT add_compression_policy('tsdb.actual_history',   compress_after => INTERVAL '7 days');
SELECT add_compression_policy('tsdb.artifact_history', compress_after => INTERVAL '7 days');
SELECT add_retention_policy('tsdb.status_history',   drop_after => INTERVAL '90 days');
SELECT add_retention_policy('tsdb.actual_history',   drop_after => INTERVAL '90 days');
SELECT add_retention_policy('tsdb.artifact_history', drop_after => INTERVAL '90 days');

-- ts 청크는 ts_chunks 에 (tsdb publication 에 섞이지 않게)
SELECT create_hypertable('ts.device_signal', 'time', chunk_time_interval => INTERVAL '7 days',
                         associated_schema_name => 'ts_chunks', create_default_indexes => false);
SELECT create_hypertable('ts.sensor_event',  'time', chunk_time_interval => INTERVAL '7 days',
                         associated_schema_name => 'ts_chunks', create_default_indexes => false);
SELECT add_retention_policy('ts.device_signal', drop_after => INTERVAL '90 days');
SELECT add_retention_policy('ts.sensor_event',  drop_after => INTERVAL '90 days');

-- ── publication — CDC 소비자 (V4 · V17) ──────────────────────────────────
-- 복제 슬롯은 만들지 않는다. 소비자(Debezium)가 처음 붙을 때 자기 슬롯을 만든다.
CREATE PUBLICATION tsdb_cdc_pub
  FOR TABLE tsdb.device, tsdb.tag_catalog, ops.cdc_heartbeat,
      TABLES IN SCHEMA _timescaledb_internal;
CREATE PUBLICATION hot_cdc_pub FOR TABLE ops.cdc_heartbeat, hot.p_progress;   -- SAP 로 가는 것은 실적뿐

-- ── 사전 시드 — m_code (TRADE · CHECKPOINT · EVENT · TRADE_RULE …) · 신호 사전 · 모듈 ──
--
-- PostgreSQL database dump
--



SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: m_code; Type: TABLE DATA; Schema: hot; Owner: postgres
--

INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('TRADE', 'C', '가공', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('TRADE', 'S', '조립', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('TRADE', 'W', '선행의장', true, 3, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('TRADE', 'P', '선행도장', true, 4, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ANCHOR_TYPE', 'DWG', '절단 도면', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ANCHOR_TYPE', 'WSTG', '조립 송선', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ANCHOR_TYPE', 'GDO', '의장 GDO', true, 3, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ANCHOR_TYPE', 'PAINT_ORDER', '도장 블록', true, 4, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SITE', 'IN', '사내', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SITE', 'OUT', '사외', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CONF', 'HIGH', '높음', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CONF', 'MID', '중간', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CONF', 'LOW', '낮음 — 완료 사실만', true, 3, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EXCLUDE_REASON', 'DWG_D', '도면 상태 D', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EXCLUDE_REASON', 'CAS_PAS', 'CAS · PAS 조립 W/O (조립으로 본다)', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EXCLUDE_REASON', 'TBAR', 'T-BAR 계열 (T · X · K · Y)', true, 3, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EXCLUDE_REASON', 'SSYW', '선판계 SSYW', true, 4, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EXCLUDE_REASON', 'AUTO_DIST', '자동분배 W/O (413 대상 아님)', true, 5, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SOURCE_TABLE', 'YPWG411M', '작업오더 마스터', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SOURCE_TABLE', 'YPWC310M', '도면별 절단 · 마킹', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SOURCE_TABLE', 'ZPPT200', '오더 공정 속성 (의장)', true, 3, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SOURCE_TABLE', 'YDEH060M', '부재 마스터', true, 4, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SOURCE_TABLE', 'YPWP720M', '도장 미세공정 요소', true, 5, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SOURCE_TABLE', 'ZCOT001', '공사 기준정보', true, 6, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('SOURCE_TABLE', 'ZPST0630', '외주 · 정산 네트워크 (액티비티)', true, 7, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'ALL', '공통', true, 0, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'LIDAR', '라이다', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'PANTILT', '팬틸트', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'EDGE_PC', '엣지 PC', true, 3, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'UTILITY', '제습기 · 판넬', true, 4, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('PART_ROLE', 'SUPPORT', '지지류', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('PART_ROLE', 'BODY', '본체', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('PAINT_STEP', 'SP', 'S/P', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('PAINT_STEP', 'TUP', 'T/UP', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('PAINT_STEP', 'FINAL', 'FINAL', true, 3, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'MARK_DONE', '마킹 완료 (절단 착수)', true, 6, 'C', '{"basis": "DWG", "phase": "CUT", "weight": "MH", "judge_source": "l_event:MARK_DONE"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'CUT_DONE', '절단 완료', true, 7, 'C', '{"done": true, "note": "사내 WO_CT · 사외 310M 두 원천, 합치지 않는다", "basis": "DWG", "phase": "CUT", "weight": "MH", "judge_source": "l_event:CUT_DONE"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'BEND_DONE', '곡가공', true, 8, 'C', '{"note": "벤딩 대상 중 완료 — 화면은 보조 줄", "basis": "PART", "phase": "CUT", "target": "bend_flag", "weight": "COUNT", "judge_source": "l_cut_part:bend_done_ts"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'GRIND', '사상', false, 85, 'C', '{"note": "보류 — 모듬판 단위 일괄, 넣는다면 키는 모듬번호", "basis": "PART", "phase": "CUT", "target": "grind_flag", "weight": "COUNT", "judge_source": "l_cut_part:grind_done_ts"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'GROUP', '모둠 선별', true, 9, 'C', '{"note": "송선 기준 · 분모 = 모듬 대상 부재", "basis": "PART", "phase": "CUT", "weight": "COUNT", "judge_source": "l_cut_part:group_ts"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'PART_ISSUE', '최종(부재) 불출', true, 10, 'C', '{"note": "대체: 송선 조립 W/O 착수일", "basis": "PART", "phase": "CUT", "weight": "COUNT", "judge_source": "l_cut_part:issue_ts"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'ASSY_ISSUED', '발행 (411 등록일)', true, 1, 'S', '{"basis": "WSTG", "judge_source": "l_event:ASSY_ISSUED"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'ASSY_STARTED', '착수', true, 2, 'S', '{"basis": "WSTG", "judge_source": "l_work_unit:actl_sd"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'ASSY_DONE', '완료', true, 3, 'S', '{"done": true, "basis": "WSTG", "judge_source": "l_work_unit:actl_fd"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'ASSY_INSPECTED', '검사 (YPMA100M)', true, 4, 'S', '{"basis": "WSTG", "judge_source": "l_event:ASSY_INSPECTED"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'ASSY_OUT', '반출 (검사장)', true, 5, 'S', '{"basis": "WSTG", "judge_source": "l_event:ASSY_OUT"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'OFT_IN', '반입 (P00/W00)', true, 1, 'W', '{"basis": "GDO"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'OFT_SUPPORT', '지지류', true, 2, 'W', '{"basis": "GDO"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'OFT_BODY', '본체', true, 3, 'W', '{"basis": "GDO"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'OFT_FULL', '100%', true, 4, 'W', '{"done": true, "basis": "GDO"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'OFT_OUT', '반출', true, 5, 'W', '{"basis": "GDO"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'PNT_IN', 'BTS 반입', true, 1, 'P', '{"basis": "PAINT_ORDER"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'PNT_OUT', 'BTS 반출', true, 2, 'P', '{"done": true, "basis": "PAINT_ORDER"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EVENT', 'STEEL_SECURED', '강재 확보 (입고 1층)', true, 1, 'C', '{"source": "ZPPT005_1.AEFLG", "denominator": "YDEH081M"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EVENT', 'CARY_DUE', '반입예정 경과 (입고 2층)', true, 2, 'C', '{"alert": "CARRY_OVERDUE", "source": "YPWC111M.CARY_DUE_DATE"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EVENT', 'STEEL_REISSUE', '겹장 재입고 · 재불출 (K73 · R)', true, 3, 'C', '{"adjust": "STEEL_ISSUE"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EVENT', 'DWG_CONFIRMED', '도면 확정', true, 4, 'C', '{"source": "ZPPT005.CRAT_IND"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EVENT', 'PART_INBOUND', '사외 절단 부재 반입 (YCPS301M)', true, 5, 'C', '{"source": "YCPS301M"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('TRADE_RULE', 'C', '가공 — 절점 도달이 곧 실적. 도면 완료율 시수 가중', true, NULL, NULL, '{"legacy": "POST", "done_cp": "CUT_DONE", "gap_rule": "NONE", "pct_basis": "DWG_MH", "back_judge": false, "state_rule": "CP_REACHED", "weight_col": "plan_mh", "site_out_skips": ["STEEL_IN", "PICK1", "PICK2", "STEEL_ISSUE"], "list_window_days": 30}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'PICK1', '1차 선별', true, 2, 'C', '{"basis": "MAT", "phase": "STOCK", "weight": "WEIGHT", "skip_if": "EMERGENCY", "judge_source": "l_event:PICK1", "record_group": "YARD"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ALERT', 'UNMATCHED', '인식했는데 레거시 앵커가 없음', true, 1, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ALERT', 'LOW_CONFIDENCE', '정합도가 모자라 실적에 쓰지 않음', true, 2, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ALERT', 'ORDER_REVERSED', '앞 절점 없이 뒤 절점이 찍힘', true, 3, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ALERT', 'CARRY_OVERDUE', '강재 반입예정일이 지났는데 불출 전', true, 4, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ALERT', 'GAP', '자동 실적과 레거시 실적 차이가 기준 초과', true, 5, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('ALERT', 'WO_MISMATCH', '인식한 송선에 맞는 W/O 가 계획 범위에 없음', true, 6, NULL, NULL, NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'PICK2', '2차 선별', true, 3, 'C', '{"basis": "MAT", "phase": "STOCK", "weight": "WEIGHT", "skip_if": "EMERGENCY", "judge_source": "l_event:PICK2", "record_group": "YARD"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'STEEL_IN', '강재 입고', true, 1, 'C', '{"note": "세 층 — 확보 STEEL_SECURED · 반입예정 경과 CARY_DUE · 도착 STEEL_IN. 도착은 운영 ProSSYS 재추출 뒤", "basis": "MAT", "phase": "STOCK", "weight": "WEIGHT", "judge_source": "l_event:STEEL_IN", "record_group": "YARD"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'PRETREAT', '전처리', true, 5, 'C', '{"note": "도면의 강판 전부 Y", "basis": "DWG", "phase": "CUT", "weight": "MH", "judge_source": "l_event:PRETREAT", "record_group": "LINE"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('CHECKPOINT', 'STEEL_ISSUE', '강재 불출', true, 4, 'C', '{"note": "= 전처리 라인 투입 = 가공 입고. K73 재입고분 차감", "basis": "MAT", "phase": "STOCK", "weight": "WEIGHT", "judge_source": "l_event:STEEL_ISSUE", "record_group": "LINE"}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('TRADE_RULE', 'P', '선행도장 — Σ413 ÷ Σ212 (공정당 1회)', true, NULL, NULL, '{"legacy": "CONTINUOUS", "done_pct": 100, "gap_rule": "NONE", "min_conf": 0.9, "pct_basis": "MH_212", "state_rule": "CONTINUOUS", "denominator": "MH_212_ONCE_PER_OPER", "anchor_wait_days": 7, "list_window_days": 30}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('TRADE_RULE', 'S', '조립 — 인식 개수 ÷ 목표 개수. 보조 Σ413 ÷ ΣSTD_MH', true, NULL, NULL, '{"legacy": "STEP", "gap_rule": "RECORD_ONLY_THEN_ALERT_3D", "min_conf": 0.9, "pct_basis": "COUNT", "state_rule": "DETECT", "held_if_no_wo": true, "match_day_pool": "WO_PLAN", "match_week_days": 28, "anchor_wait_days": 7, "list_window_days": 30}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('TRADE_RULE', 'W', '선행의장 — 설치 품목 ÷ 소요 품목', true, NULL, NULL, '{"legacy": "SETTLEMENT", "weight": "WO_MH_ELSE_ITEMS", "gap_rule": "ALERT_10", "min_conf": 0.9, "pct_basis": "ITEMS", "state_rule": "ITEMS_ALL", "gap_alert_pct": 10, "anchor_wait_days": 7, "list_window_days": 30}', NULL, NULL);
INSERT INTO hot.m_code (code_group, code, name, in_scope, seq, parent_code, attrs, valid_from, valid_to) VALUES ('EXCLUDE_REASON', 'SOURCE_DELETED', '레거시 원천에서 지워짐', true, 90, NULL, NULL, NULL, NULL);


--
-- Data for Name: m_signal_code; Type: TABLE DATA; Schema: hot; Owner: postgres
--

INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('status', 'SIGNAL', '장비 상태', 'enum', NULL, NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('heartbeat', 'SIGNAL', '하트비트', 'text', NULL, NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('scan_rate', 'SIGNAL', '스캔 속도', 'num', 'pts/s', NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('temperature', 'SIGNAL', '온도', 'num', '°C', NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('rssi', 'SIGNAL', '연결 신호 세기', 'num', 'dBm', NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('fov_mode', 'SIGNAL', '시야각 모드', 'text', NULL, NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('error_code', 'SIGNAL', '오류 코드', 'text', NULL, NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('ONLINE', 'EVENT', '온라인 전환', NULL, NULL, NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('OFFLINE', 'EVENT', '오프라인 전환', NULL, NULL, NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('FAULT_ON', 'EVENT', '고장 발생', NULL, NULL, NULL);
INSERT INTO hot.m_signal_code (signal_code, kind, name, data_type, unit, enum_values) VALUES ('FAULT_OFF', 'EVENT', '고장 해제', NULL, NULL, NULL);


--
-- Data for Name: m_signal_def; Type: TABLE DATA; Schema: hot; Owner: postgres
--

INSERT INTO hot.m_signal_def (class_group, class_code, signal_code, sample_period_s, adapter, address_template, normal_min, normal_max, fault_rule_type, fault_params, severity, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'ALL', 'heartbeat', 1, 'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', NULL, NULL, 'HEARTBEAT_TIMEOUT', '{"timeout_s": 30}', 'HIGH', NULL, NULL);
INSERT INTO hot.m_signal_def (class_group, class_code, signal_code, sample_period_s, adapter, address_template, normal_min, normal_max, fault_rule_type, fault_params, severity, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'LIDAR', 'status', 1, 'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', NULL, NULL, 'ENUM', '{"fault_values": ["ERROR"]}', 'HIGH', NULL, NULL);
INSERT INTO hot.m_signal_def (class_group, class_code, signal_code, sample_period_s, adapter, address_template, normal_min, normal_max, fault_rule_type, fault_params, severity, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'LIDAR', 'scan_rate', 1, 'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', NULL, NULL, 'RANGE', '{"min": 100000}', 'MID', NULL, NULL);
INSERT INTO hot.m_signal_def (class_group, class_code, signal_code, sample_period_s, adapter, address_template, normal_min, normal_max, fault_rule_type, fault_params, severity, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'LIDAR', 'temperature', 1, 'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', NULL, NULL, 'RANGE', '{"max": 70}', 'MID', NULL, NULL);
INSERT INTO hot.m_signal_def (class_group, class_code, signal_code, sample_period_s, adapter, address_template, normal_min, normal_max, fault_rule_type, fault_params, severity, valid_from, valid_to) VALUES ('DEVICE_CLASS', 'LIDAR', 'rssi', 1, 'ISL', '{device_id}.ot_device_{zone}_status.raw_payload', NULL, NULL, 'RANGE', '{"min": -85}', 'LOW', NULL, NULL);


--
-- Data for Name: module; Type: TABLE DATA; Schema: ops; Owner: postgres
--

INSERT INTO ops.module (module, zone_code, display_name, created_at, trade) VALUES ('mch', 'MACHINING', '가공 실적 판별', '2026-10-28 13:12:01.428499+09', 'C');
INSERT INTO ops.module (module, zone_code, display_name, created_at, trade) VALUES ('asm', 'ASSEMBLY', '조립 실적 판별', '2026-10-28 13:12:01.428499+09', 'S');
INSERT INTO ops.module (module, zone_code, display_name, created_at, trade) VALUES ('oft', 'OUTFITTING', '의장 실적 판별', '2026-10-28 13:12:01.428499+09', 'W');
INSERT INTO ops.module (module, zone_code, display_name, created_at, trade) VALUES ('pnt', 'PAINTING', '도장 실적 판별', '2026-10-28 13:12:01.428499+09', 'P');


--
-- PostgreSQL database dump complete
--

COMMIT;
