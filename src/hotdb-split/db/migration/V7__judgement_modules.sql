-- [판별 모듈] 권역 RDB 를 "모듈 N개"로 일반화하고, 연관된 표끼리 관계(FK)를 건다.
--
-- 시스템 아키텍쳐(2026-10-06): 가공 실적 판별 모듈 · 조립 · 의장 · 도장 실적 판별 서비스 — 판별 모듈은 늘 수 있다.
-- 모듈 하나 = 스키마 svc_<모듈> 하나 + 계정 svc_<모듈> 하나 + 복제 슬롯 zone_<모듈> 하나.
-- 모듈을 더할 때는 V 파일에 한 줄이면 된다:
--     SELECT ops.provision_module('cut', 'CUTTING', '절단 실적 판별');
--
-- 관계 (모듈 스키마 안 · 연관된 데이터는 연관되게):
--     tsdb.device ◀── device_status_current ◀── device_status_change
--     tsdb.device ◀── scan ◀── actual_result
--                         ◀── artifact
-- 모듈 안 FK 는 DEFERRABLE INITIALLY DEFERRED — 커밋 때 검사하므로 한 배치 안에서 부모 · 자식 순서가 섞여도 된다.
-- 배치를 넘어서는 순서(산출물이 스캔 START 보다 먼저 옴)는 라우트가 부모 자리를 먼저 만들어 푼다(scan-stub).

-- ── 모듈 등록부 ─────────────────────────────────────────────────────────
CREATE TABLE ops.module (
  module       text PRIMARY KEY CHECK (module ~ '^[a-z][a-z0-9]{1,15}$'),
  schema_name  text NOT NULL UNIQUE,
  zone_code    text NOT NULL UNIQUE,     -- tsdb.device.zone 값. 이 값의 장비만 이 모듈로 간다
  display_name text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT ON ops.module TO svc_reader, rfc_provider, hotdb_monitor;

-- ── 모듈 만들기 ─────────────────────────────────────────────────────────
-- 몇 번을 불러도 같은 결과(이미 있는 계정 · 스키마 · 표는 그대로 둔다).
CREATE FUNCTION ops.provision_module(p_module text, p_zone_code text, p_display text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE
  s text := 'svc_' || p_module;
BEGIN
  IF p_module !~ '^[a-z][a-z0-9]{1,15}$' THEN
    RAISE EXCEPTION '모듈 이름은 영소문자로 시작하는 2~16자 [a-z0-9]: %', p_module;
  END IF;

  -- 계정: CDC(복제) + 자기 스키마 쓰기. 공통 읽기(tsdb · 레거시)는 svc_reader 로 받는다
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = s) THEN
    EXECUTE format('CREATE ROLE %I LOGIN REPLICATION PASSWORD %L', s, s);
  END IF;
  EXECUTE format('GRANT svc_reader TO %I', s);

  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', s);
  EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I, rfc_provider, hotdb_monitor', s, s);

  -- 장비별 최신 상태 1행 — 장비 마스터(tsdb.device)에 매달린다
  EXECUTE format($t$
    CREATE TABLE IF NOT EXISTS %1$I.device_status_current (
      site                  text NOT NULL,
      device_id             text PRIMARY KEY,
      status                text NOT NULL,
      error_code            text,
      last_heartbeat_at     timestamptz,
      scan_rate_pts_per_sec integer,
      temperature_c         real,
      connectivity_rssi     smallint,
      fov_mode              text,
      src_event_time        timestamptz NOT NULL,
      src_received_at       timestamptz NOT NULL,
      applied_at            timestamptz NOT NULL,
      FOREIGN KEY (site, device_id) REFERENCES tsdb.device (site, device_id)
    )$t$, s);
  EXECUTE format('CREATE INDEX IF NOT EXISTS device_status_current_applied_at_idx ON %I.device_status_current (applied_at)', s);

  -- 상태가 바뀐 순간만 — 최신 상태 행의 이력
  EXECUTE format($t$
    CREATE TABLE IF NOT EXISTS %1$I.device_status_change (
      device_id       text        NOT NULL REFERENCES %1$I.device_status_current (device_id)
                                  DEFERRABLE INITIALLY DEFERRED,
      changed_at      timestamptz NOT NULL,
      status          text        NOT NULL,
      error_code      text,
      src_received_at timestamptz NOT NULL,
      applied_at      timestamptz NOT NULL,
      PRIMARY KEY (device_id, changed_at)
    )$t$, s);

  -- 스캔 1건 (START → PROGRESS … → COMPLETE). 실적 · 산출물의 부모
  EXECUTE format($t$
    CREATE TABLE IF NOT EXISTS %1$I.scan (
      scan_id             uuid PRIMARY KEY,
      site                text        NOT NULL,
      device_id           text        NOT NULL,
      hull_no             text        NOT NULL,
      block_id            text        NOT NULL,
      first_event_at      timestamptz NOT NULL,
      last_event_at       timestamptz NOT NULL,
      last_event_type     text        NOT NULL,   -- START · PROGRESS · COMPLETE · PENDING(산출물이 먼저 와서 만든 자리)
      block_progress_rate real,
      match_confidence    real,
      reference_cad_id    text,
      model_version       text,
      completed_at        timestamptz,
      src_received_at     timestamptz NOT NULL,
      applied_at          timestamptz NOT NULL,
      FOREIGN KEY (site, device_id) REFERENCES tsdb.device (site, device_id)
    )$t$, s);
  EXECUTE format('CREATE INDEX IF NOT EXISTS scan_hull_no_block_id_idx ON %I.scan (hull_no, block_id)', s);
  EXECUTE format('CREATE INDEX IF NOT EXISTS scan_device_idx ON %I.scan (device_id)', s);

  -- 실적 판별 대상 (COMPLETE 된 스캔)
  EXECUTE format($t$
    CREATE TABLE IF NOT EXISTS %1$I.actual_result (
      hull_no             text        NOT NULL,
      block_id            text        NOT NULL,
      scan_id             uuid        NOT NULL REFERENCES %1$I.scan (scan_id) DEFERRABLE INITIALLY DEFERRED,
      device_id           text        NOT NULL,
      completed_at        timestamptz NOT NULL,
      block_progress_rate real,
      match_confidence    real,
      reference_cad_id    text,
      model_version       text,
      judged_status       text        NOT NULL DEFAULT 'PENDING',
      src_received_at     timestamptz NOT NULL,
      applied_at          timestamptz NOT NULL,
      PRIMARY KEY (hull_no, block_id, scan_id)
    )$t$, s);
  EXECUTE format('CREATE INDEX IF NOT EXISTS actual_result_scan_idx ON %I.actual_result (scan_id)', s);

  -- 산출물 위치 — 스캔의 자식
  EXECUTE format($t$
    CREATE TABLE IF NOT EXISTS %1$I.artifact (
      scan_id               uuid        NOT NULL REFERENCES %1$I.scan (scan_id) DEFERRABLE INITIALLY DEFERRED,
      artifact_type         text        NOT NULL,
      segment_key           text        NOT NULL,
      device_id             text        NOT NULL,
      hull_no               text        NOT NULL,
      block_id              text        NOT NULL,
      storage_uri           text,
      file_size_bytes       bigint,
      checksum              text,
      transformation_matrix real[],
      produced_by_device_id text,
      model_version         text,
      src_event_time        timestamptz NOT NULL,
      src_received_at       timestamptz NOT NULL,
      applied_at            timestamptz NOT NULL,
      PRIMARY KEY (scan_id, artifact_type, segment_key)
    )$t$, s);
  EXECUTE format('CREATE INDEX IF NOT EXISTS artifact_applied_at_idx ON %I.artifact (applied_at)', s);

  -- 권한: 주인만 쓰고, RFC Service(CDC) · 모니터는 읽는다
  EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO %I', s, s);
  EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO rfc_provider, hotdb_monitor', s);
  EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', s, s);
  EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO rfc_provider, hotdb_monitor', s);

  -- SAP 로 가는 것은 실적뿐 (V6 의 결정) — 모듈의 actual_result 만 svc_cdc_pub 에 싣는다
  IF NOT EXISTS (SELECT 1 FROM pg_publication_tables
                  WHERE pubname = 'svc_cdc_pub' AND schemaname = s AND tablename = 'actual_result') THEN
    EXECUTE format('ALTER PUBLICATION svc_cdc_pub ADD TABLE %I.actual_result', s);
  END IF;

  INSERT INTO ops.module (module, schema_name, zone_code, display_name)
  VALUES (p_module, s, p_zone_code, p_display)
  ON CONFLICT (module) DO UPDATE SET zone_code = EXCLUDED.zone_code, display_name = EXCLUDED.display_name;
END
$fn$;

-- 모듈 걷어내기 (시험 · 폐기용). 슬롯이 남아 있으면 WAL 을 붙잡으므로 같이 지운다.
CREATE FUNCTION ops.drop_module(p_module text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE
  s text := 'svc_' || p_module;
BEGIN
  PERFORM pg_drop_replication_slot(slot_name) FROM pg_replication_slots
   WHERE slot_name = 'zone_' || p_module AND NOT active;
  EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', s);
  DELETE FROM ops.module WHERE module = p_module;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = s) THEN
    EXECUTE format('DROP OWNED BY %I', s);
    EXECUTE format('DROP ROLE %I', s);
  END IF;
END
$fn$;

-- 모듈별 반영 지연 (DB 시계). postgres_exporter 가 모듈 수와 상관없이 이 함수 하나를 부른다.
CREATE FUNCTION ops.module_apply_lag()
RETURNS TABLE (zone text, rows_1m bigint, p50_seconds double precision, p99_seconds double precision,
               max_seconds double precision)
LANGUAGE plpgsql STABLE AS $fn$
DECLARE m record;
BEGIN
  FOR m IN SELECT module, schema_name FROM ops.module ORDER BY module LOOP
    RETURN QUERY EXECUTE format($q$
      SELECT %L::text, count(*),
             coalesce(percentile_cont(0.5)  WITHIN GROUP (ORDER BY l), 0),
             coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY l), 0),
             coalesce(max(l), 0)
        FROM (SELECT extract(epoch FROM applied_at - src_received_at)::float8 AS l
                FROM %I.device_status_current WHERE applied_at > now() - interval '1 minute') x
      $q$, m.module, m.schema_name);
  END LOOP;
END
$fn$;

-- ── 1단계의 네 권역을 모듈로 다시 만든다 ─────────────────────────────────
-- V3 의 표에는 관계(FK)와 site 가 없다. 지금 들어 있는 것은 발행기 · 시험 데이터뿐이라 지우고 새로 만든다.
-- (운영 데이터가 생긴 뒤에는 이렇게 하지 않는다 — ALTER 로 옮기는 V 파일을 쓴다)
DROP SCHEMA svc_asm, svc_oft, svc_pnt, svc_mch CASCADE;

SELECT ops.provision_module('mch', 'MACHINING',  '가공 실적 판별');
SELECT ops.provision_module('asm', 'ASSEMBLY',   '조립 실적 판별');
SELECT ops.provision_module('oft', 'OUTFITTING', '의장 실적 판별');
SELECT ops.provision_module('pnt', 'PAINTING',   '도장 실적 판별');
