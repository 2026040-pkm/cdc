-- [판별 모듈 RDB 를 스키마 하나로] svc_mch · svc_asm · svc_oft · svc_pnt → svc (+ module 컬럼)
--
-- 네 모듈이 표 모양이 같고 SAP 로도 한데 모여 가므로, 표 다섯 개를 모듈이 같이 쓰고 module 컬럼으로 가른다.
-- 모듈 사이 쓰기 격리는 스키마 권한 대신 행 보안(RLS)이 맡는다:
--   계정 svc_<모듈> 은 module = '<모듈>' 인 행만 보고 · 넣고 · 고친다 (정책 module_own).
--   RFC Service(rfc_provider) · 모니터(hotdb_monitor)는 전부 읽는다 (정책 read_all).
-- 테이블 주인(postgres)은 RLS 를 받지 않으므로 FK 검사 · 마이그레이션은 그대로 돈다.
-- CDC(논리 디코딩)에는 RLS 가 걸리지 않는다 — RFC Service 는 모든 모듈의 실적을 그대로 받는다.
--
-- 모듈 추가는 여전히 한 줄:  SELECT ops.provision_module('cut', 'CUTTING', '절단 실적 판별');
-- (이제 스키마 · 표를 만들지 않고 계정 · 등록부만 만든다)

-- ── 옛 모듈 스키마 걷어내기 (시험 데이터뿐) ───────────────────────────────
DROP SCHEMA svc_asm, svc_oft, svc_pnt, svc_mch CASCADE;     -- svc_cdc_pub 에서도 같이 빠진다
DROP FUNCTION ops.provision_module(text, text, text);
DROP FUNCTION ops.drop_module(text);
DROP FUNCTION ops.module_apply_lag();
ALTER TABLE ops.module DROP COLUMN schema_name;

-- 모듈 계정 묶음 — RLS 정책이 이 역할을 대상으로 한다
CREATE ROLE svc_writer NOLOGIN;
GRANT svc_writer TO svc_asm, svc_oft, svc_pnt, svc_mch;

-- ── svc ─────────────────────────────────────────────────────────────────
CREATE SCHEMA svc;
GRANT USAGE ON SCHEMA svc TO svc_writer, rfc_provider, hotdb_monitor;

-- 장비별 최신 상태 1행 · 부모 tsdb.device
CREATE TABLE svc.device_status_current (
  module                text        NOT NULL REFERENCES ops.module (module),
  site                  text        NOT NULL,
  device_id             text        PRIMARY KEY,
  status                text        NOT NULL,
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
);
CREATE INDEX ON svc.device_status_current (module, applied_at);

-- 상태가 바뀐 순간만 · 부모 device_status_current
CREATE TABLE svc.device_status_change (
  module          text        NOT NULL REFERENCES ops.module (module),
  device_id       text        NOT NULL REFERENCES svc.device_status_current (device_id) DEFERRABLE INITIALLY DEFERRED,
  changed_at      timestamptz NOT NULL,
  status          text        NOT NULL,
  error_code      text,
  src_received_at timestamptz NOT NULL,
  applied_at      timestamptz NOT NULL,
  PRIMARY KEY (device_id, changed_at)
);

-- 스캔 1건 · 부모 tsdb.device · 실적 · 산출물의 부모
CREATE TABLE svc.scan (
  module              text        NOT NULL REFERENCES ops.module (module),
  scan_id             uuid        PRIMARY KEY,
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
);
CREATE INDEX ON svc.scan (module, hull_no, block_id);
CREATE INDEX ON svc.scan (device_id);

-- 실적 판별 대상 (COMPLETE) · 부모 scan
CREATE TABLE svc.actual_result (
  module              text        NOT NULL REFERENCES ops.module (module),
  hull_no             text        NOT NULL,
  block_id            text        NOT NULL,
  scan_id             uuid        NOT NULL REFERENCES svc.scan (scan_id) DEFERRABLE INITIALLY DEFERRED,
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
);
CREATE INDEX ON svc.actual_result (scan_id);
CREATE INDEX ON svc.actual_result (module, completed_at);

-- 산출물 위치 · 부모 scan
CREATE TABLE svc.artifact (
  module                text        NOT NULL REFERENCES ops.module (module),
  scan_id               uuid        NOT NULL REFERENCES svc.scan (scan_id) DEFERRABLE INITIALLY DEFERRED,
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
);
CREATE INDEX ON svc.artifact (module, applied_at);

-- ── 권한 · 행 보안 ───────────────────────────────────────────────────────
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA svc TO svc_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA svc TO rfc_provider, hotdb_monitor;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['device_status_current','device_status_change','scan','actual_result','artifact'] LOOP
    EXECUTE format('ALTER TABLE svc.%I ENABLE ROW LEVEL SECURITY', t);
    -- 계정 svc_asm → module 'asm'. 다른 모듈 행은 보이지도, 써지지도 않는다
    EXECUTE format($p$CREATE POLICY module_own ON svc.%I FOR ALL TO svc_writer
                     USING (module = substr(current_user, 5)) WITH CHECK (module = substr(current_user, 5))$p$, t);
    EXECUTE format('CREATE POLICY read_all ON svc.%I FOR SELECT TO rfc_provider, hotdb_monitor USING (true)', t);
  END LOOP;
END $$;

-- SAP 로 가는 것은 실적뿐 (V6 의 결정 그대로) — 이제 표 하나
ALTER PUBLICATION svc_cdc_pub SET TABLE ops.cdc_heartbeat, svc.actual_result;

-- ── 모듈 함수 (계정 · 등록부만) ──────────────────────────────────────────
CREATE FUNCTION ops.provision_module(p_module text, p_zone_code text, p_display text)
RETURNS void LANGUAGE plpgsql AS $fn$
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
$fn$;

-- 모듈 걷어내기 — 그 모듈의 행 · 슬롯 · 계정 · 등록
CREATE FUNCTION ops.drop_module(p_module text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE r text := 'svc_' || p_module;
BEGIN
  PERFORM pg_drop_replication_slot(slot_name) FROM pg_replication_slots
   WHERE slot_name = 'zone_' || p_module AND NOT active;
  SET CONSTRAINTS ALL DEFERRED;
  DELETE FROM svc.artifact WHERE module = p_module;
  DELETE FROM svc.actual_result WHERE module = p_module;
  DELETE FROM svc.scan WHERE module = p_module;
  DELETE FROM svc.device_status_change WHERE module = p_module;
  DELETE FROM svc.device_status_current WHERE module = p_module;
  DELETE FROM ops.module WHERE module = p_module;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
    EXECUTE format('DROP OWNED BY %I', r);
    EXECUTE format('DROP ROLE %I', r);
  END IF;
END
$fn$;

-- 모듈별 반영 지연 (DB 시계) — 등록된 모듈은 데이터가 없어도 한 줄씩 나온다
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
      SELECT extract(epoch FROM c.applied_at - c.src_received_at)::float8 AS lag
        FROM svc.device_status_current c
       WHERE c.module = m.module AND c.applied_at > now() - interval '1 minute') l ON true
   GROUP BY m.module
$fn$;
