-- [레거시 DB] Hot DB(필드 DB)와 나눈 별도 PostgreSQL — SAP · Oracle 사본만 둔다 (2026-10-06 분리).
--
--   erp       : SAP 사본       — RFC Service 폴링이 쓴다 (계정 rfc_agent)
--   mes · lgs · geo : Oracle 사본 — DB Agent 폴링이 쓴다 (계정 db_agent)
--   ops.poll_state  : 폴링 작업 상태 (워터마크 · 마지막 성공 · 오류)
--
-- 표는 V2 (scripts/gen-sample-legacy.py 가 만든 시험용 표 178개)가 만든다 — 아래 역할을 쓴다.
-- 비밀번호는 로컬 개발용이다.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE ROLE rfc_agent     LOGIN PASSWORD 'rfc_agent';      -- SAP → erp
CREATE ROLE db_agent      LOGIN PASSWORD 'db_agent';       -- Oracle → mes · lgs · geo
CREATE ROLE svc_reader    NOLOGIN;                         -- 판별 로직이 레거시를 참고할 때 쓸 읽기 묶음
CREATE ROLE hotdb_monitor LOGIN PASSWORD 'hotdb_monitor';  -- postgres_exporter
GRANT pg_monitor TO hotdb_monitor;

CREATE SCHEMA IF NOT EXISTS ops;
GRANT USAGE ON SCHEMA ops TO rfc_agent, db_agent, hotdb_monitor;

-- 작업(job) 하나 = 원천 표 하나 → 레거시 표 하나. 증분 작업은 마지막으로 읽은 워터마크를 남긴다
CREATE TABLE ops.poll_state (
  agent          text        NOT NULL,   -- rfc-service · db-agent
  job            text        NOT NULL,
  watermark      text,
  last_run_at    timestamptz,
  last_ok_at     timestamptz,
  last_rows      bigint,
  last_error     text,
  PRIMARY KEY (agent, job)
);
GRANT SELECT, INSERT, UPDATE ON ops.poll_state TO rfc_agent, db_agent;
GRANT SELECT ON ops.poll_state TO hotdb_monitor;
