-- [레거시 폴링] RFC Service(SAP 폴링) · DB Agent(Oracle 폴링) 의 진행 상태.
--
-- 작업(job) 하나 = 원천 표 하나 → HotDB 레거시 표 하나. 증분 작업은 마지막으로 읽은 워터마크
-- (예: upd_date || upd_time) 를 여기에 남기고, 다음 주기에 그 값 이상만 다시 읽는다.
-- 같은 워터마크의 행은 다시 읽히지만 PK UPSERT 라 결과가 같다.
CREATE TABLE ops.poll_state (
  agent          text        NOT NULL,   -- rfc-service · db-agent
  job            text        NOT NULL,   -- 설정의 작업 이름 (보통 대상 표 이름)
  watermark      text,                   -- 증분: 마지막으로 읽은 값. 전체 작업은 null
  last_run_at    timestamptz,
  last_ok_at     timestamptz,
  last_rows      bigint,
  last_error     text,
  PRIMARY KEY (agent, job)
);
GRANT SELECT, INSERT, UPDATE ON ops.poll_state TO rfc_agent, db_agent;
GRANT SELECT ON ops.poll_state TO hotdb_monitor;
GRANT USAGE ON SCHEMA ops TO rfc_agent, db_agent;
