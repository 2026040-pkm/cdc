-- [CDC 안전장치] 이전 Java 판(embedded-cdc · latest-poc, 검증 V1~V6)에 있던 것을 옮긴다.
--
--   ① ops.cdc_checkpoint — 소비자가 어디까지 처리했는지 DB 에도 남긴다.
--      Debezium 오프셋은 서비스 볼륨(zone-*-data)에 있어서 볼륨과 슬롯이 같이 사라지면
--      Debezium 은 "처음 기동"으로 보고 새 슬롯을 만든다 — 그 사이 변경은 조용히 빠진다.
--      이 기록이 있으면 기동 때 슬롯과 대조해 되받을 수 없는 구간(캡처 갭)을 잡는다.
--   ② ops.cdc_dead_letter 에 상태 — 원인을 고친 뒤 RETRY_REQUESTED 로 표시한 건만 다시 반영한다.

CREATE TABLE ops.cdc_checkpoint (
  pipeline          text        PRIMARY KEY,
  slot_name         text        NOT NULL,
  last_applied_lsn  pg_lsn      NOT NULL,   -- 처리를 끝낸 위치 (거꾸로 가지 않는다)
  updated_at        timestamptz NOT NULL
);
GRANT SELECT, INSERT, UPDATE ON ops.cdc_checkpoint TO svc_reader, rfc_provider;
GRANT SELECT ON ops.cdc_checkpoint TO hotdb_monitor;

-- status: PENDING(격리됨) · RETRY_REQUESTED(사람이 재처리 신청) · RESOLVED(재처리 끝) · DISCARDED(버리기로 함)
-- resolution: APPLIED(행이 바뀜) · STALE_SKIPPED(더 새 값이 이미 있어 0행 — 복구 끝으로 본다)
ALTER TABLE ops.cdc_dead_letter
  ADD COLUMN status      text        NOT NULL DEFAULT 'PENDING'
                         CHECK (status IN ('PENDING', 'RETRY_REQUESTED', 'RESOLVED', 'DISCARDED')),
  ADD COLUMN attempts    int         NOT NULL DEFAULT 1,
  ADD COLUMN resolution  text,
  ADD COLUMN last_error  text,
  ADD COLUMN resolved_at timestamptz;
CREATE INDEX ON ops.cdc_dead_letter (pipeline, status, id);
-- 재처리기가 자기 파이프라인 건의 상태를 바꾼다. 사람의 재처리 신청은 postgres 로 한다
GRANT UPDATE (status, attempts, resolution, last_error, resolved_at) ON ops.cdc_dead_letter TO svc_reader, rfc_provider;

-- 모듈을 걷어낼 때 체크포인트도 지운다 — 남겨 두면 같은 이름으로 다시 만들 때 캡처 갭으로 오인한다
CREATE OR REPLACE FUNCTION ops.drop_module(p_module text)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE r text := 'svc_' || p_module;
BEGIN
  PERFORM pg_drop_replication_slot(slot_name) FROM pg_replication_slots
   WHERE slot_name = 'zone_' || p_module AND NOT active;
  DELETE FROM ops.cdc_checkpoint WHERE pipeline = 'zone-' || p_module;
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
