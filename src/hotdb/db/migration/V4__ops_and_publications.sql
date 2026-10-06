-- [ops] CDC 운영 표와 publication.

-- 각 CDC 소비자가 heartbeat 로 한 행씩 갱신한다. 관심 표에 변경이 없어도 슬롯이 전진하게
-- publication 에 넣는다 (넣지 않으면 슬롯이 WAL 을 붙잡는다 — embedded-cdc V6 실측).
CREATE TABLE ops.cdc_heartbeat (
  pipeline text PRIMARY KEY,
  beat_at  timestamptz NOT NULL
);
GRANT SELECT, INSERT, UPDATE ON ops.cdc_heartbeat TO svc_reader, rfc_provider;
GRANT SELECT ON ops.cdc_heartbeat TO hotdb_monitor;

-- 재시도해도 반영되지 않은 이벤트. 원인을 고친 뒤 손으로 다시 넣는다.
CREATE TABLE ops.cdc_dead_letter (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  pipeline    text        NOT NULL,
  route       text        NOT NULL,
  source      text        NOT NULL,
  lsn         bigint,
  payload     jsonb       NOT NULL,
  error       text        NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON ops.cdc_dead_letter (pipeline, created_at);
GRANT SELECT, INSERT ON ops.cdc_dead_letter TO svc_reader, rfc_provider;
GRANT USAGE ON SEQUENCE ops.cdc_dead_letter_id_seq TO svc_reader, rfc_provider;
GRANT SELECT ON ops.cdc_dead_letter TO hotdb_monitor;

-- tsdb → 권역 서비스.
-- 하이퍼테이블 행은 부모가 아니라 청크(_timescaledb_internal._hyper_N_M_chunk)에 있으므로
-- 청크가 사는 스키마째로 싣는다. 청크는 인터벌마다 새로 생기는데 스키마 단위라 자동으로 실린다.
-- (2026-10-06 확인 · timescaledb 2.29.2/PG17: 새 청크 자동 편입, publication 이 있어도
--  새 하이퍼테이블 · 연속집계 생성 가능 — FOR ALL TABLES 와 달리 막히지 않는다.)
-- 함께 실리는 압축 청크(*_compressed) · bgw_* 표는 소비자의 table.include.list 정규식이 버린다.
CREATE PUBLICATION tsdb_cdc_pub
  FOR TABLE tsdb.device, tsdb.tag_catalog, ops.cdc_heartbeat,
      TABLES IN SCHEMA _timescaledb_internal;

-- 권역 RDB → RFC Provider (3단계). 스키마 단위라 나중에 만드는 표도 자동으로 실린다.
CREATE PUBLICATION svc_cdc_pub
  FOR TABLE ops.cdc_heartbeat,
      TABLES IN SCHEMA svc_asm, svc_oft, svc_pnt, svc_mch;

-- 복제 슬롯은 여기서 만들지 않는다. 소비자가 붙지 않은 슬롯은 WAL 을 계속 붙잡는다 —
-- 각 소비자(Debezium)가 처음 접속할 때 자기 슬롯을 만든다.
