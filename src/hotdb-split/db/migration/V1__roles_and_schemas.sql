-- HotDB 한 인스턴스 안의 영역과 계정.
--
--   tsdb                     : 필드 데이터      — HotDB Provider 만 쓴다 (계정 hotdb_provider)
--   svc_asm/oft/pnt/mch      : 권역별 RDB       — 그 권역 서비스만 쓴다 (계정 svc_asm …)
--   ops                      : CDC 운영 표      — heartbeat · dead letter · flyway 이력
--   레거시 사본 : 별도 레거시 DB (db/legacy-db, V10 참고)
--
-- 원칙: 영역마다 쓰는 주체는 하나다. 남의 영역에는 쓰지 못하게 계정으로 막는다.
-- 비밀번호는 로컬 개발용이다. 서버에 올릴 때는 ALTER ROLE 로 바꾼다.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS tsdb;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA svc_asm;   -- 조립
CREATE SCHEMA svc_oft;   -- 의장
CREATE SCHEMA svc_pnt;   -- 도장
CREATE SCHEMA svc_mch;   -- 가공

REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- 필드 데이터 적재 (현재는 field-simulator 가 이 계정으로 대신 쓴다)
CREATE ROLE hotdb_provider LOGIN PASSWORD 'hotdb_provider';

-- 권역 서비스: 복제 접속(CDC) + 자기 스키마 쓰기 + tsdb · 레거시 읽기
CREATE ROLE svc_asm LOGIN REPLICATION PASSWORD 'svc_asm';
CREATE ROLE svc_oft LOGIN REPLICATION PASSWORD 'svc_oft';
CREATE ROLE svc_pnt LOGIN REPLICATION PASSWORD 'svc_pnt';
CREATE ROLE svc_mch LOGIN REPLICATION PASSWORD 'svc_mch';
CREATE ROLE svc_reader NOLOGIN;   -- 권역 서비스 공통 읽기 권한 묶음
GRANT svc_reader TO svc_asm, svc_oft, svc_pnt, svc_mch;

-- 레거시 유입 (3단계). 계정만 먼저 만든다
CREATE ROLE rfc_agent LOGIN PASSWORD 'rfc_agent';   -- SAP 폴링
CREATE ROLE db_agent  LOGIN PASSWORD 'db_agent';    -- Oracle 폴링
-- svc_* → SAP (3단계)
CREATE ROLE rfc_provider LOGIN REPLICATION PASSWORD 'rfc_provider';

-- 모니터링 (postgres_exporter)
CREATE ROLE hotdb_monitor LOGIN PASSWORD 'hotdb_monitor';
GRANT pg_monitor TO hotdb_monitor;

-- tsdb: Provider 는 적재 · 중복 판정 조회, 서비스는 읽기만.
-- 하이퍼테이블에 건 권한은 청크에 그대로 복사된다 — CDC 계정이 청크를 따로 받을 필요 없다.
GRANT USAGE ON SCHEMA tsdb TO hotdb_provider, svc_reader, hotdb_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA tsdb GRANT SELECT, INSERT ON TABLES TO hotdb_provider;
ALTER DEFAULT PRIVILEGES IN SCHEMA tsdb GRANT SELECT ON TABLES TO svc_reader, hotdb_monitor;

-- ops: heartbeat 는 각 CDC 소비자가 쓰고, dead letter 는 서비스가 쓴다
GRANT USAGE ON SCHEMA ops TO svc_reader, rfc_provider, hotdb_monitor;

-- 권역 스키마: 주인만 쓴다. RFC Provider(CDC) · 모니터는 읽기
DO $$
DECLARE s text;
BEGIN
  FOREACH s IN ARRAY ARRAY['svc_asm','svc_oft','svc_pnt','svc_mch'] LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I, rfc_provider, hotdb_monitor', s, s);
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', s, s);
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT USAGE, SELECT ON SEQUENCES TO %I', s, s);
    EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO rfc_provider, hotdb_monitor', s);
  END LOOP;
END $$;
