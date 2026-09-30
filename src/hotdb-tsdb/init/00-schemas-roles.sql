-- 한 인스턴스(PG 17 + TimescaleDB + logical WAL) 안의 세 영역.
--
--   legacy : 레거시 RDB     — RFC Service 가 SAP 를 폴링해 넣는다 (유입)
--   svc    : 서비스 생성 RDB — 권역별 서비스가 저장 · 조회, RFC Service 로 CDC (유출)
--   tsdb   : 필드 데이터     — Hot DB Provider 가 저장
--
-- 영역마다 쓰는 주체가 하나다. 계정도 그 주체 단위로 나눠 남의 영역에 쓰지 못하게 한다.
-- 비밀번호는 로컬 개발용이다.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA legacy;
CREATE SCHEMA svc;
CREATE SCHEMA tsdb;

-- RFC Service (유입): legacy 에만 쓴다
CREATE ROLE rfc_ingest     LOGIN PASSWORD 'rfc_ingest';
-- 권역별 서비스: svc 에 테이블을 만들고 쓰고, legacy · tsdb 는 읽기만
CREATE ROLE svc_app        LOGIN PASSWORD 'svc_app';
-- Hot DB Provider: tsdb 에 적재만
CREATE ROLE hotdb_provider LOGIN PASSWORD 'hotdb_provider';
-- RFC Service (CDC): 복제 접속 + svc 초기 스냅샷용 읽기
CREATE ROLE svc_cdc        LOGIN REPLICATION PASSWORD 'svc_cdc';

REVOKE ALL ON SCHEMA public FROM PUBLIC;

GRANT USAGE ON SCHEMA legacy TO rfc_ingest, svc_app;
GRANT USAGE ON SCHEMA svc    TO svc_app, svc_cdc;
GRANT CREATE ON SCHEMA svc   TO svc_app;
GRANT USAGE ON SCHEMA tsdb   TO hotdb_provider, svc_app;

-- 앞으로 만들어질 테이블에도 같은 권한이 붙도록 기본 권한으로 건다.
-- legacy · tsdb 는 관리자(postgres)가 DDL 을 돌리고, svc 는 관리자 또는 svc_app 이 만든다.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA legacy
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO rfc_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA legacy
  GRANT SELECT ON TABLES TO svc_app;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA legacy
  GRANT USAGE, SELECT ON SEQUENCES TO rfc_ingest;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA svc
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO svc_app;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA svc
  GRANT USAGE, SELECT ON SEQUENCES TO svc_app;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA svc
  GRANT SELECT ON TABLES TO svc_cdc;
ALTER DEFAULT PRIVILEGES FOR ROLE svc_app IN SCHEMA svc
  GRANT SELECT ON TABLES TO svc_cdc;

-- Provider 는 적재 · 중복 판정 조회만. 이력 정리는 보관 정책(drop_chunks)이 한다.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA tsdb
  GRANT SELECT, INSERT ON TABLES TO hotdb_provider;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA tsdb
  GRANT SELECT ON TABLES TO svc_app;
