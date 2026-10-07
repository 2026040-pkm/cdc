-- [레거시 분리] SAP · Oracle 사본은 별도 레거시 DB(compose.legacy.yml, db/legacy-db)로 옮긴다.
-- Hot DB 는 이제 필드 DB 다 — tsdb(필드 원본) · svc(판별 모듈 RDB) · ops(CDC 운영)만 남는다.
--
-- 폴링 상태 표와 레거시만 쓰던 계정을 지운다. 사본 표는 새 레거시 DB 에서 다음 폴링 주기에 다시 채워진다.
DROP TABLE IF EXISTS ops.poll_state;

-- 다른 객체 권한이 남아 있으면 DROP ROLE 이 실패하므로 먼저 걷는다
DROP OWNED BY rfc_agent, db_agent;
DROP ROLE rfc_agent;
DROP ROLE db_agent;
