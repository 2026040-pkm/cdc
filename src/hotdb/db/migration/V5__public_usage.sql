-- V1 의 REVOKE ALL ON SCHEMA public 이 USAGE 까지 막아, public 에 있는 TimescaleDB 함수
-- (hypertable_size · time_bucket …)를 관리자 외에는 부를 수 없었다. 막을 것은 CREATE 뿐이다.
-- (V1 은 이미 적용됐으므로 고치지 않고 이 파일로 바로잡는다 — Flyway 체크섬 규칙)
GRANT USAGE ON SCHEMA public TO PUBLIC;
