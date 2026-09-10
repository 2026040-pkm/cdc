-- ─────────────────────────────────────────────────────────────────────────────
-- embedded-cdc(Debezium) 가 이 DB 를 원천으로 읽기 위한 설정
--
-- 먼저 embedded-cdc 의 원천 스키마(docker-compose.db.yml 이 /embedded-cdc-source 로 마운트)를
-- 그대로 읽어 car · computer · grade · member · cdc_heartbeat 와 cdc_user, publication 을 만든다.
-- 그 다음 lidar 스택의 일반 테이블 네 개를 그 publication 에 더한다.
-- (lidar_tag_catalog 는 InterSysLink 태그 등록부의 사본이다. 자주 바뀌지 않지만 수신 측에서
--  숫자 tid 를 장비로 되돌리려면 같이 있어야 해서 넣는다.)
--
-- 하이퍼테이블 셋(lidar_status · lidar_scan_actual · lidar_scan_artifact)은 넣지 않는다.
-- 청크가 주기마다 새 테이블로 생기고, 압축은 행을 내부 압축 청크로 옮기며, 보존은 청크째
-- DROP 한다 — 어느 것도 행 단위 INSERT/UPDATE/DELETE 로 보이지 않아 CDC 가 성립하지 않는다
-- (docs/timescaledb-cdc-impact.html B안). 그 데이터는 lidar-ingest 가 직접 적재한다.
--
-- ⚠️ replication slot 은 만료되지 않는다. cdc-service 가 오래 죽어 있으면 슬롯이 붙잡은
--    WAL 이 계속 쌓여 이 DB 디스크가 고갈된다. embedded-cdc 를 안 쓸 때는 슬롯을 지운다:
--    SELECT pg_drop_replication_slot('embedded_cdc_slot');
-- ─────────────────────────────────────────────────────────────────────────────

\i /embedded-cdc-source/01-schema.sql

-- UPDATE/DELETE 의 before 이미지에 전체 컬럼을 담는다 (embedded-cdc 의 네 표와 같은 이유)
ALTER TABLE lidar_device_state   REPLICA IDENTITY FULL;
ALTER TABLE lidar_status_message REPLICA IDENTITY FULL;
ALTER TABLE lidar_ingest_reject  REPLICA IDENTITY FULL;
ALTER TABLE lidar_tag_catalog    REPLICA IDENTITY FULL;

-- 최초 snapshot 읽기 권한
GRANT SELECT ON lidar_device_state, lidar_status_message, lidar_ingest_reject, lidar_tag_catalog TO cdc_user;

ALTER PUBLICATION embedded_cdc_pub
    ADD TABLE lidar_device_state, lidar_status_message, lidar_ingest_reject, lidar_tag_catalog;
