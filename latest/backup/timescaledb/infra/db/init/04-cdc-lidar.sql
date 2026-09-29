-- ─────────────────────────────────────────────────────────────────────────────
-- embedded-cdc(Debezium) 가 이 DB 를 원천으로 읽기 위한 설정
--
-- 먼저 embedded-cdc 의 원천 스키마(docker-compose.db.yml 이 /embedded-cdc-source 로 마운트)를
-- 그대로 읽어 car · computer · grade · member · cdc_heartbeat 와 cdc_user, publication 을 만든다.
-- 그 다음 lidar 스택의 일반 테이블 네 개를 그 publication 에 더한다.
-- (lidar_tag_catalog 는 InterSysLink 태그 등록부의 사본이다. 자주 바뀌지 않지만 수신 측에서
--  숫자 tid 를 장비로 되돌리려면 같이 있어야 해서 넣는다.)
--
-- 캡처 대상은 전부 rdb 스키마에 있다. 하이퍼테이블 셋(tsdb.lidar_status ·
-- tsdb.lidar_scan_actual · tsdb.lidar_scan_artifact)은 넣지 않는다. 청크가 주기마다 새
-- 테이블로 생기고, 압축은 행을 내부 압축 청크로 옮기며, 보존은 청크째 DROP 한다 — 어느 것도
-- 행 단위 INSERT/UPDATE/DELETE 로 보이지 않아 CDC 가 성립하지 않는다
-- (docs/cdc/timescaledb-cdc-impact.html B안). 그 데이터는 lidar-ingest 가 직접 적재한다.
-- 스키마를 나눈 것이 바로 그 선이다 — 01-schema.sql 의 "스키마 둘" 주석 참고.
--
-- ⚠️ replication slot 은 만료되지 않는다. cdc-service 가 오래 죽어 있으면 슬롯이 붙잡은
--    WAL 이 계속 쌓여 이 DB 디스크가 고갈된다. embedded-cdc 를 안 쓸 때는 슬롯을 지운다:
--    SELECT pg_drop_replication_slot('embedded_cdc_slot');
-- ─────────────────────────────────────────────────────────────────────────────

-- 공유 파일이라 손대지 않는다(embedded-cdc 스택이 자기 원천 DB 에도 그대로 쓴다).
-- 대신 세션 search_path 로 착지 지점을 바꾼다 — \i 는 같은 세션이므로 그 안의 수식 없는
-- CREATE TABLE · CREATE PUBLICATION 이 전부 rdb 에 만들어진다.
-- public 을 뒤에 남기는 것은 그 파일이 TimescaleDB 함수처럼 public 에 사는 것을 부를 때를 위해서다.
SET search_path = rdb, public;
\i /embedded-cdc-source/01-schema.sql
RESET search_path;

-- 그 파일의 GRANT USAGE 대상은 public 으로 하드코딩돼 있다(01-schema.sql:71).
-- 표가 rdb 로 옮겨 왔으므로 스키마를 따로 열어 준다. 없으면 snapshot 이 권한 오류로 죽는다.
GRANT USAGE ON SCHEMA rdb TO cdc_user;

-- UPDATE/DELETE 의 before 이미지에 전체 컬럼을 담는다 (embedded-cdc 의 네 표와 같은 이유)
ALTER TABLE rdb.lidar_device_state   REPLICA IDENTITY FULL;
ALTER TABLE rdb.lidar_status_message REPLICA IDENTITY FULL;
ALTER TABLE rdb.lidar_ingest_reject  REPLICA IDENTITY FULL;
ALTER TABLE rdb.lidar_tag_catalog    REPLICA IDENTITY FULL;
ALTER TABLE rdb.lidar_block_progress     REPLICA IDENTITY FULL;
ALTER TABLE rdb.lidar_block_artifact REPLICA IDENTITY FULL;

-- 최초 snapshot 읽기 권한
GRANT SELECT ON rdb.lidar_device_state, rdb.lidar_status_message,
                rdb.lidar_ingest_reject, rdb.lidar_tag_catalog,
                rdb.lidar_block_progress, rdb.lidar_block_artifact TO cdc_user;

-- 표를 열거하는 대신 ALTER PUBLICATION embedded_cdc_pub ADD TABLES IN SCHEMA rdb 로
-- 스키마째 실을 수도 있다(PG15+). 그러면 rdb 에 표가 늘어도 저절로 따라오고 하이퍼테이블은
-- 구조적으로 못 들어온다. 안 쓰는 이유는 소유권이다 — 이 publication 은 위 공유 파일에서
-- 이미 cdc_user(비 superuser) 소유로 넘어갔고, 스키마 단위 항목을 가진 publication 은
-- superuser 소유여야 한다. 열거로 두고, 표가 늘면 여기를 같이 고친다.
-- 채널마다 상태 표가 하나씩이고, 셀 다 축과 모양이 다르다.
--   status    lidar_device_state    장비 축(tid)         최신 측정치
--   actual    lidar_block_progress  블록 축(hull, block)  공정 마일스톤
--   artifact  lidar_block_artifact  블록 축(hull, block)  종류별 대장
-- 셀 다 행 수가 유계다(350 · 349 · 349). 원문이 쌓이는 하이퍼테이블은 못 싣지만
-- 그것을 유계 축으로 접은 상태 표는 일반 표라 실릴 수 있다. 수신 측이 세 채널을
-- 모두 보게 되는 것이 이 셋 덕분이다.
ALTER PUBLICATION embedded_cdc_pub
    ADD TABLE rdb.lidar_device_state, rdb.lidar_status_message,
              rdb.lidar_ingest_reject, rdb.lidar_tag_catalog,
              rdb.lidar_block_progress, rdb.lidar_block_artifact;
