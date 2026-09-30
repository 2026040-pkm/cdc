-- [CDC] 서비스 생성 RDB(svc) → RFC Service.
--
-- FOR TABLES IN SCHEMA 라 svc 에 나중에 만드는 테이블도 자동으로 실린다.
-- FOR ALL TABLES 를 쓰지 않는 이유: 그러면 tsdb 의 청크 · 압축 내부 테이블까지 실리고,
-- 그 publication 이 있는 동안 새 하이퍼테이블 · 연속 집계를 만들 수 없다
-- (2026-09-17 스파이크 실측, docs/cdc/timescaledb-cdc-impact.html B4).
--
-- 복제 슬롯은 여기서 만들지 않는다. 소비자가 붙지 않은 슬롯은 WAL 을 계속 붙잡는다 —
-- RFC Service(CDC 클라이언트)가 처음 접속할 때 pgoutput 슬롯을 만든다.
CREATE PUBLICATION svc_cdc_pub FOR TABLES IN SCHEMA svc;
