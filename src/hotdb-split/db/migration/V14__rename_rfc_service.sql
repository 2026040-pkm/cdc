-- [이름 정리] rfc-provider → rfc-service. 그림의 RFC Service(O-8) — SAP 폴링과 실적 2단 CDC 를 함께 하는 서비스
-- 이름을 하나로 맞춘다 (2026-10-07). 모듈 · 컨테이너 · 지표 이름은 코드에서, DB 쪽은 여기서 바꾼다.
--
--   계정    rfc_provider → rfc_service  (권한 · 행 보안 정책은 역할 OID 로 묶여 있어 그대로 따라온다)
--   슬롯    rfc_provider 를 지운다 — 슬롯은 이름을 바꿀 수 없다. rfc-service 가 기동하며 rfc_service 를 새로 만들고
--           실적 표를 처음부터 다시 읽는다(snapshot initial). 이미 보낸 실적은 ops.rfc_sent 가 걸러 SAP 에 다시 가지 않는다
--   운영 행 heartbeat · dead letter 의 pipeline 이름을 바꾸고, 처리 위치(checkpoint)는 지운다 — 새 슬롯이라
--           옛 위치와 대조하면 연속성 점검(SlotContinuityGuard)이 캡처 갭으로 본다
--
-- 순서: rfc-service(옛 rfc-provider)를 먼저 내린다. 슬롯을 쓰고 있으면 이 마이그레이션은 멈춘다.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'rfc_provider' AND active) THEN
    RAISE EXCEPTION '슬롯 rfc_provider 를 아직 쓰고 있다 — rfc-provider 컨테이너를 내린 뒤 다시 돌린다';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rfc_provider') THEN
    ALTER ROLE rfc_provider RENAME TO rfc_service;
  END IF;
END $$;
-- 이름을 바꾸면 MD5 비밀번호는 지워진다 (SCRAM 은 남지만 이름과 맞춰 다시 건다). 로컬 개발용 값
ALTER ROLE rfc_service PASSWORD 'rfc_service';

SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'rfc_provider';

UPDATE ops.cdc_heartbeat   SET pipeline = 'rfc-service' WHERE pipeline = 'rfc-provider';
UPDATE ops.cdc_dead_letter SET pipeline = 'rfc-service' WHERE pipeline = 'rfc-provider';
DELETE FROM ops.cdc_checkpoint WHERE pipeline = 'rfc-provider';
