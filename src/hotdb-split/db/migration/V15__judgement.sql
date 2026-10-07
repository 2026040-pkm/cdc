-- [실적 판별] svc.actual_result 의 judged_status 를 판별 모듈이 채운다 (2026-10-07).
--
--   흐름  tsdb ─CDC─▶ 판별 모듈(라우트) ─▶ actual_result (PENDING)
--                     판별 모듈(판별 단계) ─ PENDING 을 집어 규칙 적용 ─▶ CONFIRMED · REJECTED + judged_at
--   CDC 반영과 판별은 같은 프로세스의 다른 스레드다. 판별이 느리거나 실패해도 슬롯은 밀리지 않는다.
--   라우트는 judged_status 를 keep 으로 써서, 같은 실적이 다시 와도 판정을 되돌리지 않는다.
--   판정이 바뀌면 RFC Service 가 (실적 키 + 판정 상태) 새 키로 SAP 에 한 번 더 보낸다 (V6 ops.rfc_sent).

ALTER TABLE svc.actual_result ADD COLUMN judged_at timestamptz;

-- 판별 단계가 매 주기 훑는 자리 — 판정이 끝난 행은 빠지므로 작게 유지된다
CREATE INDEX actual_result_pending ON svc.actual_result (module, completed_at) WHERE judged_status = 'PENDING';
