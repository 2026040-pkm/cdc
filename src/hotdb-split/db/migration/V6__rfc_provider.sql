-- [2단 CDC] 권역 RDB(svc_*) → RFC Provider → SAP.
--
-- 1) svc_cdc_pub 를 실적 표로 좁힌다.
--    V4 는 svc_* 스키마째 실었는데, 그러면 device_status_current 의 초당 갱신(장비 수만큼, ≈ 350/s)이
--    전부 RFC Provider 로 흘러가 버려진다. SAP 로 가는 것은 실적뿐이다.
--    표를 더 보내야 하면 여기에 V 파일로 더한다 (스키마 단위 자동 편입은 포기한다).
ALTER PUBLICATION svc_cdc_pub SET TABLE
  ops.cdc_heartbeat,
  svc_asm.actual_result, svc_oft.actual_result, svc_pnt.actual_result, svc_mch.actual_result;

-- 2) 보낸 기록. 같은 실적 · 같은 판정 상태는 한 번만 보낸다.
--    CDC 는 at-least-once 이고, 권역 서비스의 UPSERT 는 재전송에도 행을 갱신해(applied_at) UPDATE 를 또 낸다 —
--    이 표의 PK 가 중복 송신을 막는다. 판정 상태가 바뀌면(PENDING → CONFIRMED) 새 행이라 다시 보낸다.
CREATE TABLE ops.rfc_sent (
  zone            text        NOT NULL,   -- asm · oft · pnt · mch (원천 스키마에서)
  hull_no         text        NOT NULL,
  block_id        text        NOT NULL,
  scan_id         uuid        NOT NULL,
  judged_status   text        NOT NULL,
  src_received_at timestamptz NOT NULL,   -- Provider 수신 시각 (tsdb received_at) — sent_at 과 빼면 전 구간 지연
  src_applied_at  timestamptz NOT NULL,   -- 권역 RDB 반영 시각 (svc_*.actual_result.applied_at)
  src_lsn         bigint,
  sent_at         timestamptz NOT NULL DEFAULT clock_timestamp(),   -- sent_at - src_applied_at = 2단 지연 (DB 시계)
  PRIMARY KEY (zone, hull_no, block_id, scan_id, judged_status)
);
CREATE INDEX ON ops.rfc_sent (sent_at);
GRANT SELECT, INSERT ON ops.rfc_sent TO rfc_provider;
GRANT SELECT ON ops.rfc_sent TO hotdb_monitor;
