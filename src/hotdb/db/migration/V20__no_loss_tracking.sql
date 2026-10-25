-- [누락 개선] 2026-10-25 · 계보 아티팩트 「개선안」 탭 F1 ~ F5
--   F1 앵커보다 센서 실적이 먼저 오면 UNMATCHED 로 끝나던 것 → 앵커를 기다리는 상태(WAIT_ANCHOR)로 두고 다시 판정
--   F2 실적 기준일 = 인식 시각의 날짜 (코드만)
--   F3 같은 상태에서 진척만 바뀌면 SAP 로 다시 안 보내던 것 → 송신 기록 키에 내용 해시
--   F4 레거시에서 지운 행이 남던 것 → l_* 8표에 적재 추적 열, 전체 작업이 안 온 행에 src_deleted_at
--   F5 수정 시각 없이 바뀐 행 → 증분 작업에 하루 1회 전체 재동기화 짝 (poll-jobs-hot.yml)

-- F1 앵커 기다림
ALTER TABLE hot.s_scan_detection
  ADD COLUMN IF NOT EXISTS match_attempts   int NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS match_wait_until timestamptz;
COMMENT ON COLUMN hot.s_scan_detection.match_attempts IS '앵커 찾기 횟수 (0 = 아직). V20';
COMMENT ON COLUMN hot.s_scan_detection.match_wait_until IS '앵커를 기다리는 끝 시각 — judge_note WAIT_ANCHOR 일 때. 지나면 UNMATCHED 확정. V20';
CREATE INDEX IF NOT EXISTS s_scan_detection_wait_idx ON hot.s_scan_detection (hull_no, block_no)
  WHERE judge_note = 'WAIT_ANCHOR';
UPDATE hot.m_code SET attrs = attrs || '{"anchor_wait_days": 7}'::jsonb
 WHERE code_group = 'TRADE_RULE' AND code IN ('S', 'W', 'P');      -- 기본값. 운영 사본 갱신 주기 · W/O 발행 지연을 보고 조정

-- F3 송신 기록 키 = (앵커, 기준일, 내용 해시)
ALTER TABLE ops.rfc_progress_sent ADD COLUMN IF NOT EXISTS payload_hash text;
UPDATE ops.rfc_progress_sent SET payload_hash = state WHERE payload_hash IS NULL;   -- 기존 행은 상태만으로
ALTER TABLE ops.rfc_progress_sent ALTER COLUMN payload_hash SET NOT NULL;
ALTER TABLE ops.rfc_progress_sent DROP CONSTRAINT rfc_progress_sent_pkey;
ALTER TABLE ops.rfc_progress_sent ADD PRIMARY KEY (anchor_id, as_of, payload_hash);
COMMENT ON COLUMN ops.rfc_progress_sent.payload_hash IS 'state · auto_pct · legacy_pct · last_cp 의 해시 — 바뀌면 다시 보낸다. V20';

-- F4 적재 추적 (l_* 8표 같은 모양). 열이 있으면 poll-core 가 알아서 채운다
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['l_work_unit', 'l_event', 'l_cut_part', 'l_assy_node', 'l_outfit_item',
                           'l_paint_element', 'l_wo_daily', 'l_bts_move'] LOOP
    EXECUTE format('ALTER TABLE hot.%I ADD COLUMN IF NOT EXISTS src_job text,
                      ADD COLUMN IF NOT EXISTS polled_at timestamptz,
                      ADD COLUMN IF NOT EXISTS src_deleted_at timestamptz', t);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON hot.%I (src_job, polled_at) WHERE src_deleted_at IS NULL',
                   t || '_polled_idx', t);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON hot.%I (src_deleted_at) WHERE src_deleted_at IS NOT NULL',
                   t || '_deleted_idx', t);
    EXECUTE format('COMMENT ON COLUMN hot.%I.src_deleted_at IS %L', t,
                   '레거시 원천에서 사라진 것을 안 시각 — 판별 · 분모에서 뺀다. V20');
  END LOOP;
END $$;
INSERT INTO hot.m_code (code_group, code, name, seq) VALUES ('EXCLUDE_REASON', 'SOURCE_DELETED', '레거시 원천에서 지워짐', 90)
ON CONFLICT DO NOTHING;

-- 고아 대조에서 지워진 행은 뺀다
CREATE OR REPLACE VIEW hot.v_l_orphans AS
  SELECT 'l_work_unit.block' AS ref, unit_id AS key FROM hot.l_work_unit u
   WHERE block_no IS NOT NULL AND src_deleted_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM hot.m_block b WHERE b.hull_no = u.hull_no AND b.block_no = u.block_no)
  UNION ALL
  SELECT 'l_event.unit', unit_id FROM hot.l_event e
   WHERE src_deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = e.unit_id)
  UNION ALL
  SELECT 'l_cut_part.dwg', part_no FROM hot.l_cut_part p
   WHERE dwg_no IS NOT NULL AND src_deleted_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = p.dwg_no AND u.level = 'DWG')
  UNION ALL
  SELECT 'l_wo_daily.wo', wo_no FROM hot.l_wo_daily d
   WHERE src_deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = d.wo_no)
  UNION ALL
  SELECT 'm_block.bts_lot', hull_no || '‖' || block_no FROM hot.m_block b
   WHERE bts_lot IS NOT NULL AND NOT EXISTS (SELECT 1 FROM hot.m_lot l WHERE l.lot_code = b.bts_lot);
