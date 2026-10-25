-- [판별 · 적재 보강] 계보 「통합안 · 정규화」 대조(2026-10-24)에서 나온 빈칸을 메운다.
--
--   적재 (RFC Service, poll-jobs-hot.yml)
--     · BTS 실시간 위치(Oracle APNB.NB202M) → m_block.bts_lot · bts_at, l_bts_move (도착 시각마다 1행)
--       지번(m_lot)은 다른 원천(SDE)에서 따로 폴링되므로 적재 순서를 보장할 수 없다 → bts_lot 의 FK 를 논리 FK 로 바꾼다
--       (V17 의 "레거시 적재 표끼리는 논리 FK" 원칙과 같다. hot.v_l_orphans 로 대조)
--   판별 (판별 서비스)
--     · 도장 블록 실적률 Σ413 ÷ Σ212(공정당 1회) · 의장 GDO 설치율 · 조립 W/O 매칭 · 가공 절점 계획일 · 지연 · 반입예정 경과 알림
--     · 공종별 판별 대상 기간(목록 모수)을 사전 TRADE_RULE 에 둔다 — 판별 서비스가 읽는다

ALTER TABLE hot.m_block DROP CONSTRAINT IF EXISTS m_block_bts_lot_fkey;
ALTER TABLE hot.p_progress_block DROP CONSTRAINT IF EXISTS p_progress_block_bts_lot_fkey;

CREATE OR REPLACE VIEW hot.v_l_orphans AS
  SELECT 'l_work_unit.block' AS ref, unit_id AS key FROM hot.l_work_unit u
   WHERE block_no IS NOT NULL AND NOT EXISTS (SELECT 1 FROM hot.m_block b WHERE b.hull_no = u.hull_no AND b.block_no = u.block_no)
  UNION ALL
  SELECT 'l_event.unit', unit_id FROM hot.l_event e
   WHERE NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = e.unit_id)
  UNION ALL
  SELECT 'l_cut_part.dwg', part_no FROM hot.l_cut_part p
   WHERE dwg_no IS NOT NULL AND NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = p.dwg_no AND u.level = 'DWG')
  UNION ALL
  SELECT 'l_wo_daily.wo', wo_no FROM hot.l_wo_daily d
   WHERE NOT EXISTS (SELECT 1 FROM hot.l_work_unit u WHERE u.unit_id = d.wo_no)
  UNION ALL
  SELECT 'm_block.bts_lot', hull_no || '‖' || block_no FROM hot.m_block b
   WHERE bts_lot IS NOT NULL AND NOT EXISTS (SELECT 1 FROM hot.m_lot l WHERE l.lot_code = b.bts_lot);

-- 공종별 판별 대상 기간 · 규칙 인자. 가공(C)은 V17 에 이미 list_window_days 30
UPDATE hot.m_code SET attrs = attrs || '{"list_window_days": 30, "match_day_pool": "WO_PLAN", "match_week_days": 28}'::jsonb
 WHERE code_group = 'TRADE_RULE' AND code = 'S';
UPDATE hot.m_code SET attrs = attrs || '{"list_window_days": 30, "gap_alert_pct": 10, "weight": "WO_MH_ELSE_ITEMS"}'::jsonb
 WHERE code_group = 'TRADE_RULE' AND code = 'W';
UPDATE hot.m_code SET attrs = attrs || '{"list_window_days": 30, "done_pct": 100, "denominator": "MH_212_ONCE_PER_OPER"}'::jsonb
 WHERE code_group = 'TRADE_RULE' AND code = 'P';

-- 판별이 남기는 알림 종류 (p_action.sub_type) — 사전으로 둔다
INSERT INTO hot.m_code (code_group, code, name, seq) VALUES
  ('ALERT', 'UNMATCHED', '인식했는데 레거시 앵커가 없음', 1),
  ('ALERT', 'LOW_CONFIDENCE', '정합도가 모자라 실적에 쓰지 않음', 2),
  ('ALERT', 'ORDER_REVERSED', '앞 절점 없이 뒤 절점이 찍힘', 3),
  ('ALERT', 'CARRY_OVERDUE', '강재 반입예정일이 지났는데 불출 전', 4),
  ('ALERT', 'GAP', '자동 실적과 레거시 실적 차이가 기준 초과', 5),
  ('ALERT', 'WO_MISMATCH', '인식한 송선에 맞는 W/O 가 계획 범위에 없음', 6)
ON CONFLICT DO NOTHING;

-- 판별 조회 자리
CREATE INDEX IF NOT EXISTS l_work_unit_act_no_idx ON hot.l_work_unit (act_no) WHERE level = 'WO';
CREATE INDEX IF NOT EXISTS p_action_open_idx ON hot.p_action (hull_no, block_no) WHERE status = 'OPEN';

-- BTS 현재 위치: 적재는 l_bts_move 에만 한다. 블록 마스터(m_block)는 호선 FK 가 있어 모르는 호선 블록이 섞이면
-- 적재 묶음 전체가 실패하므로, 이미 있는 블록에 한해 더 새로운 도착이 들어올 때 트리거가 bts_lot · bts_at 을 맞춘다
CREATE OR REPLACE FUNCTION hot.l_bts_move_to_block() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE hot.m_block b SET bts_lot = NEW.to_lot, bts_at = NEW.move_ts
   WHERE b.hull_no = NEW.hull_no AND b.block_no = NEW.block_no
     AND (b.bts_at IS NULL OR b.bts_at <= NEW.move_ts);
  RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS l_bts_move_to_block ON hot.l_bts_move;
CREATE TRIGGER l_bts_move_to_block AFTER INSERT OR UPDATE ON hot.l_bts_move
  FOR EACH ROW EXECUTE FUNCTION hot.l_bts_move_to_block();
-- 블록이 나중에 들어오는 경우 — 블록 적재 때 최신 도착을 붙인다
CREATE OR REPLACE FUNCTION hot.m_block_bts_fill() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.bts_lot IS NULL THEN
    SELECT to_lot, move_ts INTO NEW.bts_lot, NEW.bts_at FROM hot.l_bts_move
     WHERE hull_no = NEW.hull_no AND block_no = NEW.block_no ORDER BY move_ts DESC LIMIT 1;
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS m_block_bts_fill ON hot.m_block;
CREATE TRIGGER m_block_bts_fill BEFORE INSERT ON hot.m_block FOR EACH ROW EXECUTE FUNCTION hot.m_block_bts_fill();
