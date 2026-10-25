-- [판별] 가공 절점 기록 묶음 (2026-10-24, 실데이터 검증에서 나온 것)
--   옛 도면은 야드(ProSSYS) · 라인(140M) 기록이 아예 없어 마킹만 찍혀도 전부 순서 위반(ORDER_REVERSED)이 됐다 (검증 3,000 도면 중 1,522).
--   기록이 없는 원천은 해당 없음으로 보고, 묶음에 기록이 하나라도 있을 때만 빈 절점을 순서 위반으로 본다.
-- 가공 절점의 기록 묶음 — 묶음에 이 도면의 기록이 하나도 없으면 그 절점들은 해당 없음(순서 위반 아님). CheckpointEvaluator
--   YARD = ProSSYS(STY01H) 입고 · 1차 · 2차 선별,  LINE = 140M 불출 · 전처리(자동마킹)
UPDATE hot.m_code SET attrs = attrs || '{"record_group": "YARD"}'::jsonb
 WHERE code_group = 'CHECKPOINT' AND parent_code = 'C' AND code IN ('STEEL_IN', 'PICK1', 'PICK2');
UPDATE hot.m_code SET attrs = attrs || '{"record_group": "LINE"}'::jsonb
 WHERE code_group = 'CHECKPOINT' AND parent_code = 'C' AND code IN ('STEEL_ISSUE', 'PRETREAT');
