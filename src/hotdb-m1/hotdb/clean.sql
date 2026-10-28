-- Hot DB 데이터만 비운다 — ./hotdb.sh clean
-- 남기는 것: 스키마 · 사전 시드(hot.m_code 시드 13그룹 · m_signal_code · m_signal_def · ops.module)
DO $$
DECLARE t text;
BEGIN
  SELECT string_agg(format('%I.%I', schemaname, tablename), ', ') INTO t
  FROM pg_tables
  WHERE schemaname IN ('hot', 'ts', 'tsdb', 'ops')
    AND format('%s.%s', schemaname, tablename) NOT IN
        ('hot.m_code', 'hot.m_signal_code', 'hot.m_signal_def', 'ops.module');
  EXECUTE 'TRUNCATE ' || t || ' RESTART IDENTITY';
  -- m_code 는 시드 그룹만 남긴다 — 적재 작업이 넣은 공통 코드(YPGS010M 그룹 · MP)는 지운다
  DELETE FROM hot.m_code
   WHERE code_group NOT IN ('ALERT', 'ANCHOR_TYPE', 'CHECKPOINT', 'CONF', 'DEVICE_CLASS', 'EVENT', 'EXCLUDE_REASON',
                            'PAINT_STEP', 'PART_ROLE', 'SITE', 'SOURCE_TABLE', 'TRADE', 'TRADE_RULE');
END $$;
