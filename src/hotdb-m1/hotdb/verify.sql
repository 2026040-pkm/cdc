-- Hot DB 구축 확인 — ./hotdb.sh verify
\pset footer off
\echo '== 엔진'
SELECT split_part(version(), ' ', 2) AS postgres,
       (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb') AS timescaledb,
       current_setting('shared_preload_libraries') AS preload, current_setting('timezone') AS timezone;

\echo '== CDC (WAL 논리 복제) 설정 — 기대: logical · 16 · 16 · 20GB'
SELECT current_setting('wal_level') AS wal_level,
       current_setting('max_wal_senders') AS max_wal_senders,
       current_setting('max_replication_slots') AS max_replication_slots,
       current_setting('max_slot_wal_keep_size') AS max_slot_wal_keep_size,
       current_setting('wal_sender_timeout') AS wal_sender_timeout;
SELECT slot_name, plugin, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;

\echo '== 스키마별 표 (기대: hot 28 + 뷰 1 · ts 2 · tsdb 5 · ops 6)'
SELECT n.nspname AS schema,
       count(*) FILTER (WHERE c.relkind IN ('r','p')) AS tables,
       count(*) FILTER (WHERE c.relkind = 'v') AS views
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('hot','ts','tsdb','ops') AND c.relkind IN ('r','p','v')
GROUP BY 1 ORDER BY 1;

\echo '== 하이퍼테이블 · 정책'
SELECT h.hypertable_schema || '.' || h.hypertable_name AS hypertable, d.time_interval AS chunk,
       string_agg(j.proc_name || ' ' || coalesce(j.config->>'compress_after', j.config->>'drop_after'), ', ' ORDER BY j.proc_name) AS policies
FROM timescaledb_information.hypertables h
JOIN timescaledb_information.dimensions d USING (hypertable_schema, hypertable_name)
LEFT JOIN timescaledb_information.jobs j USING (hypertable_schema, hypertable_name)
GROUP BY 1, 2 ORDER BY 1;

\echo '== publication (CDC)'
SELECT pubname, string_agg(schemaname || '.' || tablename, ', ' ORDER BY schemaname, tablename) FILTER (WHERE schemaname <> '_timescaledb_internal') AS tables
FROM pg_publication_tables GROUP BY 1 ORDER BY 1;

\echo '== 계정'
SELECT rolname, rolcanlogin AS login, rolreplication AS replication FROM pg_roles
WHERE rolname IN ('hotdb_provider','hotdb_monitor','rfc_service','svc_mch','svc_asm','svc_oft','svc_pnt','svc_reader','svc_writer') ORDER BY 1;

\echo '== M1-1.2 가공 절점 사전 (m_code CHECKPOINT · trade C)'
SELECT seq, code, name, in_scope, attrs->>'basis' AS basis, attrs->>'judge_source' AS judge_source
FROM hot.m_code WHERE code_group = 'CHECKPOINT' AND parent_code = 'C' ORDER BY seq;

\echo '== 적재 현황 (M1-1.1 l_* · M1-1.2 p_* · M1-1.3 m_*)'
SELECT 'hot.m_hull' AS t, count(*) FROM hot.m_hull UNION ALL
SELECT 'hot.m_block', count(*) FROM hot.m_block UNION ALL
SELECT 'hot.m_location', count(*) FROM hot.m_location UNION ALL
SELECT 'hot.m_code', count(*) FROM hot.m_code UNION ALL
SELECT 'hot.l_work_unit (DWG)', count(*) FROM hot.l_work_unit WHERE level = 'DWG' UNION ALL
SELECT 'hot.l_event (가공)', count(*) FROM hot.l_event e JOIN hot.m_code c ON c.code_group = e.event_group AND c.code = e.event_code WHERE c.parent_code = 'C' UNION ALL
SELECT 'hot.l_cut_part', count(*) FROM hot.l_cut_part UNION ALL
SELECT 'hot.p_progress', count(*) FROM hot.p_progress UNION ALL
SELECT 'hot.v_l_orphans', count(*) FROM hot.v_l_orphans;
