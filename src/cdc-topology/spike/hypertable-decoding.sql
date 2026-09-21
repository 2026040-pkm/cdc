-- ─────────────────────────────────────────────────────────────────────────────
-- 하이퍼테이블 · 일반 표가 logical decoding 에 어떻게 보이는가
--
-- docs/cdc/timescaledb-cdc-impact.html 04절이 "확인 필요" 로 남긴 세 질문과, 구성 5안 비교에 필요한
-- 비용 수치를 이 이미지(timescaledb 2.29.2-pg17)에서 실기로 확정한다.
--
--   Q1  publication FOR TABLE <하이퍼테이블> 로 행이 나오는가. 이후 생기는 청크는 따라오는가
--   Q2  압축(컬럼스토어 전환)이 DELETE 이벤트로 나오는가
--   Q3  보존(drop_chunks)이 이벤트를 남기는가
--   Q4  연속 집계 refresh 가 이벤트를 만드는가
--   Q5  하이퍼테이블에 건 REPLICA IDENTITY 가 새 청크에 상속되는가
--   C1  행당 WAL — 하이퍼테이블 INSERT vs 상태 표 UPDATE(REPLICA IDENTITY FULL)
--
-- 슬롯 셋을 같이 연다.
--   s_td    test_decoding — publication 과 무관하게 WAL 의 모든 DML 을 사람이 읽는 모양으로 보여 준다 (기준선)
--   s_h     pgoutput + pub_h   (FOR TABLE h · r)   — Debezium 이 기본으로 쓰는 경로
--   s_all   pgoutput + pub_all (FOR ALL TABLES)    — 청크를 잡으려고 넓힐 때의 경로
-- 단계마다 슬롯을 비우고(get) 그 단계의 이벤트만 센다.
-- ─────────────────────────────────────────────────────────────────────────────
\set ON_ERROR_STOP on
\pset footer off
SET client_min_messages = warning;

SELECT extversion AS timescaledb, current_setting('server_version') AS postgres
  FROM pg_extension WHERE extname = 'timescaledb';

-- ── 준비 ─────────────────────────────────────────────────────────────────────
-- h : lidar tsdb.lidar_status 를 줄인 모양 (시각 · 장비 · 값 · 멱등 키). 청크 1시간
-- r : lidar rdb.lidar_device_state 를 줄인 모양 (장비 축 최신값). REPLICA IDENTITY FULL — 원천과 같다
CREATE TABLE h (
    time            timestamptz      NOT NULL,
    tid             text             NOT NULL,
    status          text             NOT NULL,
    temperature_c   double precision,
    idempotency_key text             NOT NULL
);
SELECT create_hypertable('h', by_range('time', interval '1 hour'));
CREATE UNIQUE INDEX h_uq ON h (idempotency_key, time);

CREATE TABLE r (
    tid           text PRIMARY KEY,
    status        text NOT NULL,
    temperature_c double precision,
    last_event_at timestamptz NOT NULL
);
ALTER TABLE r REPLICA IDENTITY FULL;

-- 하이퍼테이블 부모에 FULL 을 걸어 둔다 → Q5 에서 새 청크가 물려받는지 본다
ALTER TABLE h REPLICA IDENTITY FULL;

CREATE PUBLICATION pub_h FOR TABLE h, r;
CREATE PUBLICATION pub_all FOR ALL TABLES;

SELECT 'slot' AS step, pg_create_logical_replication_slot('s_td', 'test_decoding') IS NOT NULL AS ok
UNION ALL SELECT 'slot', pg_create_logical_replication_slot('s_h', 'pgoutput') IS NOT NULL
UNION ALL SELECT 'slot', pg_create_logical_replication_slot('s_all', 'pgoutput') IS NOT NULL;

-- 단계별 수거표
CREATE TABLE spike_note (item text, result text);
CREATE TABLE spike_td (step text, tbl text, op text, n bigint);
CREATE TABLE spike_po (step text, slot text, kind text, rel text, n bigint);

-- test_decoding: "table <schema.rel>: INSERT: ..." 를 표 · 연산으로 센다
CREATE FUNCTION drain_td(p_step text) RETURNS void LANGUAGE sql AS $$
    INSERT INTO spike_td
    SELECT p_step, m[1], m[2], count(*)
      FROM pg_logical_slot_get_changes('s_td', NULL, NULL) c,
           LATERAL regexp_match(c.data, '^table ([^:]+): ([A-Z]+):') m
     WHERE m IS NOT NULL
     GROUP BY 2, 3;
$$;

-- pgoutput(proto 1): 첫 바이트가 메시지 종류(R=Relation · I · U · D · T=Truncate), 그 뒤 4바이트가 relation oid.
-- Relation 메시지에서 oid → "schema.rel" 을 뽑아 같은 단계의 I/U/D 에 붙인다.
CREATE FUNCTION drain_po(p_step text, p_slot name, p_pub text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    -- drain() 이 한 트랜잭션에서 슬롯 둘을 차례로 부르므로 ON COMMIT DROP 으로는 두 번째가 겹친다
    DROP TABLE IF EXISTS _po;
    CREATE TEMP TABLE _po AS
    SELECT chr(get_byte(data, 0)) AS kind, data
      FROM pg_logical_slot_get_binary_changes(p_slot, NULL, NULL,
             'proto_version', '1', 'publication_names', p_pub);
    INSERT INTO spike_po
    WITH rel AS (
        SELECT DISTINCT encode(substring(data FROM 2 FOR 4), 'hex') AS oid,
               (regexp_match(encode(substring(data FROM 6), 'escape'), '^(.*?)\\000(.*?)\\000')) AS nm
          FROM _po WHERE kind = 'R'
    )
    SELECT p_step, p_slot, p.kind, coalesce(r.nm[1] || '.' || r.nm[2], '?'), count(*)
      FROM _po p
      LEFT JOIN rel r ON r.oid = encode(substring(p.data FROM 2 FOR 4), 'hex')
     WHERE p.kind IN ('I', 'U', 'D')
     GROUP BY 3, 4;
    INSERT INTO spike_po SELECT p_step, p_slot, 'R', count(*)::text, NULL FROM _po WHERE kind = 'R';
END $$;

CREATE FUNCTION drain(p_step text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM drain_td(p_step);
    PERFORM drain_po(p_step, 's_h', 'pub_h');
    -- 6단계에서 pub_all 을 내린다. 그 뒤로는 s_all 이 없다
    IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 's_all') THEN
        PERFORM drain_po(p_step, 's_all', 'pub_all');
    END IF;
END $$;

-- ── 1) 적재 — 청크 셋에 걸쳐 1,050행 · 상태 표 350행 ─────────────────────────
-- 슬롯을 만든 뒤에 생기는 청크다. "이후 생기는 청크가 따라오는가" 가 이 단계에서 드러난다.
BEGIN;
INSERT INTO h
SELECT date_trunc('hour', now()) - interval '3 hours' + (k * interval '1 hour') + (d * interval '1 second'),
       'LDR-' || d, 'ONLINE', 40 + d % 5, 'LDR-' || d || ':' || k
  FROM generate_series(0, 2) k, generate_series(1, 350) d;
INSERT INTO r SELECT 'LDR-' || d, 'ONLINE', 40, now() FROM generate_series(1, 350) d;
COMMIT;
SELECT drain('1 insert');
INSERT INTO spike_note
SELECT 'pub_all 이 가리키는 표 (1단계 뒤)', count(*) || '개 · 그중 _timescaledb_internal ' ||
       count(*) FILTER (WHERE schemaname = '_timescaledb_internal') || '개'
  FROM pg_publication_tables WHERE pubname = 'pub_all';

-- ── 2) 상태 표 갱신 — 350 UPDATE (lidar 소비자가 1초마다 하는 일) ──────────
UPDATE r SET temperature_c = temperature_c + 1, last_event_at = now();
SELECT drain('2 update r');

-- ── 3) 압축 — 가장 오래된 청크 하나를 컬럼스토어로 ─────────────────────────
ALTER TABLE h SET (timescaledb.compress, timescaledb.compress_segmentby = 'tid', timescaledb.compress_orderby = 'time DESC');
SELECT compress_chunk(c) AS compressed FROM show_chunks('h') c ORDER BY c LIMIT 1;
SELECT drain('3 compress');

-- ── 4) 압축된 청크에 늦은 행 350 (백필) ──────────────────────────────────────
INSERT INTO h
SELECT date_trunc('hour', now()) - interval '3 hours' + (d * interval '1 second') + interval '1 millisecond',
       'LDR-' || d, 'ONLINE', 50, 'LDR-' || d || ':late'
  FROM generate_series(1, 350) d;
SELECT drain('4 insert into compressed');

-- ── 5) 보존 — 압축 청크를 drop_chunks 로 지운다 ──────────────────────────────
SELECT count(*) AS dropped FROM drop_chunks('h', older_than => date_trunc('hour', now()) - interval '2 hours');
SELECT drain('5 drop_chunks');

-- ── 6) 연속 집계 생성 + refresh ──────────────────────────────────────────────
-- FOR ALL TABLES publication 이 있으면 연속 집계가 만들어지지 않는다(2026-09-17 첫 실행에서 발견).
-- 집계의 materialization 하이퍼테이블이 생기는 순간 "publication 에 든 표" 가 되기 때문이다.
-- 그 사실을 기록하고, pub_all 을 내린 뒤 집계를 만들어 refresh 가 WAL 에 무엇을 남기는지 본다.
DO $$
BEGIN
    EXECUTE $q$CREATE MATERIALIZED VIEW h_1m_try WITH (timescaledb.continuous) AS
               SELECT time_bucket('1 minute', time) AS bucket, tid, avg(temperature_c) AS t
                 FROM h GROUP BY 1, 2 WITH NO DATA$q$;
    INSERT INTO spike_note VALUES ('cagg with FOR ALL TABLES publication', 'created');
EXCEPTION WHEN OTHERS THEN
    INSERT INTO spike_note VALUES ('cagg with FOR ALL TABLES publication', 'ERROR: ' || SQLERRM);
END $$;
SELECT drain('6a cagg attempt');
SELECT pg_drop_replication_slot('s_all');
DROP PUBLICATION pub_all;
CREATE MATERIALIZED VIEW h_1m WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket, tid, avg(temperature_c) AS t
  FROM h GROUP BY 1, 2 WITH NO DATA;
CALL refresh_continuous_aggregate('h_1m', NULL, NULL);
SELECT drain('6b cagg refresh');

-- ── 7) 새 청크 (슬롯 생성 이후 처음 보는 시간대) ────────────────────────────
INSERT INTO h
SELECT date_trunc('hour', now()) + interval '5 hours' + (d * interval '1 second'),
       'LDR-' || d, 'ONLINE', 41, 'LDR-' || d || ':future'
  FROM generate_series(1, 350) d;
SELECT drain('7 insert new chunk');

-- ── 결과 ─────────────────────────────────────────────────────────────────────
\echo
\echo '== test_decoding (WAL 에 실제로 무엇이 나왔나 · publication 무관) =='
SELECT step, tbl, op, n FROM spike_td ORDER BY step, tbl, op;

\echo
\echo '== pgoutput · publication 별 (Debezium 이 받는 것) =='
SELECT step, slot, kind, rel, n FROM spike_po WHERE kind <> 'R' ORDER BY step, slot, rel, kind;

\echo
\echo '== 기록 =='
SELECT item, result FROM spike_note;

\echo
\echo '== publication 이 가리키는 표 =='
SELECT pubname, schemaname || '.' || tablename AS tbl FROM pg_publication_tables
 WHERE pubname = 'pub_h' ORDER BY 2;

\echo
\echo '== Q5 청크의 REPLICA IDENTITY (부모 h 에 FULL) · f=FULL d=DEFAULT =='
SELECT c.relname AS chunk, c.relreplident
  FROM show_chunks('h') s JOIN pg_class c ON c.oid = s::regclass ORDER BY 1;

-- ── C1 행당 WAL ──────────────────────────────────────────────────────────────
-- 슬롯이 WAL 을 붙잡는 비용은 원천 쓰기의 WAL 양이 정한다. 같은 350대 · 10회분을 두 모양으로 쓴다.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;
CHECKPOINT;
SELECT pg_current_wal_insert_lsn() AS l0 \gset
INSERT INTO h
SELECT date_trunc('hour', now()) + interval '6 hours' + (k * interval '1 second') + (d * interval '1 millisecond'),
       'LDR-' || d, 'ONLINE', 40 + d % 5, 'LDR-' || d || ':w' || k
  FROM generate_series(1, 10) k, generate_series(1, 350) d;
SELECT pg_current_wal_insert_lsn() AS l1 \gset
DO $$ BEGIN FOR i IN 1..10 LOOP
    UPDATE r SET temperature_c = temperature_c + 1, last_event_at = now();
END LOOP; END $$;
SELECT pg_current_wal_insert_lsn() AS l2 \gset
ALTER TABLE r REPLICA IDENTITY DEFAULT;
DO $$ BEGIN FOR i IN 1..10 LOOP
    UPDATE r SET temperature_c = temperature_c + 1, last_event_at = now();
END LOOP; END $$;
SELECT pg_current_wal_insert_lsn() AS l3 \gset

\echo
\echo '== C1 행당 WAL (3,500행씩 · 체크포인트 직후라 첫 변경의 full page image 가 섞인다) =='
SELECT 'h INSERT (하이퍼테이블 · 인덱스 3개)'    AS what, round(pg_wal_lsn_diff(:'l1', :'l0') / 3500.0) AS bytes_per_row
UNION ALL
SELECT 'r UPDATE (REPLICA IDENTITY FULL)',  round(pg_wal_lsn_diff(:'l2', :'l1') / 3500.0)
UNION ALL
SELECT 'r UPDATE (REPLICA IDENTITY DEFAULT)', round(pg_wal_lsn_diff(:'l3', :'l2') / 3500.0);
