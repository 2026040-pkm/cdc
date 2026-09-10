-- ─────────────────────────────────────────────────────────────────────────────
-- 압축 · 보존 · 연속 집계
-- 함수 이름은 TimescaleDB 2.29 기준이다. 압축 정책이 CALL 이고 나머지가 SELECT 인 것은
-- 실제로 프로시저와 함수로 나뉘어 있기 때문이다.
--
-- 하이퍼테이블 셋(lidar_status · lidar_scan_actual · lidar_scan_artifact)에 같은 정책을 건다.
-- 성격이 같기 때문이다 — 셋 다 INSERT 만 있고 정정이 없다. 정정이 들어오는 표가 생기면
-- 그 표에는 압축을 걸지 않는다(정정마다 세그먼트를 풀었다 담아야 한다).
-- ─────────────────────────────────────────────────────────────────────────────

-- 1) 컬럼스토어 전환: 3일 지난 청크를 열 지향으로 재배치한다.
--    상태값은 ONLINE 이 대부분이고 산출물 메타는 같은 문자열이 반복돼 압축이 잘 듣는다.
--
--    CREATE TABLE ... WITH (tsdb.segmentby, tsdb.orderby) 는 2.29 에서 압축 정책까지
--    자동으로 만든다 (compress_after = chunk_interval). 그대로 두면 청크가 만들어지자마자
--    압축되는데, 늦게 도착하는 항목(버퍼 방출)이 압축된 청크에 꽂히면 세그먼트를 풀었다
--    담는 비용이 든다. 그래서 자동 정책을 걷어내고 3일로 다시 건다.
CALL remove_columnstore_policy('lidar_status', if_exists => true);
CALL add_columnstore_policy('lidar_status', after => INTERVAL '3 days');

CALL remove_columnstore_policy('lidar_scan_actual', if_exists => true);
CALL add_columnstore_policy('lidar_scan_actual', after => INTERVAL '3 days');

CALL remove_columnstore_policy('lidar_scan_artifact', if_exists => true);
CALL add_columnstore_policy('lidar_scan_artifact', after => INTERVAL '3 days');

-- 2) 원본 보존 31일. DELETE 가 아니라 청크 DROP 이라 VACUUM 부담이 없다.
--
--    ⚠️ 산출물 메타는 본체가 DB 밖(storage_uri)에 있다. 여기서 행을 지워도 파일은 남으므로,
--    파일 정리 잡이 생기기 전까지는 이 표만 보존 기간을 따로 볼 것.
SELECT add_retention_policy('lidar_status',        drop_after => INTERVAL '31 days');
SELECT add_retention_policy('lidar_scan_actual',   drop_after => INTERVAL '31 days');
SELECT add_retention_policy('lidar_scan_artifact', drop_after => INTERVAL '31 days');

-- 3) 장비별 1분 집계. materialized_only = false 라 아직 집계 안 된 최근 구간은
--    원본에서 실시간으로 메워 조회된다 — 대시보드가 이 뷰를 읽는다.
--    상태 네 가지는 발행기의 상태 전이(ONLINE ↔ CALIBRATING · ERROR → OFFLINE) 그대로다.
CREATE MATERIALIZED VIEW lidar_status_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 minute', time) AS bucket,
       tid,
       avg(temperature_c)                             AS avg_temperature_c,
       max(temperature_c)                             AS max_temperature_c,
       avg(scan_rate_pts_per_sec)                     AS avg_scan_rate,
       min(connectivity_rssi)                         AS min_rssi,
       count(*)                                       AS sample_cnt,
       count(*) FILTER (WHERE status = 'ONLINE')      AS online_cnt,
       count(*) FILTER (WHERE status = 'CALIBRATING') AS calibrating_cnt,
       count(*) FILTER (WHERE status = 'ERROR')       AS error_cnt,
       count(*) FILTER (WHERE status = 'OFFLINE')     AS offline_cnt
FROM lidar_status
GROUP BY bucket, tid
WITH NO DATA;

-- end_offset 2분: 아직 들어오는 중인 최근 2분은 건드리지 않는다.
-- start_offset 1시간: 그 이전 1시간은 매번 다시 계산해 지연 도착분을 반영한다.
SELECT add_continuous_aggregate_policy('lidar_status_1m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '1 minute');

-- 4) 오류 코드별 1분 집계. 장비 축이 아니라 코드 축이라 따로 둔다.
CREATE MATERIALIZED VIEW lidar_error_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 minute', time) AS bucket,
       error_code,
       count(*)            AS cnt,
       count(DISTINCT tid) AS device_cnt
FROM lidar_status
WHERE error_code IS NOT NULL
GROUP BY bucket, error_code
WITH NO DATA;

SELECT add_continuous_aggregate_policy('lidar_error_1m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '1 minute');

-- 5) 스캔(실적) 1분 집계. 장비 상태와 달리 스캔은 분 단위라 버킷당 장비별 1~2건이다.
--    정합 신뢰도와 블록 진척률이 여기서 나온다 — 상태 표에는 없는 축이다.
CREATE MATERIALIZED VIEW lidar_scan_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 minute', time) AS bucket,
       tid,
       stage,
       avg(match_confidence)                            AS avg_match_confidence,
       min(match_confidence)                            AS min_match_confidence,
       max(block_progress_rate)                         AS max_progress_rate,
       count(*)                                         AS scan_cnt,
       count(*) FILTER (WHERE event_type = 'COMPLETE')  AS complete_cnt
FROM lidar_scan_actual
GROUP BY bucket, tid, stage
WITH NO DATA;

SELECT add_continuous_aggregate_policy('lidar_scan_1m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '1 minute');

-- 6) 산출물 1분 집계. 종류별 건수와 바이트 — NAS 에 하루 얼마가 쌓이는지가 여기서 나온다.
CREATE MATERIALIZED VIEW lidar_artifact_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 minute', time) AS bucket,
       artifact_type,
       count(*)                       AS cnt,
       count(DISTINCT scan_id)        AS scan_cnt,
       sum(file_size_bytes)           AS total_bytes,
       avg(file_size_bytes)           AS avg_bytes
FROM lidar_scan_artifact
GROUP BY bucket, artifact_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('lidar_artifact_1m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '1 minute');

-- 7) 집계는 원본보다 오래 남긴다. 원본 31일 < 집계 1년.
SELECT add_retention_policy('lidar_status_1m',   drop_after => INTERVAL '1 year');
SELECT add_retention_policy('lidar_error_1m',    drop_after => INTERVAL '1 year');
SELECT add_retention_policy('lidar_scan_1m',     drop_after => INTERVAL '1 year');
SELECT add_retention_policy('lidar_artifact_1m', drop_after => INTERVAL '1 year');
