-- ─────────────────────────────────────────────────────────────────────────────
-- 압축 · 보존 · 연속 집계
-- 함수 이름은 TimescaleDB 2.29 기준이다. 압축 정책이 CALL 이고 나머지가 SELECT 인 것은
-- 실제로 프로시저와 함수로 나뉘어 있기 때문이다.
-- ─────────────────────────────────────────────────────────────────────────────

-- 1) 컬럼스토어 전환: 3일 지난 청크를 열 지향으로 재배치한다.
--    상태값은 ONLINE 이 대부분(관측 88%)이라 압축이 잘 듣는다.
--
--    CREATE TABLE ... WITH (tsdb.segmentby, tsdb.orderby) 는 2.29 에서 압축 정책까지
--    자동으로 만든다 (compress_after = chunk_interval = 6시간). 그대로 두면 6시간 지난
--    청크가 바로 압축되는데, 늦게 도착하는 항목(버퍼 방출)이 압축된 청크에 꽂히면
--    세그먼트를 풀었다 담는 비용이 든다. 그래서 자동 정책을 걷어내고 3일로 다시 건다.
CALL remove_columnstore_policy('lidar_status', if_exists => true);
CALL add_columnstore_policy('lidar_status', after => INTERVAL '3 days');

-- 2) 원본 보존 31일. DELETE 가 아니라 청크 DROP 이라 VACUUM 부담이 없다.
SELECT add_retention_policy('lidar_status', drop_after => INTERVAL '31 days');

-- 3) 장비별 1분 집계. materialized_only = false 라 아직 집계 안 된 최근 구간은
--    원본에서 실시간으로 메워 조회된다 — 대시보드가 이 뷰를 읽는다.
CREATE MATERIALIZED VIEW lidar_status_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 minute', time) AS bucket,
       tid,
       avg(temperature_c)                          AS avg_temperature_c,
       max(temperature_c)                          AS max_temperature_c,
       avg(scan_rate_pts_per_sec)                  AS avg_scan_rate,
       avg(point_cloud_quality_score)              AS avg_quality,
       min(connectivity_rssi)                      AS min_rssi,
       count(*)                                    AS sample_cnt,
       count(*) FILTER (WHERE status = 'ERROR')    AS error_cnt,
       count(*) FILTER (WHERE status = 'WARNING')  AS warning_cnt,
       count(*) FILTER (WHERE status = 'IDLE')     AS idle_cnt
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

-- 5) 집계는 원본보다 오래 남긴다. 원본 31일 < 집계 1년.
SELECT add_retention_policy('lidar_status_1m', drop_after => INTERVAL '1 year');
SELECT add_retention_policy('lidar_error_1m',  drop_after => INTERVAL '1 year');
