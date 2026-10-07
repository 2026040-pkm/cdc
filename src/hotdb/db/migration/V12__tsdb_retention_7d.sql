-- [tsdb 보존 7일] 필드 이력 하이퍼테이블 셋 모두 7일 뒤 청크째 지운다 (90일 → 7일, 2026-10-07 사용자 결정).
--
-- 압축 정책은 지운다 — 압축도 7일 뒤였으므로 이제 지울 청크를 지우기 직전에 압축하는 일만 남는다.
-- 7일 안의 청크는 압축하지 않아 CDC(WAL 디코딩)와 판별 모듈 조회가 늘 원본 청크를 읽는다.
-- 이미 압축된 청크가 있으면 보존 작업이 그대로 지운다 (압축 해제 필요 없음).
SELECT remove_compression_policy('tsdb.status_history',   if_exists => true);
SELECT remove_compression_policy('tsdb.actual_history',   if_exists => true);
SELECT remove_compression_policy('tsdb.artifact_history', if_exists => true);

SELECT remove_retention_policy('tsdb.status_history',   if_exists => true);
SELECT remove_retention_policy('tsdb.actual_history',   if_exists => true);
SELECT remove_retention_policy('tsdb.artifact_history', if_exists => true);
SELECT add_retention_policy('tsdb.status_history',   drop_after => INTERVAL '7 days');
SELECT add_retention_policy('tsdb.actual_history',   drop_after => INTERVAL '7 days');
SELECT add_retention_policy('tsdb.artifact_history', drop_after => INTERVAL '7 days');
