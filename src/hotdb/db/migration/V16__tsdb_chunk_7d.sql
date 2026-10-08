-- [Hot DB] tsdb 이력 하이퍼테이블 청크 간격을 7일로 (2026-10-07, 사용자 결정 "일단 7일").
-- 처음에 V12 로 만들었으나 같은 날 V12(보존 정책)와 번호가 겹쳐 V16 으로 옮겼다 — Flyway 는 같은 버전이 둘이면 기동을 거부한다.
-- 이미 만들어진 청크는 그대로이고, 다음에 만들어지는 청크부터 7일 범위다 — 데이터 이동 없음, 서비스 정지 불필요.
-- 압축 정책(compress_after 7 days)은 청크 범위 전체가 7일을 지나야 걸리므로 실제 압축은 7~14일 뒤가 된다.
-- 되돌릴 때는 새 V 파일에서 set_chunk_time_interval 을 다시 부른다 (6시간 · 1일 · 1일 이 원래 값).
SELECT set_chunk_time_interval('tsdb.status_history',   INTERVAL '7 days');
SELECT set_chunk_time_interval('tsdb.actual_history',   INTERVAL '7 days');
SELECT set_chunk_time_interval('tsdb.artifact_history', INTERVAL '7 days');
