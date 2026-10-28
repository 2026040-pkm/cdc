#!/usr/bin/env bash
# Hot DB 절차 래퍼 — 구축 · 초기화 · 리셋 · 확인
#   ./hotdb.sh up       기동 + 확인. 볼륨이 비어 있으면 initdb 가 스키마를 만든다
#   ./hotdb.sh clean    데이터만 비움 — 스키마 · 사전 시드 유지
#   ./hotdb.sh reset    볼륨까지 지우고 처음부터 다시 구축 (데이터 전부 삭제) — 스키마를 바꾼 뒤에도 이것
#   ./hotdb.sh verify   구축 상태 · CDC 설정 · 가공 절점 사전 · 적재 건수 출력
#   ./hotdb.sh cdc-check  시험 슬롯으로 tsdb · hot.p_progress 변경이 publication 으로 나오는지 확인
#   ./hotdb.sh psql     접속
#   ./hotdb.sh down     컨테이너 정지 (데이터 유지)
# 엔진은 docker 우선, 없으면 podman. HOTDB_ENGINE=podman 으로 강제할 수 있다.
set -euo pipefail
cd "$(dirname "$0")"

ENGINE=${HOTDB_ENGINE:-$(command -v docker >/dev/null 2>&1 && echo docker || echo podman)}
export PODMAN_COMPOSE_WARNING_LOGS=false   # podman 외부 compose 안내문 끄기

compose() { "$ENGINE" compose "$@"; }
# 관리자는 컨테이너의 POSTGRES_USER 를 그대로 쓴다 (.env HOTDB_ADMIN_USER)
psql_db() { compose exec -T hotdb sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'; }

cmd_verify()  { psql_db < hotdb/verify.sql; }
cmd_cdc()     { psql_db < hotdb/cdc_check.sql; }
cmd_up()      { compose up -d --wait hotdb && cmd_verify; }
cmd_reset()   { compose down -v --remove-orphans && cmd_up; }
cmd_clean()   { psql_db < hotdb/clean.sql && echo "데이터를 비웠습니다 (스키마 · 사전 시드 유지)."; }

case "${1:-}" in
  up|clean|reset|verify) "cmd_$1" ;;
  cdc-check) cmd_cdc ;;
  psql) compose exec hotdb sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' ;;
  down) compose down ;;
  *) sed -n '2,10p' "$0"; exit 1 ;;
esac
