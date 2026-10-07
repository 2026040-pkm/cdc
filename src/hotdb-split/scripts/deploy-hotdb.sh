#!/usr/bin/env bash
# Hot DB(필드 DB) 서버에 compose.db.yml 을 올린다 — 원격 docker 호스트에 SSH 로.
#
#   scripts/deploy-hotdb.sh                       # .env 의 HOTDB_SSH 로 (예: HOTDB_SSH="ssh -p 7481 white@172.30.1.88")
#   HOTDB_SSH="ssh -p 7481 white@172.30.1.88" scripts/deploy-hotdb.sh
#   scripts/deploy-hotdb.sh down                  # 내리기 (볼륨은 둔다)
#   scripts/deploy-hotdb.sh ps | logs             # 상태 · 로그
#
# 보내는 것: compose.db.yml · compose.monitoring.yml(node-exporter · cadvisor 만 띄움) · db/ · monitoring/ · app/Dockerfile ·
#           hotdb-migrate jar. 이미지는 저쪽에서 docker compose build 로 만든다 (jar 는 아키텍처를 안 탄다).
# 미리: app/ 에서 gradle bootJar (scripts/up-services.sh 가 한다) · 원격 계정이 docker 를 쓸 수 있어야 한다.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && set -a && . ./.env && set +a
: "${HOTDB_SSH:?HOTDB_SSH 를 .env 나 환경변수로 (예: ssh -p 7481 white@172.30.1.88)}"
REMOTE_DIR="${HOTDB_REMOTE_DIR:-~/hotdb}"
action="${1:-up}"

rsh() { $HOTDB_SSH "$@"; }

# 원격의 compose 명령 (docker compose v2 → docker-compose → sudo)
compose_cmd=$(rsh 'if docker compose version >/dev/null 2>&1; then echo "docker compose";
  elif command -v docker-compose >/dev/null 2>&1; then echo "docker-compose";
  elif sudo -n docker compose version >/dev/null 2>&1; then echo "sudo docker compose";
  else echo ""; fi')
[ -n "$compose_cmd" ] || { echo "원격에 docker compose 가 없거나 $USER 가 docker 를 못 씁니다 (sudo usermod -aG docker <계정>)"; exit 1; }
echo "원격 compose: $compose_cmd  ($HOTDB_SSH, $REMOTE_DIR)"

if [ "$action" = up ]; then
  jar=app/hotdb-migrate/build/libs/app.jar
  [ -f "$jar" ] || { echo "$jar 가 없습니다 — 먼저 scripts/up-services.sh 로 jar 를 만드세요"; exit 1; }
  rsh "mkdir -p $REMOTE_DIR/app/hotdb-migrate/build/libs"
  # rsync 가 없을 수도 있어 tar 로 보낸다
  tar czf - compose.db.yml compose.monitoring.yml db monitoring app/Dockerfile "$jar" \
    | rsh "tar xzf - -C $REMOTE_DIR"
  # 원격 .env — 이 쪽 .env 의 HotDB 서버용 값만 넘긴다
  rsh "cat > $REMOTE_DIR/.env" <<EOF
HOTDB_ADMIN_PASSWORD=${HOTDB_ADMIN_PASSWORD:-postgres}
HOTDB_SHARED_BUFFERS=${HOTDB_SHARED_BUFFERS:-256MB}
HOTDB_PUBLIC_PORT=${HOTDB_PORT:-59433}
HOTDB_EXPORTER_PORT=${HOTDB_EXPORTER_PORT:-59487}
NODE_EXPORTER_PORT=${NODE_EXPORTER_PORT:-59489}
CADVISOR_PORT=${CADVISOR_PORT:-59492}
EOF
  rsh "cd $REMOTE_DIR && $compose_cmd -f compose.db.yml build hotdb-migrate && $compose_cmd -f compose.db.yml up -d \
       && $compose_cmd -f compose.monitoring.yml up -d node-exporter cadvisor"
  echo
  echo "마이그레이션 로그:"
  rsh "cd $REMOTE_DIR && $compose_cmd -f compose.db.yml logs --no-log-prefix hotdb-migrate | tail -20"
  echo
  rsh "cd $REMOTE_DIR && $compose_cmd -f compose.db.yml ps"
elif [ "$action" = down ]; then
  rsh "cd $REMOTE_DIR && $compose_cmd -f compose.db.yml -f compose.monitoring.yml down"
elif [ "$action" = ps ]; then
  rsh "cd $REMOTE_DIR && $compose_cmd -f compose.db.yml -f compose.monitoring.yml ps"
elif [ "$action" = logs ]; then
  rsh "cd $REMOTE_DIR && $compose_cmd -f compose.db.yml logs --tail=100 ${2:-}"
else
  echo "사용: $0 [up|down|ps|logs [service]]"; exit 2
fi
