#!/usr/bin/env bash
# 서버 to 서버 — 이 PC(docker)에 서비스 쪽을 띄운다: 판별 모듈 4 · RFC Service · DB Agent · 발행기 · Prometheus · Grafana.
# Hot DB 는 .env 의 HOTDB_HOST 가 비어 있거나 hotdb-pg 면 **이 PC 에 같이 띄우고**(compose.db.yml), 아니면 그 서버
# (scripts/deploy-hotdb.sh) 에 있다고 본다. 레거시 DB 셋(legacy-db · sap-sim · oracle-sim)은 LEGACY_HOST 의 서버
# (dist/legacy-bundle, scripts/make-legacy-bundle.sh). 주소는 .env 에.
#
#   scripts/up-services.sh                 # jar 빌드 + 이미지 빌드 + 기동
#   scripts/up-services.sh --skip-build    # 빌드 없이
#   scripts/up-services.sh --no-legacy     # 레거시 서버가 아직 없을 때: RFC Service · DB Agent 는 빼고 띄운다
#   scripts/up-services.sh down [-v]       # 내리기 (-v 면 오프셋 볼륨까지)
#   scripts/up-services.sh ps | logs [svc]
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { echo ".env 가 없습니다 — .env.example 을 복사해 HOTDB_HOST · LEGACY_HOST 를 적으세요"; exit 1; }
set -a; . ./.env; set +a
HOTDB_HOST="${HOTDB_HOST:-hotdb-pg}"
local_db=0; [ "$HOTDB_HOST" = hotdb-pg ] && local_db=1
if [ $local_db = 1 ]; then HOTDB_PORT=5432; else : "${HOTDB_PORT:=59433}"; fi
export HOTDB_HOST HOTDB_PORT

files=(-f compose.zone.yml -f compose.rfc.yml -f compose.agent.yml -f compose.sim.yml -f compose.monitoring.yml)
[ $local_db = 1 ] && files=(-f compose.db.yml "${files[@]}")
[ "${LSIM_LOCAL:-0}" = 1 ] && files+=(-f compose.legacy-sim.yml)
# podman-exporter 는 docker 에서 못 쓴다 (podman 소켓). 나머지만.
services=(zone-asm zone-oft zone-pnt zone-mch rfc-service db-agent field-simulator prometheus grafana node-exporter cadvisor)
monitoring=(prometheus grafana node-exporter cadvisor)
build=1; legacy=1
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-build) build=0 ;;
    --no-legacy)  legacy=0 ;;
    down) shift; exec docker compose "${files[@]}" down "$@" ;;
    ps)   exec docker compose "${files[@]}" ps ;;
    logs) shift; exec docker compose "${files[@]}" logs --tail=100 -f "$@" ;;
    *) echo "모르는 인자: $1"; exit 2 ;;
  esac; shift
done
[ "${LSIM_LOCAL:-0}" = 1 ] && services+=(legacy-simulator)
if [ $legacy = 0 ]; then
  services=("${services[@]/rfc-service}"); services=("${services[@]/db-agent}"); services=("${services[@]/legacy-simulator}")
elif [ -z "${LEGACY_HOST:-}" ]; then
  echo ".env 에 LEGACY_HOST 가 없습니다 — 레거시 서버가 아직 없으면 --no-legacy 로 띄우세요"; exit 1
fi

# ── 1. jar ──────────────────────────────────────────────────────────────
if [ $build = 1 ]; then
  if [ -z "${JAVA_HOME:-}" ] || ! "$JAVA_HOME/bin/java" -version 2>&1 | grep -q '"21'; then
    JAVA_HOME=$(/usr/libexec/java_home -v 21 2>/dev/null || true)
  fi
  [ -n "$JAVA_HOME" ] || { echo "JDK 21 이 없습니다"; exit 1; }
  export JAVA_HOME
  gradle=app/gradlew
  if [ ! -f app/gradle/wrapper/gradle-wrapper.jar ]; then   # *.jar 가 .gitignore 라 래퍼 jar 가 없다 — 받아 둔 배포판으로
    gradle=$(ls -d ~/.gradle/wrapper/dists/gradle-8.14-bin/*/gradle-8.14/bin/gradle 2>/dev/null | head -1 || true)
    [ -n "$gradle" ] || gradle=$(command -v gradle || true)
    [ -n "$gradle" ] || { echo "gradle 이 없습니다 (brew install gradle 또는 app/gradle/wrapper/gradle-wrapper.jar)"; exit 1; }
  fi
  echo "gradle bootJar ($JAVA_HOME) ..."
  (cd app && "$gradle" bootJar --no-daemon -q)
  echo "docker compose build ..."
  docker compose "${files[@]}" build zone-asm rfc-service db-agent field-simulator
fi

# ── 2. Prometheus 대상 — 서버 주소를 적는다 (Prometheus 가 30초마다 다시 읽는다) ──
t=monitoring/prometheus/targets
# hotdb_db=1 — Hot DB 데이터 디스크가 있는 서버. 슬롯 WAL 이 디스크를 먼저 채우는지(장기 다운 허용 한계) 이 서버 디스크로 본다
if [ $local_db = 1 ]; then hotdb_exp="hotdb-exporter:9187"; hotdb_node=""; hotdb_cad=""; svc_db=', "hotdb_db": "1"'
else hotdb_exp="${HOTDB_HOST}:${HOTDB_EXPORTER_PORT:-59487}"; svc_db=""
     hotdb_node=",
  { \"targets\": [\"${HOTDB_HOST}:${NODE_EXPORTER_PORT:-59489}\"], \"labels\": { \"server\": \"hotdb\", \"hotdb_db\": \"1\" } }"
     hotdb_cad=",
  { \"targets\": [\"${HOTDB_HOST}:${CADVISOR_PORT:-59492}\"], \"labels\": { \"server\": \"hotdb\" } }"; fi
cat > $t/hotdb.json <<EOF
[ { "targets": ["${hotdb_exp}"], "labels": { "server": "hotdb" } } ]
EOF
cat > $t/node.json <<EOF
[ { "targets": ["node-exporter:9100"], "labels": { "server": "services"${svc_db} } }${hotdb_node} ]
EOF
legacy_cadvisor=""
[ -n "${LEGACY_HOST:-}" ] && legacy_cadvisor=",
  { \"targets\": [\"${LEGACY_HOST}:${CADVISOR_PORT:-59492}\"], \"labels\": { \"server\": \"legacy\" } }"
cat > $t/podman.json <<EOF
[ { "targets": ["cadvisor:8080"], "labels": { "server": "services" } }${hotdb_cad}${legacy_cadvisor} ]
EOF
if [ -n "${LEGACY_HOST:-}" ]; then
  cat > $t/legacy-db.json <<EOF
[ { "targets": ["${LEGACY_HOST}:${LEGACY_EXPORTER_PORT:-59491}"], "labels": { "server": "legacy" } } ]
EOF
  # 레거시 발행기는 레거시 서버(묶음 up.sh)에 두는 게 기본. 이 PC 에서 띄웠으면(LSIM_LOCAL=1) 로컬 컨테이너를 본다
  if [ "${LSIM_LOCAL:-0}" = 1 ]; then lsim_target="legacy-simulator:8080"; lsim_server="services"
  else lsim_target="${LEGACY_HOST}:${LSIM_PORT:-59493}"; lsim_server="legacy"; fi
  cat > $t/legacy-simulator.json <<EOF
[ { "targets": ["${lsim_target}"], "labels": { "server": "${lsim_server}" } } ]
EOF
  cat > $t/node.json <<EOF
[ { "targets": ["node-exporter:9100"], "labels": { "server": "services"${svc_db} } }${hotdb_node},
  { "targets": ["${LEGACY_HOST}:${NODE_EXPORTER_PORT:-59489}"], "labels": { "server": "legacy" } } ]
EOF
fi

# ── 3. 기동 ─────────────────────────────────────────────────────────────
if [ $local_db = 1 ]; then
  echo "Hot DB 를 이 PC 에 띄우고 마이그레이션을 기다립니다 ..."
  docker compose "${files[@]}" up -d hotdb-pg hotdb-migrate hotdb-exporter
  for _ in $(seq 1 60); do
    st=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' hotdb-migrate 2>/dev/null || echo "none 1")
    case "$st" in "exited 0") echo "  마이그레이션 완료"; break ;; exited*) echo "  마이그레이션 실패"; docker logs --tail 40 hotdb-migrate; exit 1 ;; esac
    sleep 3
  done
else
  echo "HotDB ${HOTDB_HOST}:${HOTDB_PORT} 연결 확인 ..."
  if ! nc -z -G 3 "$HOTDB_HOST" "$HOTDB_PORT"; then
    echo "  ${HOTDB_HOST}:${HOTDB_PORT} 에 못 붙습니다 — HotDB 서버가 아직 없으니 모니터링(Prometheus · Grafana · 수집기)만 띄웁니다"
    services=("${monitoring[@]}")
  fi
fi
export HOTDB_ADDR="${HOTDB_ADDR:-${HOTDB_HOST}:${HOTDB_PORT}}"
docker compose "${files[@]}" up -d --no-deps "${services[@]}"
echo
docker compose "${files[@]}" ps
cat <<EOF

Grafana     http://localhost:${GRAFANA_PORT:-59400}   (HotDB 상태 / HotDB CDC)
$([ $local_db = 1 ] && echo "HotDB       localhost:${HOTDB_PUBLIC_PORT:-59433}  db=hotdb  (postgres/postgres)")
Prometheus  http://localhost:${PROMETHEUS_PORT:-59490}
판별 모듈   http://localhost:59481 (asm) · 59482 (oft) · 59483 (pnt) · 59484 (mch)  /actuator/health
RFC Service http://localhost:59485/actuator/health   DB Agent http://localhost:59486/actuator/health
발행기      http://localhost:59480/sim
EOF
