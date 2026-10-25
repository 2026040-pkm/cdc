#!/usr/bin/env bash
# 한 대에 전부 (docker) — Hot DB · 레거시 DB · SAP 대역 · Oracle 대역 · 판별 모듈 4 · RFC Service · DB Agent · 발행기 둘 · 모니터링.
# scripts/up.ps1 (Windows · podman) 과 같은 일을 docker 로 한다. 서버 to 서버(wssh)는 scripts/up-services.sh + .env.wssh.
#
#   scripts/up-local.sh                # jar · 이미지 빌드 + 기동 (레거시 대역 원천 표가 비어 있으면 시드)
#   scripts/up-local.sh --skip-build   # 빌드 없이
#   scripts/up-local.sh down [-v]      # 내리기 (-v 면 DB · 오프셋 볼륨까지 — 처음부터)
#   scripts/up-local.sh ps | logs [svc]
#
# .env 의 LEGACY_HOST=legacy-db · SAP_URL=…sap-sim… · ORACLE_URL=…oracle-sim… 이어야 한다 (컨테이너 이름으로 붙는다).
# RFC Service 의 hot 적재 원천은 inshoplocal(docker inshoplocal-pg, 54433) — host.docker.internal 로 붙는다.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { echo ".env 가 없습니다 — .env.example 을 복사하세요"; exit 1; }
set -a; . ./.env; set +a
export HOTDB_HOST=hotdb-pg HOTDB_PORT=5432 LEGACY_HOST=legacy-db LEGACY_PORT=5432
export HOTDB_ADDR=hotdb-pg:5432

files=(-f compose.db.yml -f compose.legacy.yml -f compose.zone.yml -f compose.rfc.yml -f compose.agent.yml
       -f compose.legacy-sim.yml -f compose.sim.yml -f compose.monitoring.yml -f compose.local.yml)
dc() { docker compose "${files[@]}" "$@"; }

build=1
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-build) build=0 ;;
    down) shift; exec docker compose "${files[@]}" down "$@" ;;
    ps)   exec docker compose "${files[@]}" ps ;;
    logs) shift; exec docker compose "${files[@]}" logs --tail=100 -f "$@" ;;
    *) echo "모르는 인자: $1"; exit 2 ;;
  esac; shift
done

# ── 1. jar · 이미지 ─────────────────────────────────────────────────────
if [ $build = 1 ]; then
  if [ -z "${JAVA_HOME:-}" ] || ! "$JAVA_HOME/bin/java" -version 2>&1 | grep -q '"21'; then
    JAVA_HOME=$(/usr/libexec/java_home -v 21 2>/dev/null || ls -d ~/.jdks/*21*/Contents/Home 2>/dev/null | head -1 || true)
  fi
  [ -n "${JAVA_HOME:-}" ] || { echo "JDK 21 이 없습니다"; exit 1; }
  export JAVA_HOME
  echo "gradle bootJar ($JAVA_HOME) ..."
  (cd app && ./gradlew bootJar --no-daemon -q)
  echo "docker compose build ..."
  dc build hotdb-migrate zone-asm rfc-service db-agent field-simulator legacy-simulator
fi

# ── 2. Prometheus 대상 — 한 대 주소 ─────────────────────────────────────
t=monitoring/prometheus/targets
echo '[ { "targets": ["hotdb-exporter:9187"], "labels": { "server": "local" } } ]' > $t/hotdb.json
echo '[ { "targets": ["legacy-exporter:9187"], "labels": { "server": "local" } } ]' > $t/legacy-db.json
echo '[ { "targets": ["node-exporter:9100"], "labels": { "server": "local", "hotdb_db": "1" } } ]' > $t/node.json
echo '[ { "targets": ["cadvisor:8080"], "labels": { "server": "local" } } ]' > $t/podman.json
echo '[ { "targets": ["legacy-simulator:8080"], "labels": { "server": "local" } } ]' > $t/legacy-simulator.json

# ── 3. DB 셋 + 마이그레이션 ─────────────────────────────────────────────
echo "Hot DB · 레거시 DB · SAP 대역 · Oracle 대역 기동 (Oracle 은 처음에 1~2분) ..."
dc up -d --wait hotdb-pg legacy-db sap-sim oracle-sim
dc up -d hotdb-migrate legacy-migrate
for c in hotdb-migrate hotdb-legacy-migrate; do
  for _ in $(seq 1 60); do
    st=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' $c 2>/dev/null || echo "none 1")
    case "$st" in "exited 0") echo "  $c 완료"; break ;; exited*) echo "  $c 실패"; docker logs --tail 40 $c; exit 1 ;; esac
    sleep 3
  done
done

# ── 4. 레거시 대역 원천 표 시드 (비어 있을 때만) ────────────────────────
has=$(docker exec hotdb-sap-sim psql -U sapsim -d sapsim -Atc "SELECT to_regclass('erpsrc.zhotdb_progress') IS NOT NULL" 2>/dev/null || true)
if [ "$has" != "t" ]; then
  echo "레거시 대역 시드 (scripts/legacy-sim.py setup) ..."
  CONTAINER_CLI=docker python3 scripts/legacy-sim.py setup
fi

# ── 5. 서비스 ───────────────────────────────────────────────────────────
dc up -d hotdb-exporter legacy-exporter zone-asm zone-oft zone-pnt zone-mch rfc-service db-agent field-simulator \
  legacy-simulator prometheus grafana node-exporter cadvisor
echo
dc ps
cat <<EOF

Grafana     http://localhost:${GRAFANA_PORT:-59400}   (HotDB 상태 / HotDB CDC)
HotDB       localhost:${HOTDB_PUBLIC_PORT:-59433}  db=hotdb   (postgres/postgres)
레거시 DB   localhost:59435  db=legacy  (postgres/postgres)
SAP 대역    localhost:59434  db=sapsim  (sapsim/sapsim) — 실적 Z 표 erpsrc.zhotdb_progress
Oracle 대역 localhost:59521/FREEPDB1 (legacy_reader/legacy_reader)
판별 모듈   http://localhost:59481 (asm) · 59482 (oft) · 59483 (pnt) · 59484 (mch)  /actuator/health
RFC Service http://localhost:59485/actuator/health   DB Agent http://localhost:59486/actuator/health
발행기      http://localhost:59480/sim   레거시 발행기 http://localhost:59493/sim
EOF
