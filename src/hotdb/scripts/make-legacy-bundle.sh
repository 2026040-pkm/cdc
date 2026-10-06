#!/usr/bin/env bash
# 레거시 DB 서버용 묶음을 만든다 → dist/legacy-bundle/ + dist/hotdb-legacy-bundle.tar.gz
#   들어가는 것: compose.legacy.yml(레거시 DB 사본 · 마이그레이션 · exporter) · compose.legacy-sim.yml(SAP 대역 · Oracle 대역) ·
#                db/legacy-db · scripts/legacy-sim.py · exporter 쿼리 · hotdb-migrate 이미지(amd64 · arm64 tar) + 빌드 재료(Dockerfile · jar) · README
#   그 서버에 docker(또는 podman) 와 python3 만 있으면 된다. 공개 이미지(timescaledb · oracle-free · postgres-exporter)는
#   거기서 pull 한다 (--with-public-images 면 그것까지 tar 에 넣는다 — 2GB 가까이 커진다).
set -euo pipefail
cd "$(dirname "$0")/.."
with_public=0; [ "${1:-}" = --with-public-images ] && with_public=1

jar=app/hotdb-migrate/build/libs/app.jar
[ -f "$jar" ] || { echo "$jar 가 없습니다 — 먼저 scripts/up-services.sh 로 jar 를 만드세요"; exit 1; }

out=dist/legacy-bundle
rm -rf "$out"; mkdir -p "$out/app" "$out/db" "$out/scripts" "$out/monitoring/exporter" "$out/images"
cp compose.legacy.yml compose.legacy-sim.yml compose.monitoring.yml "$out/"
cp -R db/legacy-db "$out/db/"
cp scripts/legacy-sim.py "$out/scripts/"
cp monitoring/exporter/queries-legacy.yaml "$out/monitoring/exporter/"
cp app/Dockerfile "$out/app/"

# hotdb-migrate · legacy-simulator 이미지 — 받는 쪽 CPU 를 몰라 둘 다
for mod in hotdb-migrate legacy-simulator; do
  [ -f "app/$mod/build/libs/app.jar" ] || { echo "app/$mod/build/libs/app.jar 가 없습니다"; exit 1; }
  mkdir -p "$out/app/$mod/build/libs"; cp "app/$mod/build/libs/app.jar" "$out/app/$mod/build/libs/"
  img=localhost/hotdb/$mod:local
  for arch in amd64 arm64; do
    docker buildx build --platform "linux/$arch" --build-arg MODULE=$mod -t "$img" --load app -q >/dev/null
    docker save "$img" | gzip > "$out/images/$mod-$arch.tar.gz"
    echo "images/$mod-$arch.tar.gz"
  done
  docker buildx build --build-arg MODULE=$mod -t "$img" --load app -q >/dev/null   # 로컬은 원래 아키텍처로 되돌린다
done

if [ $with_public = 1 ]; then
  for i in docker.io/timescale/timescaledb:2.29.2-pg17 docker.io/gvenzl/oracle-free:23-slim quay.io/prometheuscommunity/postgres-exporter:v0.17.1; do
    docker pull --platform linux/amd64 -q "$i" >/dev/null
    docker save "$i" | gzip > "$out/images/$(echo "$i" | sed 's#.*/##; s#:#-#').tar.gz"
    echo "images/$(echo "$i" | sed 's#.*/##; s#:#-#').tar.gz"
  done
fi

cat > "$out/.env" <<'EOF'
# 레거시 DB 서버 — 밖으로 여는 포트 (RFC Service · DB Agent 가 붙는다). 바꾸면 서비스 쪽 .env 도 맞춘다
LEGACY_PUBLIC_PORT=59435      # 레거시 DB 사본 (erp · mes · lgs · geo · ops.poll_state)
LEGACY_EXPORTER_PORT=59491    # postgres_exporter (Prometheus 가 읽는다)
SAP_SIM_PORT=59434            # SAP 대역 (PostgreSQL, sapsim/sapsim)
ORACLE_SIM_PORT=59521         # Oracle 대역 (FREEPDB1, legacy_reader/legacy_reader)
LEGACY_ADMIN_PASSWORD=postgres
LEGACY_SHARED_BUFFERS=128MB
NODE_EXPORTER_PORT=59489     # 호스트 지표 수집기
CADVISOR_PORT=59492           # 컨테이너 지표 수집기 (docker). podman 이면 PODMAN_EXPORTER_PORT=59488
EOF

cat > "$out/up.sh" <<'EOF'
#!/usr/bin/env bash
# 레거시 DB 셋을 띄우고 원천 표를 채운다. docker(compose v2 · docker-compose) 또는 podman.
#   ./up.sh [up|down [-v]|ps|logs|mutate [행]]        ※ sh up.sh 가 아니라 ./up.sh 또는 bash up.sh
[ -n "${BASH_VERSION:-}" ] || exec bash "$0" "$@"
set -euo pipefail; cd "$(dirname "$0")"

# ── 컨테이너 CLI · compose 고르기 ────────────────────────────────────────
CLI=${CONTAINER_CLI:-}
if [ -z "$CLI" ]; then
  if command -v podman >/dev/null 2>&1; then CLI=podman
  elif command -v docker >/dev/null 2>&1; then CLI=docker
  else echo "docker 나 podman 이 없습니다"; exit 1; fi
fi
SUDO=""
if [ "$CLI" = docker ] && ! docker info >/dev/null 2>&1; then
  if sudo -n docker info >/dev/null 2>&1; then SUDO="sudo"; echo "docker 는 sudo 로 씁니다 (영구: sudo usermod -aG docker $USER 후 재로그인)"
  else echo "docker 데몬에 못 붙습니다 — 데몬이 떠 있는지, 이 계정이 docker 그룹인지 (sudo usermod -aG docker $USER)"; exit 1; fi
fi
# compose 플러그인이 없으면 GitHub 릴리스에서 받아 cli-plugins 에 넣는다 (sudo 없이는 ~/.docker 아래)
install_compose() {
  v=v2.29.7; case "$(uname -m)" in x86_64|amd64) a=x86_64 ;; aarch64|arm64) a=aarch64 ;; *) a=x86_64 ;; esac
  url="https://github.com/docker/compose/releases/download/$v/docker-compose-linux-$a"
  if [ -n "$SUDO" ]; then dir=/usr/local/lib/docker/cli-plugins; else dir="$HOME/.docker/cli-plugins"; fi
  echo "docker compose 플러그인이 없어 설치합니다 → $dir/docker-compose ($v)"
  $SUDO mkdir -p "$dir" && $SUDO curl -fsSL "$url" -o "$dir/docker-compose" && $SUDO chmod +x "$dir/docker-compose"
}
if [ "$CLI" = docker ]; then
  if $SUDO docker compose version >/dev/null 2>&1; then C="$SUDO docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then C="$SUDO docker-compose"
  elif install_compose && $SUDO docker compose version >/dev/null 2>&1; then C="$SUDO docker compose"
  else echo "docker compose 를 못 구했습니다 — Ubuntu: sudo apt install docker-compose-v2 (docker.io 패키지) 또는 docker-compose-plugin (Docker 저장소)"; exit 1; fi
else
  if podman compose version >/dev/null 2>&1; then C="podman compose"
  elif command -v podman-compose >/dev/null 2>&1; then C="podman-compose"
  else echo "podman compose 가 없습니다 (pip install podman-compose)"; exit 1; fi
fi
PY=$(command -v python3 || command -v python || true)
CLI="$SUDO $CLI"
files="-f compose.legacy.yml -f compose.legacy-sim.yml"
mon="-f compose.monitoring.yml"

# 오래된 compose 는 --wait 가 없다 → 직접 healthy 를 기다린다
wait_healthy() {
  for c in "$@"; do
    printf '%s 준비 대기 ' "$c"
    for _ in $(seq 1 120); do
      s=$($CLI inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$c" 2>/dev/null || echo none)
      case "$s" in healthy|running) echo " $s"; continue 2 ;; exited|dead) echo " $s"; $CLI logs --tail 30 "$c"; exit 1 ;; esac
      printf '.'; sleep 5
    done
    echo " 시간 초과"; $CLI logs --tail 30 "$c"; exit 1
  done
}
load_images() {
  arch=$(uname -m); case "$arch" in x86_64|amd64) a=amd64 ;; aarch64|arm64) a=arm64 ;; *) a=amd64 ;; esac
  for mod in hotdb-migrate legacy-simulator; do
    [ -f "images/$mod-$a.tar.gz" ] || continue
    if ! $CLI image inspect "localhost/hotdb/$mod:local" >/dev/null 2>&1; then
      echo "$mod 이미지 load ($a)"; gunzip -c "images/$mod-$a.tar.gz" | $CLI load
    fi
  done
  for f in images/*.tar.gz; do
    case "$f" in *hotdb-migrate*|*legacy-simulator*) ;; *) echo "$f load"; gunzip -c "$f" | $CLI load ;; esac
  done
}

case "${1:-up}" in
  up)
    echo "컨테이너: $CLI · compose: $C"
    load_images
    echo "SAP · Oracle 대역 기동 (Oracle 은 처음에 1~2분) ..."
    $C $files up -d sap-sim oracle-sim
    wait_healthy hotdb-sap-sim hotdb-oracle-sim
    if [ "$($CLI exec hotdb-sap-sim psql -U sapsim -d sapsim -Atc "SELECT to_regclass('erpsrc.item') IS NOT NULL" 2>/dev/null)" != t ]; then
      [ -n "$PY" ] || { echo "python3 이 없습니다 — 원천 표 시드(scripts/legacy-sim.py)에 필요합니다"; exit 1; }
      CONTAINER_CLI="${CLI# }" "$PY" scripts/legacy-sim.py setup
    fi
    $C $files up -d
    wait_healthy hotdb-legacy-db
    if [ "${CLI# }" = docker ] || [ "$SUDO" = sudo ]; then $C $mon up -d node-exporter cadvisor; else $C $mon up -d node-exporter podman-exporter; fi
    echo; echo "레거시 DB 마이그레이션 대기 ..."
    for _ in $(seq 1 60); do
      s=$($CLI inspect -f '{{.State.Status}} {{.State.ExitCode}}' hotdb-legacy-migrate 2>/dev/null || echo "none 1")
      case "$s" in "exited 0") echo "마이그레이션 완료 (표 178)"; break ;; exited*) echo "마이그레이션 실패"; $CLI logs --tail 40 hotdb-legacy-migrate; exit 1 ;; esac
      sleep 3
    done
    echo; $C $files $mon ps
    echo
    echo "서비스 PC 의 .env 에 적을 것 (이 서버 IP 로):"
    echo "  LEGACY_HOST=<IP>  SAP_URL=jdbc:postgresql://<IP>:59434/sapsim  ORACLE_URL=jdbc:oracle:thin:@//<IP>:59521/FREEPDB1" ;;
  down) shift; $C $files $mon down "$@" ;;
  ps)   $C $files $mon ps ;;
  logs) shift; $C $files $mon logs --tail=100 "$@" ;;
  mutate) CONTAINER_CLI="${CLI# }" "$PY" scripts/legacy-sim.py mutate "${2:-5}" ;;
  *) echo "사용: $0 [up|down [-v]|ps|logs|mutate [행]]"; exit 2 ;;
esac
EOF
chmod +x "$out/up.sh"

cat > "$out/up.ps1" <<'EOF'
# Windows(podman 또는 docker) 용 — up.sh 와 같다.  .\up.ps1 [up|down|ps|mutate]
param([string]$Action = "up")
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot
$cli = if (Get-Command podman -ErrorAction SilentlyContinue) { "podman" } else { "docker" }
if ($env:CONTAINER_CLI) { $cli = $env:CONTAINER_CLI }
$env:CONTAINER_CLI = $cli
$files = @("-f", "compose.legacy.yml", "-f", "compose.legacy-sim.yml")
switch ($Action) {
  "up" {
    $arch = if ($env:PROCESSOR_ARCHITECTURE -match "ARM") { "arm64" } else { "amd64" }
    & $cli image inspect localhost/hotdb/hotdb-migrate:local 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { & $cli load -i "images\hotdb-migrate-$arch.tar.gz" }
    Get-ChildItem images\*.tar.gz | Where-Object { $_.Name -notmatch "hotdb-migrate" } | ForEach-Object { & $cli load -i $_.FullName }
    Write-Host "SAP · Oracle 대역 기동 (Oracle 은 처음에 1~2분) ..."
    & $cli compose @files up -d --wait sap-sim oracle-sim
    $has = & $cli exec hotdb-sap-sim psql -U sapsim -d sapsim -Atc "SELECT to_regclass('erpsrc.item') IS NOT NULL" 2>$null
    if ("$has".Trim() -ne "t") { python scripts\legacy-sim.py setup }
    & $cli compose @files up -d
    $exp = if ($cli -eq "docker") { "cadvisor" } else { "podman-exporter" }
    & $cli compose -f compose.monitoring.yml up -d node-exporter $exp
    & $cli compose @files -f compose.monitoring.yml ps
  }
  "down"   { & $cli compose @files down }
  "ps"     { & $cli compose @files ps }
  "mutate" { python scripts\legacy-sim.py mutate }
}
EOF

cat > "$out/README.md" <<'EOF'
# 레거시 DB 서버 묶음

HotDB 서버 to 서버 시험에서 **레거시 쪽** 셋을 한 서버에 띄운다 (`src/hotdb/README.md` 의 "서버 to 서버").

| 컨테이너 | 역할 | 포트(기본) | 계정 |
|---|---|---|---|
| hotdb-legacy-db (+ legacy-migrate · legacy-exporter) | 레거시 DB 사본 — erp · mes · lgs · geo · `ops.poll_state`. RFC Service · DB Agent 가 **쓴다** | 59435 (exporter 59491) | postgres/postgres |
| hotdb-sap-sim | SAP 대역 (PostgreSQL) — 원천 `erpsrc.*` · 송신 대상 Z 표. RFC Service 가 **읽고 쓴다** | 59434 | sapsim/sapsim |
| hotdb-oracle-sim | Oracle 대역 (Oracle Free 23) — 소유자 MES · LGS · GEO 64표. DB Agent 가 **읽는다** | 59521 (`FREEPDB1`) | legacy_reader/legacy_reader · system/hotdb_sim_sys |
| hotdb-legacy-simulator | 레거시 발행기 — 위 두 대역의 원천 표를 주기(10초)마다 바꿔 폴링이 가져갈 변경분을 만든다. `/sim` · `POST /sim/speed?value=5` · `/sim/pause` | 59493 | — |
| hotdb-node-exporter · hotdb-cadvisor | 호스트 · 컨테이너 지표 (서비스 PC 의 Prometheus 가 읽는다) | 59489 · 59492 | — |

## 올리기

필요한 것: docker (compose v2) 또는 podman, python3. 공개 이미지 셋은 인터넷에서 받는다
(`timescale/timescaledb:2.29.2-pg17` · `gvenzl/oracle-free:23-slim` · `postgres-exporter:v0.17.1`). hotdb-migrate 이미지는 `images/` 에 있다.

```bash
./up.sh            # Linux · macOS     (이미지 load → SAP · Oracle 대역 → 원천 표 시드 → 레거시 DB + 마이그레이션)
.\up.ps1           # Windows PowerShell
./up.sh ps         # 상태 — legacy-migrate 가 Exited (0) 이면 사본 표 178개 준비 끝
./up.sh mutate     # 원천 몇 행을 한 번 고쳐 증분 폴링 확인 (발행기가 돌고 있으면 필요 없다)
./up.sh down [-v]  # 내리기 (-v 면 데이터까지)
```

처음 Oracle 기동은 1~2분 걸린다. 시드(`scripts/legacy-sim.py setup`)는 SAP 대역에 표가 없을 때만 돈다.

## 서비스 쪽에 알려 줄 것

이 서버의 IP 와 위 포트들(59434 · 59435 · 59521 · 59489 · 59491 · 59492 · 59493)이 서비스 PC 에서 열려 있어야 한다(방화벽). 서비스 PC 의 `src/hotdb/.env`:

```
LEGACY_HOST=<이 서버 IP>
LEGACY_PORT=59435
SAP_URL=jdbc:postgresql://<이 서버 IP>:59434/sapsim
ORACLE_URL=jdbc:oracle:thin:@//<이 서버 IP>:59521/FREEPDB1
```

이미지를 직접 다시 만들려면 `docker compose -f compose.legacy.yml build` (`app/Dockerfile` + jar 가 들어 있다).
EOF

tar czf dist/hotdb-legacy-bundle.tar.gz -C dist legacy-bundle
echo; du -sh "$out" dist/hotdb-legacy-bundle.tar.gz; echo; find "$out" -type f | sort
