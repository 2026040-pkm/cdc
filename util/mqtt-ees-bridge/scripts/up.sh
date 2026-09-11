#!/usr/bin/env bash
# 스택 기동. Kafka + Kafka UI + 브리지가 한 번에 뜬다.
#
#   scripts/up.sh          # 기동만 한다
#   scripts/up.sh --tsdb   # 수신 측(src/timescaledb)이 이 Kafka 를 읽도록 tsdb-net 에 붙인다
#
# --tsdb 는 tsdb-net 에 이미 붙어 있는 Aspire Kafka(kafka-xxxxxxxx)를 떼고 이쪽을 alias
# kafka 로 붙인다. 같은 네트워크에 alias 가 둘이면 소비자가 어느 쪽에 붙을지 알 수 없다.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

attach_tsdb=0
[ "${1:-}" = "--tsdb" ] && attach_tsdb=1

$engine compose -f "$root/docker-compose.yml" up -d --build

if [ "$attach_tsdb" = "1" ]; then
  if ! $engine network exists tsdb-net 2>/dev/null && ! $engine network inspect tsdb-net >/dev/null 2>&1; then
    echo "!! tsdb-net 이 없다. src/timescaledb 스택을 먼저 올려라." >&2
  else
    members="$($engine network inspect tsdb-net --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null || true)"
    for ctr in $members; do
      case "$ctr" in
        kafka-*) $engine network disconnect tsdb-net "$ctr" && echo "Aspire Kafka $ctr 를 tsdb-net 에서 뗐다";;
      esac
    done
    if echo "$members" | grep -qw ees-kafka; then
      echo "ees-kafka 는 이미 tsdb-net 에 있다"
    else
      $engine network connect tsdb-net ees-kafka --alias kafka
      echo "ees-kafka → tsdb-net (alias kafka)"
    fi
    $engine restart tsdb-lidar-ingest >/dev/null 2>&1 && echo "tsdb-lidar-ingest 재기동 (새 브로커로 붙는다)" || true
  fi
fi

cat <<EOS

── 기동 완료 ─────────────────────────────────────
  Kafka      : localhost:61092   (컨테이너끼리는 kafka:9093)
  Kafka UI   : http://localhost:61080
  브리지 지표 : http://localhost:61090/metrics
  MQTT 원천   : ${MQTT_HOST:-host.docker.internal}:${MQTT_PORT:-1884} (EMQX)

  발행기를 돌려야 데이터가 흐른다 — util/mqtt-lidar-sim/1-publish.cmd
EOS
