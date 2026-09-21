#!/usr/bin/env bash
# 전체 스택 기동. 루트 docker-compose.yml 하나가 include 로 DB·모니터링·소비자를 모두 끌어온다.
#
# Kafka 는 이 스택 밖(Aspire 세션)에 있다. 소비자를 Aspire 네트워크에 넣으면 Aspire 가
# 5초 안에 떼어 내므로, 반대로 Kafka 컨테이너를 이쪽 네트워크(tsdb-net)에 alias "kafka" 로
# 붙인다. 브로커가 광고하는 내부 주소가 kafka:9093 이라 alias 이름은 반드시 kafka 여야 한다.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

# kafka.env 의 KAFKA_CONTAINER (비어 있으면 자동 탐지)
kafka_ctr="${KAFKA_CONTAINER:-$(sed -n 's/^KAFKA_CONTAINER=//p' "$root/kafka.env" | tr -d '[:space:]')}"
if [ -z "$kafka_ctr" ]; then
  kafka_ctr="$($engine ps --format '{{.Names}}' | grep -E '^kafka-[a-z0-9]+$' | head -1 || true)"
fi

# 1) 기동. compose 파일을 -f 로 여러 개 올리지 않는다 — 프로젝트가 갈라져 충돌한다.
$engine compose -f "$root/docker-compose.yml" up -d --build

# 2) Kafka 컨테이너를 tsdb-net 에 붙인다 (이미 붙어 있으면 건너뛴다).
#    소비자는 그 사이 kafka:9093 해석에 실패하며 재시도하다가 붙는 즉시 소비를 시작한다.
if [ -n "$kafka_ctr" ]; then
  if $engine network inspect tsdb-net --format '{{range .Containers}}{{.Name}} {{end}}' | grep -qw "$kafka_ctr"; then
    echo "Kafka 컨테이너 $kafka_ctr 는 이미 tsdb-net 에 있다"
  else
    $engine network connect tsdb-net "$kafka_ctr" --alias kafka
    echo "Kafka 컨테이너 $kafka_ctr → tsdb-net (alias kafka)"
  fi
else
  echo "!! 떠 있는 Kafka 컨테이너(kafka-xxxxxxxx)를 못 찾았다. Aspire 를 띄운 뒤 다시 실행하거나" >&2
  echo "   $engine network connect tsdb-net <컨테이너> --alias kafka 를 손으로 실행하라." >&2
fi

cat <<EOS

── 기동 완료 ─────────────────────────────────────
  TimescaleDB  : localhost:59432 (lidar / postgres:postgres)
  lidar-ingest : http://localhost:59080/metrics
  Prometheus   : http://localhost:59090
  Grafana      : http://localhost:59380  (admin/admin)
  Kafka        : ${kafka_ctr:-<not found>} (kafka:9093 on tsdb-net)
EOS
