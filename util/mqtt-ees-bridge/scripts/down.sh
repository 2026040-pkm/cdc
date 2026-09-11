#!/usr/bin/env bash
# 스택 정지. -v 면 볼륨(Kafka 로그·자동 태그 상태)까지 지운다.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

# tsdb-net 에 붙여 뒀으면 먼저 뗀다 — 안 그러면 네트워크가 남아 다음 기동에서 꼬인다.
$engine network disconnect tsdb-net ees-kafka >/dev/null 2>&1 && echo "ees-kafka 를 tsdb-net 에서 뗐다" || true

if [ "${1:-}" = "-v" ]; then
  $engine compose -f "$root/docker-compose.yml" down -v
  echo "볼륨까지 삭제했다 (Kafka 로그·자동 태그 번호가 사라진다)"
else
  $engine compose -f "$root/docker-compose.yml" down
fi
