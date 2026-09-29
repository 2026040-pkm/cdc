#!/usr/bin/env bash
# 정지. 기본은 데이터 유지, `down.sh -v` 면 볼륨까지 지운다 (init SQL 재실행 필요할 때).
#
# up 이 Kafka 컨테이너를 tsdb-net 에 붙여 두었으므로 먼저 떼어 낸다. 안 떼면
# compose 가 네트워크를 지우지 못해 "network is being used" 로 끝난다.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

if $engine network inspect tsdb-net >/dev/null 2>&1; then
  for ctr in $($engine network inspect tsdb-net --format '{{range .Containers}}{{.Name}} {{end}}'); do
    case "$ctr" in
      tsdb-*) ;;   # 이 스택의 컨테이너는 compose 가 지운다
      *) $engine network disconnect tsdb-net "$ctr" && echo "tsdb-net 에서 분리: $ctr" ;;
    esac
  done
fi

$engine compose -f "$root/docker-compose.yml" down "$@"
