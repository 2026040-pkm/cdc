#!/usr/bin/env bash
# latest 스택 전체 기동 — 순서가 있다.
#
#   1) timescaledb        원천. 네트워크 tsdb-net 을 만들고 Kafka 를 거기 붙인다
#   2) embedded-cdc-tsdb  그 tsdb-net 에 external 로 붙는다. 1 이 없으면 기동 자체가 막힌다
#
# 두 스택을 compose 하나로 합치지 않은 이유는 ../README.md 를 볼 것.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

echo "── 1/2 · timescaledb (원천) ──────────────────────"
bash "$root/timescaledb/scripts/up.sh"

echo
echo "── 2/2 · embedded-cdc-tsdb (CDC) ─────────────────"
bash "$root/embedded-cdc-tsdb/scripts/up.sh"
