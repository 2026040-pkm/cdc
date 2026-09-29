#!/usr/bin/env bash
# latest 스택 전체 정지 — 기동의 역순이다.
# 수신(CDC)을 먼저 내려야 원천 네트워크(tsdb-net)를 지울 수 있다.
#
#   down.sh       데이터 유지
#   down.sh -v    볼륨까지 삭제 (init SQL 재실행. CDC 슬롯·오프셋도 같이 정리된다)
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"

echo "── 1/2 · embedded-cdc-tsdb ───────────────────────"
bash "$root/embedded-cdc-tsdb/scripts/down.sh" "$@"

echo
echo "── 2/2 · timescaledb ─────────────────────────────"
bash "$root/timescaledb/scripts/down.sh" "$@"
