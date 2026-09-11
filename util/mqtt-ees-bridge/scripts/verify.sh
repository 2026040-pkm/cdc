#!/usr/bin/env bash
# 흐르고 있는지 확인한다. 토픽 끝 오프셋 · 브리지 지표 · 레코드 한 건의 모양.
set -uo pipefail
# Git Bash 는 /opt/kafka/... 를 윈도 경로로 바꿔 버린다. 컨테이너 안 경로라 그러면 안 된다.
export MSYS_NO_PATHCONV=1
root="$(cd "$(dirname "$0")/.." && pwd)"

engine=docker
command -v docker >/dev/null 2>&1 || engine=podman

topics="ot.lidar.status ot.lidar.actual ot.lidar.artifact"

echo "── Kafka 오프셋 ──────────────────────────────────"
for t in $topics; do
  $engine exec ees-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9093 --topic "$t" 2>/dev/null | sed 's/^/  /'
done

echo "── 브리지 지표 ───────────────────────────────────"
curl -s http://localhost:61090/metrics \
  | grep -E '^ees_bridge_(mqtt_messages_total|mqtt_drops_total|records_total|items_total|delivered_total|delivery_errors_total|tag_catalog_rows|tag_auto_rows|tag_unregistered_total|mqtt_connected)' \
  | sed 's/^/  /'

echo "── 계약 검사 (수신 측 규칙 그대로) ───────────────"
$engine exec ees-bridge python check_contract.py

echo "── 레코드 한 건 (ot.lidar.status) ────────────────"
$engine exec ees-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9093 --topic ot.lidar.status \
  --max-messages 1 --timeout-ms 10000 --property print.headers=true 2>/dev/null \
  | cut -c1-1200 | sed 's/^/  /'

echo "── 브리지가 직접 번호를 매긴 태그 ────────────────"
$engine exec ees-bridge sh -c 'python - <<PY
import json,pathlib
p=pathlib.Path("/state/auto-tags.json")
if not p.exists():
    print("  없음 (등록부로 전부 풀렸다)")
else:
    tags=json.loads(p.read_text(encoding="utf-8"))["tags"]
    print(f"  {len(tags)}개 — /state/auto-tags.sql 로 수신 측 카탈로그를 채울 수 있다")
    for k in list(tags)[:5]:
        print(f"    {k} → {tags[k]}")
PY' 2>/dev/null
