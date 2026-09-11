#!/usr/bin/env python3
"""
계약 검사 — 브리지가 넣은 레코드를 수신 측 규칙 그대로 읽어 본다.

`src/timescaledb/dev/ingest/ingest.py` 의 parse_record · parse_item 이 실제로 요구하는 것만
본다. 여기를 통과하면 그 소비자가 받아서 표에 넣을 수 있다.

  · 헤더 11개가 순서까지 맞는가 (EesHeaderBuilderV1_0)
  · Key 가 project_id(=토픽 이름)인가
  · value 가 비어 있지 않은 배열이고, 항목마다 content 가 있는가
  · content.timestamp 가 ns epoch · tid 가 숫자 문자열 · value 가 JSON 문자열 · pm_mode 가 false 인가
  · raw_payload 에 채널별 필수 키가 있는가
  · tid 가 태그 조회표로 풀리는가 (안 풀리면 수신 측에서 장비 축이 '#숫자' 가 된다)

    podman exec ees-bridge python check_contract.py

채널마다 레코드 하나씩만 본다. 계약은 레코드 단위로 같은 모양이라 한 건이면 드러난다.
"""
import json
import os
import sys
import time

from confluent_kafka import Consumer

CHANNELS = ("status", "actual", "artifact")
HEADERS = ["project_id", "data_type", "method_id", "msg_timestamp", "infra_proc_name",
           "task_area_code", "message_version", "uniqueid", "request", "reply", "replycluster"]
# ingest.py 의 세 빌더가 반드시 찾는 키
REQUIRED = {
    "status": ("device_role", "site", "zone", "status", "occurred_at", "idempotency_key"),
    "actual": ("zone", "stage", "scan_id", "occurred_at", "idempotency_key", "event_type"),
    "artifact": ("scan_id", "artifact_type", "occurred_at"),
}
NS_MIN = 10 ** 15

CATALOG = os.environ.get("TAG_CATALOG", "/app/tags/tag-catalog.json")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9093")
TIMEOUT_S = int(os.environ.get("CHECK_TIMEOUT_SECONDS", "40"))

catalog = json.load(open(CATALOG, encoding="utf-8"))["tags"]
by_param = {}
for tag_id, (send_topic, param_id) in catalog.items():
    by_param[(send_topic, param_id)] = tag_id

# 오프셋을 커밋하지 않는 임시 그룹이다 — 몇 번을 돌려도 늘 처음부터 읽는다.
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "ees-bridge-contract-check",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe(["ot.lidar.%s" % ch for ch in CHANNELS])

seen, items_ok, unresolved, problems = {}, 0, 0, []
end = time.time() + TIMEOUT_S
while time.time() < end and len(seen) < 3:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    topic = msg.topic()
    channel = topic.rsplit(".", 1)[-1]
    if channel in seen:
        continue

    got = {k: (v.decode() if v else "") for k, v in (msg.headers() or [])}
    if [k for k, _ in (msg.headers() or [])] != HEADERS:
        problems.append(f"{topic}: 헤더 순서/구성이 다르다 → {[k for k,_ in msg.headers()]}")
    for key, expect in (("data_type", "value"), ("method_id", "TRACE"),
                        ("message_version", "1.0"), ("project_id", topic), ("request", topic)):
        if got.get(key) != expect:
            problems.append(f"{topic}: 헤더 {key}={got.get(key)!r} (기대 {expect!r})")
    if not (msg.key() or b"").decode() == topic:
        problems.append(f"{topic}: Key={msg.key()!r} (기대 {topic!r})")
    if not got.get("msg_timestamp", "").isdigit() or int(got["msg_timestamp"]) < NS_MIN:
        problems.append(f"{topic}: msg_timestamp 가 ns epoch 가 아니다")

    body = json.loads(msg.value())
    if not isinstance(body, list) or not body:
        problems.append(f"{topic}: value 가 비어 있지 않은 배열이 아니다")
        continue
    for item in body:
        content = item.get("content")
        if not isinstance(content, dict):
            problems.append(f"{topic}: content 객체가 없다"); break
        ts, tid, value, pm = content.get("timestamp"), content.get("tid"), content.get("value"), content.get("pm_mode")
        if not isinstance(ts, int) or ts < NS_MIN:
            problems.append(f"{topic}: content.timestamp 가 ns epoch 가 아니다 ({ts})"); break
        if not isinstance(tid, str) or not tid.isdigit():
            problems.append(f"{topic}: content.tid 가 숫자 문자열이 아니다 ({tid!r})"); break
        if not isinstance(value, str):
            problems.append(f"{topic}: content.value 가 문자열이 아니다"); break
        if pm is not False:
            problems.append(f"{topic}: content.pm_mode 가 false 가 아니다 ({pm!r})"); break
        raw = json.loads(value)
        missing = [k for k in REQUIRED[channel] if k not in raw]
        if missing:
            problems.append(f"{topic}: raw_payload 에 {missing} 이(가) 없다"); break
        if (topic, int(tid)) not in by_param:
            unresolved += 1
        items_ok += 1
    seen[channel] = (msg.offset(), len(body))

c.close()
for ch in CHANNELS:
    if ch in seen:
        print(f"  ot.lidar.{ch:<9} offset {seen[ch][0]:>6} · 항목 {seen[ch][1]:>4}개  OK")
    else:
        print(f"  ot.lidar.{ch:<9} 레코드를 못 읽었다")
print(f"  검사한 항목 {items_ok}개 · 조회표가 못 푼 tid {unresolved}개")
if not seen:
    print("  !! 레코드를 한 건도 못 읽었다 — 발행기가 돌고 있는지 확인하라")
    sys.exit(1)
if problems:
    print("  !! 계약 위반")
    for p in problems[:10]:
        print("    -", p)
    sys.exit(1)
print("  계약 위반 없음")
