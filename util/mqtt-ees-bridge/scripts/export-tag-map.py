#!/usr/bin/env python3
"""
태그 등록부 → 브리지가 읽는 조회표(`tags/tag-catalog.json`).

브리지는 MQTT 메시지 하나를 EES 항목 하나로 바꿀 때 **숫자 ParameterId** 가 필요하다.
그 숫자를 정하는 것은 InterSysLink 의 태그 등록부이지 브리지가 아니다 — 여기서 지어내면
같은 장비가 ISL 이 보낼 때와 브리지가 보낼 때 서로 다른 tid 로 나가고, 수신 측
`lidar_tag_catalog` 가 통째로 어긋난다.

    MQTT  {"id": "LDR-GJ-A1B3-07", "raw_payload": {...}}  on ot/device/assembly/lidar/status
      → TagId  LDR-GJ-A1B3-07.ot_device_assembly_lidar_status.raw_payload
      → 이 표  ("ot.lidar.status", 1)
      → EES   {"content": {"timestamp": …, "tid": "1", "value": "{…}", "pm_mode": false}}

TagId 는 `{id}.{토픽의 / 를 _ 로 접은 것}.raw_payload` 다 (`lidar-sim.cs` 의 TagCatalog 와
UI 의 serializeTopicTagFile 이 같은 규칙을 쓴다). 그래서 표의 키를 TagId 하나로 잡았다.

원본은 둘 중 하나다.

  1. `src/timescaledb/infra/db/init/03-tag-catalog.sql`  (기본)
     이미 저장소에 있는 생성물이라 ISL 이 없어도 돈다. 수신 측 카탈로그와 같은 파일에서
     나오므로 양쪽이 어긋날 수 없다.
  2. `--db <isl-secondary.db>`
     ISL 등록부를 다시 임포트했을 때. 읽는 방식은
     `src/timescaledb/scripts/export-tag-catalog.py` 와 같다.

사용:

    python scripts/export-tag-map.py
    python scripts/export-tag-map.py --db "D:\\git\\intersyslink-v4\\...\\isl-secondary.db"
"""
import argparse
import json
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DEFAULT_SQL = ROOT.parent.parent / "src" / "timescaledb" / "infra" / "db" / "init" / "03-tag-catalog.sql"
DEFAULT_OUT = ROOT / "tags" / "tag-catalog.json"
DEFAULT_DB = Path(r"D:\git\intersyslink-v4\src\Engine\Engine\bin\Debug\net10.0\isl-secondary.db")

CHANNELS = ("status", "actual", "artifact")

# 03-tag-catalog.sql 의 VALUES 한 줄. 생성 파일이라 모양이 고정돼 있다.
#   ('ot.lidar.actual', 1401, 'TagId', 'deviceId', 'tid', 'actual', NULL, 'ot/…', 'ot_…', 'raw_payload', 'STRING')
ROW_RE = re.compile(
    r"\(\s*'([^']*)'\s*,\s*(\d+)\s*,\s*'((?:[^']|'')*)'\s*,"      # send_topic, param_id, tag_id
    r"\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*,\s*'([^']*)'" # device_id, tid, channel
)


def channel_of(mqtt_topic):
    """MQTT 토픽의 마지막 마디가 채널이다. 채널마다 토픽 깊이가 달라 끝만 본다."""
    last = mqtt_topic.rstrip("/").rsplit("/", 1)[-1].lower()
    return last if last in CHANNELS else None


def from_sql(path):
    """03-tag-catalog.sql → {TagId: [send_topic, param_id]}"""
    text = path.read_text(encoding="utf-8")
    tags = {}
    for send_topic, param_id, tag_id, _device_id, _tid, channel in ROW_RE.findall(text):
        if channel not in CHANNELS:
            continue
        tags[tag_id.replace("''", "'")] = [send_topic, int(param_id)]
    return tags


def from_sqlite(path):
    """ISL 의 SQLite 등록부 → {TagId: [send_topic, param_id]}"""
    with tempfile.TemporaryDirectory() as tmp:
        # 엔진이 돌고 있으면 파일이 잠겨 있다. 복사본을 읽는다.
        copy = Path(tmp) / "isl.db"
        shutil.copy(path, copy)
        for side in ("-wal", "-shm"):
            src = Path(str(path) + side)
            if src.exists():
                shutil.copy(src, Path(str(copy) + side))
        conn = sqlite3.connect(f"file:{copy}?mode=ro", uri=True)
        try:
            topics = {}
            for message_id, config in conn.execute("SELECT Id, ConfigJson FROM EES_MESSAGE"):
                try:
                    topics[message_id] = json.loads(config).get("key")
                except (TypeError, ValueError):
                    topics[message_id] = None

            tags = {}
            query = """
                SELECT t.MessageId, t.ParameterId, k.TagCatalogId, k.Attributes
                FROM EES_TAG t
                JOIN TAG_CATALOG k ON k.Id = t.TagCatalogId
                WHERE COALESCE(k.IsDeleted, 0) = 0
            """
            for message_id, param_id, tag_id, attributes in conn.execute(query):
                send_topic = topics.get(message_id)
                if not send_topic:
                    continue
                try:
                    attrs = json.loads(attributes or "{}")
                except (TypeError, ValueError):
                    attrs = {}
                if channel_of(attrs.get("topic") or "") is None:
                    continue
                tags[tag_id] = [send_topic, int(param_id)]
            return tags
        finally:
            conn.close()


def summarize(tags):
    """send_topic 별 id 수(산출물은 장비 하나가 종류마다 다른 id 다). 등록 누락을 눈으로 잡으라고 찍는다."""
    per_topic = {}
    for tag_id, (send_topic, _param) in tags.items():
        per_topic.setdefault(send_topic, set()).add(tag_id.split(".", 1)[0])
    return {topic: len(ids) for topic, ids in sorted(per_topic.items())}


def main():
    ap = argparse.ArgumentParser(description="태그 등록부 → 브리지 조회표")
    ap.add_argument("--sql", type=Path, default=DEFAULT_SQL, help=f"기본: {DEFAULT_SQL}")
    ap.add_argument("--db", type=Path, default=None, help=f"ISL SQLite 에서 직접 읽는다 (예: {DEFAULT_DB})")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"기본: {DEFAULT_OUT}")
    args = ap.parse_args()

    if args.db is not None:
        if not args.db.exists():
            sys.exit(f"SQLite 를 못 찾았다: {args.db}")
        tags, source = from_sqlite(args.db), str(args.db)
    else:
        if not args.sql.exists():
            sys.exit(f"카탈로그 SQL 을 못 찾았다: {args.sql}")
        tags, source = from_sql(args.sql), str(args.sql)

    if not tags:
        sys.exit("태그를 한 줄도 못 읽었다 — 원본 모양이 바뀌었는지 확인하라.")

    doc = {
        "generated_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "source": source,
        "count": len(tags),
        "tags": dict(sorted(tags.items())),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

    print(f"{args.out}  ({len(tags)}개 태그)")
    for topic, devices in summarize(tags).items():
        print(f"  {topic:<20} id {devices}개")


if __name__ == "__main__":
    main()
