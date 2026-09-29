#!/usr/bin/env python3
"""
InterSysLink 의 태그 등록부 → `lidar_tag_catalog` 시드 SQL.

Kafka 레코드의 `content.tid` 는 **숫자**다 (예 "135" · "2257"). 문자열 TagId
(`LDR-GJ-A1B3-07.ot_device_assembly_lidar_status.raw_payload`) 가 아니다 — EES Kafka
Provider 가 메시지에 등록된 태그의 ParameterId 를 싣기 때문이다. 그리고 raw_payload
어디에도 장비 id 필드가 없다(발행기 `util/mqtt-lidar-sim/lidar-sim.cs` 의 세 빌더 확인).

즉 **이 표가 없으면 장비 축이 통째로 사라진다** — `lidar_device_state`, 대시보드의
`$tid` 변수, 장비별 온도·스캔 속도 추이가 전부 숫자만 남는다.

매핑 원본은 InterSysLink 의 SQLite 다.

    EES_MESSAGE.ConfigJson.key      = Kafka 토픽 (ot.lidar.status …)
    EES_TAG.MessageId               → 그 메시지
    EES_TAG.ParameterId             = content.tid (숫자)
    EES_TAG.TagCatalogId            → TAG_CATALOG.Id
    TAG_CATALOG.TagCatalogId        = 문자열 TagId
    TAG_CATALOG.Attributes.deviceId = 장비 id (산출물은 -REGISTERED_PCD 같은 접미사가 붙는다)
    TAG_CATALOG.Attributes.topic    = MQTT 토픽

ParameterId 는 (workflowId, PType) 안에서만 1부터 매기는 일련번호다 — 등록 화면에서 손으로
바꿀 수도 있다(`EesTagRegistrationDraftService.GetNextId`). 지금 데이터는 세 메시지가
1~2310 으로 겹치지 않지만 계약이 그렇지는 않으므로 키를 (send_topic, param_id) 로 잡는다.

SQLite 를 직접 읽는 것은 이 PoC 가 같은 PC 에서 돌기 때문이다. Engine 이 원격이면 같은 값을
REST 로 받을 수 있다 — `GET /api/workflows/{workflowId}/ees-messages/{messageId}/ees-tag-settings`
(항목에 TagId 와 ParameterId 가 같이 들어 있다) 또는 `ees-tag` CSV 내보내기.

사용:

    python scripts/export-tag-catalog.py                    # 기본 경로에서 읽어 init SQL 갱신
    python scripts/export-tag-catalog.py --db <경로.db> --out <경로.sql>

태그를 다시 임포트했으면 이 스크립트를 다시 돌리고 `scripts/down.sh -v` 로 볼륨을 지워야
반영된다(init SQL 은 볼륨이 처음 만들어질 때만 돈다).
"""
import argparse
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_DB = Path(r"D:\git\intersyslink-v4\src\Engine\Engine\bin\Debug\net10.0\isl-secondary.db")
DEFAULT_OUT = HERE.parent / "infra" / "db" / "init" / "03-tag-catalog.sql"

# 산출물 채널의 태그 id 접미사. 장비 축(tid)에는 이걸 뗀 값을 넣는다.
ARTIFACT_TYPES = ("REGISTERED_PCD", "TRANSFORMATION_MATRIX", "SEGMENTED_PCD")

CHANNELS = ("status", "actual", "artifact")


def channel_of(mqtt_topic):
    """MQTT 토픽의 마지막 마디가 채널이다. 토픽 깊이가 채널마다 다르므로 끝만 본다."""
    last = mqtt_topic.rstrip("/").rsplit("/", 1)[-1].lower()
    return last if last in CHANNELS else None


def split_artifact(device_id):
    """LDR-…-01-SEGMENTED_PCD → (LDR-…-01, SEGMENTED_PCD). 접미사가 없으면 (그대로, None)."""
    for suffix in ARTIFACT_TYPES:
        tail = "-" + suffix
        if device_id.endswith(tail):
            return device_id[: -len(tail)], suffix
    return device_id, None


def quote(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def read_rows(db_path):
    # 엔진이 돌고 있으면 파일이 잠겨 있다. 복사본을 읽는다.
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / "isl.db"
        shutil.copy(db_path, copy)
        for wal in (".db-wal", ".db-shm"):
            side = Path(str(db_path)[: -len(".db")] + wal)
            if side.exists():
                shutil.copy(side, Path(str(copy)[: -len(".db")] + wal))
        conn = sqlite3.connect(f"file:{copy}?mode=ro", uri=True)
        try:
            topics = {}
            for message_id, config in conn.execute("SELECT Id, ConfigJson FROM EES_MESSAGE"):
                try:
                    topics[message_id] = json.loads(config).get("key")
                except (TypeError, ValueError):
                    topics[message_id] = None

            rows = []
            query = """
                SELECT t.MessageId, t.ParameterId, k.TagCatalogId, k.Attributes, k.DataType
                FROM EES_TAG t
                JOIN TAG_CATALOG k ON k.Id = t.TagCatalogId
                WHERE COALESCE(k.IsDeleted, 0) = 0
            """
            for message_id, param_id, tag_id, attributes, data_type in conn.execute(query):
                send_topic = topics.get(message_id)
                if not send_topic:
                    continue
                try:
                    attrs = json.loads(attributes or "{}")
                except (TypeError, ValueError):
                    attrs = {}
                mqtt_topic = attrs.get("topic")
                device_id = attrs.get("deviceId")
                if not mqtt_topic or not device_id:
                    continue
                channel = channel_of(mqtt_topic)
                if channel is None:
                    continue
                tid, artifact_type = split_artifact(device_id)
                # TagId 의 가운데 조각. 예전 계약(문자열 tid)에서 채널 판정에 쓰던 값이라
                # 표에 남겨 두면 옛 자료와 대조가 된다.
                mqtt_topic_key = mqtt_topic.strip("/").replace("/", "_")
                rows.append({
                    "send_topic": send_topic,
                    "param_id": int(param_id),
                    "tag_id": tag_id,
                    "device_id": device_id,
                    "tid": tid,
                    "channel": channel,
                    "artifact_type": artifact_type,
                    "mqtt_topic": mqtt_topic,
                    "mqtt_topic_key": mqtt_topic_key,
                    "field": attrs.get("address"),
                    "data_type": data_type,
                })
            return rows
        finally:
            conn.close()


def main():
    # Windows 기본 콘솔 코드페이지(cp949)로는 이 파일의 안내문이 안 찍힌다.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=os.environ.get("ISL_DB", str(DEFAULT_DB)),
                        help="InterSysLink SQLite 경로 (기본: 엔진 bin 의 isl-secondary.db)")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="쓸 SQL 파일")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"!! {db_path} 가 없다. --db 로 경로를 주거나 InterSysLink 를 한 번 띄워라.", file=sys.stderr)
        return 1

    rows = read_rows(db_path)
    if not rows:
        print("!! 태그가 하나도 안 나왔다. 그 DB 에 EES 메시지/태그가 등록돼 있는지 확인하라.", file=sys.stderr)
        return 1
    rows.sort(key=lambda r: (r["send_topic"], r["param_id"]))

    per_channel = {}
    for row in rows:
        per_channel[row["channel"]] = per_channel.get(row["channel"], 0) + 1
    devices = len({r["tid"] for r in rows})
    generated = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    cols = ("send_topic, param_id, tag_id, device_id, tid, channel, artifact_type, "
            "mqtt_topic, mqtt_topic_key, field, data_type")
    values = ",\n".join(
        "    ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            quote(r["send_topic"]), r["param_id"], quote(r["tag_id"]), quote(r["device_id"]),
            quote(r["tid"]), quote(r["channel"]), quote(r["artifact_type"]),
            quote(r["mqtt_topic"]), quote(r["mqtt_topic_key"]), quote(r["field"]), quote(r["data_type"]),
        )
        for r in rows
    )

    header = f"""-- ─────────────────────────────────────────────────────────────────────────────
-- lidar_tag_catalog 시드 — 자동 생성 파일. 손으로 고치지 않는다.
--
--   생성 : scripts/export-tag-catalog.py
--   원본 : {db_path}
--   시각 : {generated}
--   태그 : {len(rows)}개 ({', '.join(f'{k} {v}' for k, v in sorted(per_channel.items()))}) · 장비 {devices}대
--
-- 이 표가 Kafka 의 숫자 tid(content.tid = EES ParameterId)를 장비 id 로 되돌린다.
-- 없으면 장비 축이 사라진다 — 배경은 스크립트 상단 주석 참고.
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO rdb.lidar_tag_catalog ({cols}) VALUES
{values}
ON CONFLICT (send_topic, param_id) DO UPDATE SET
    tag_id = EXCLUDED.tag_id,
    device_id = EXCLUDED.device_id,
    tid = EXCLUDED.tid,
    channel = EXCLUDED.channel,
    artifact_type = EXCLUDED.artifact_type,
    mqtt_topic = EXCLUDED.mqtt_topic,
    mqtt_topic_key = EXCLUDED.mqtt_topic_key,
    field = EXCLUDED.field,
    data_type = EXCLUDED.data_type,
    updated_at = now();
"""
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(header, encoding="utf-8")
    print(f"{out} — 태그 {len(rows)}개 · 장비 {devices}대 "
          f"({', '.join(f'{k} {v}' for k, v in sorted(per_channel.items()))})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
