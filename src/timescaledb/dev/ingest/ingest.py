#!/usr/bin/env python3
"""
InterSysLink(OT) LiDAR 필드 데이터 Kafka → TimescaleDB 소비자.

레코드 하나의 값은 태그 여러 개의 값 배열이다 (EES Kafka Provider · data_type=value · V1.0).

    [{"content": {"timestamp": <ns epoch>,
                  "tid": "135",
                  "value": "{\"device_role\":\"LIDAR\",\"status\":\"ONLINE\",...}",   # JSON 문자열
                  "pm_mode": false}}, ...]

채널은 **Kafka 토픽**이 정한다. EES 메시지 하나가 토픽 하나이고, 채널마다 메시지를 따로
만들어 두었다 (msgHeaderFormat.topic.send_topic). 토픽 이름의 마지막 마디로 고른다.

    ot.lidar.status    ← ot/device/{zone}/lidar/status              → lidar_status        (1건/1초·대)
    ot.lidar.actual    ← ot/sensor/{stage}/actual                   → lidar_scan_actual   (1건/1분·대)
    ot.lidar.artifact  ← ot/pipeline/{zone}/{shop}/{bay}/artifact   → lidar_scan_artifact (12건/1분·대)

**content.tid 는 TagId 가 아니라 EES ParameterId(숫자)다.** Provider 가 태그를 실을 때
문자열 TagId 를 버리고 이 번호만 쓴다 (ValueMessageFormatterV1_0). raw_payload 안에도 장비 id
필드는 없다 — 즉 Kafka 만 읽어서는 어느 장비인지 알 수 없다. 그래서 InterSysLink 등록부를
옮겨 둔 lidar_tag_catalog 를 기동할 때 통째로 읽어 (토픽, 숫자) → 장비 id 로 푼다.
그 표를 채우는 것은 scripts/export-tag-catalog.py 다.

카탈로그에 없는 숫자를 만나도 멈추지 않는다. 표를 다시 읽어 보고(레이트 리밋), 그래도 없으면
상태 채널은 idempotency_key 앞부분(LDR-…:20260910T151724)에서 장비 id 를 뽑고, 그마저 없으면
'#<숫자>' 를 장비 축으로 둔다 — 적재를 멈추는 대신 미해석을 눈에 보이게 남긴다.

tagMode=raw 라 content.value 는 raw_payload 통째의 JSON 문자열이다 — 채널마다 키가 다르다.

하는 일은 다섯이다.
  1. 배열을 풀어 항목마다(채널은 토픽이 정한다) 해당 표에 한 행 (멱등 키 충돌은 DO NOTHING)
  2. 배치 안의 장비별 최신 상태로 lidar_device_state UPSERT (오래된 이벤트는 못 덮는다)
  3. 레코드 자체를 lidar_status_message 한 행 (헤더·오프셋·채널별 항목 수)
  4. 파싱이 안 되거나 모르는 토픽인 항목은 lidar_ingest_reject 로 격리 — 나머지는 그대로 적재
  5. 지표 노출 (/metrics)

전달 보장: DB 트랜잭션이 커밋된 뒤에만 Kafka 오프셋을 커밋한다 (at-least-once).
재전달로 같은 항목이 다시 와도 (idempotency_key, time) 유니크 인덱스가 걸러 낸다.
DB 쓰기가 실패하면 오프셋을 커밋하지 않고 같은 배치를 다시 시도한다.
"""
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg
from confluent_kafka import Consumer, KafkaException
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ── 설정 ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9093")
# 쉼표로 여러 개를 받는다. 채널 하나에 토픽 하나이고, 분류는 토픽 이름의 마지막 마디로 한다
# (status · actual · artifact). 토픽 이름이 바뀌어도 끝 마디만 맞으면 그대로 동작한다.
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ot.lidar.status,ot.lidar.actual,ot.lidar.artifact")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "tsdb-lidar-ingest")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")
PG_DSN = os.environ.get("PG_DSN", "postgresql://postgres:postgres@timescaledb:5432/lidar")
BATCH_MAX_MESSAGES = int(os.environ.get("BATCH_MAX_MESSAGES", "50"))
BATCH_MAX_WAIT_S = int(os.environ.get("BATCH_MAX_WAIT_MS", "1000")) / 1000.0
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
# 카탈로그에 없는 숫자 tid 를 만났을 때 표를 다시 읽어 보는 최소 간격.
# 태그를 추가 등록하면 그 사이 들어온 항목은 미해석으로 적재되고 이 주기 뒤부터 풀린다.
TAG_CATALOG_REFRESH_S = int(os.environ.get("TAG_CATALOG_REFRESH_SECONDS", "60"))

TOPICS = [t.strip() for t in KAFKA_TOPIC.split(",") if t.strip()]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("lidar-ingest")

# ── 지표 ────────────────────────────────────────────────────────────────────
MESSAGES = Counter("lidar_ingest_messages_total", "Kafka records consumed")
ITEMS = Counter("lidar_ingest_items_total", "items parsed from records", ["channel"])
ROWS_INSERTED = Counter("lidar_ingest_rows_inserted_total", "rows inserted (all tables)")
ROWS_DUPLICATE = Counter("lidar_ingest_rows_duplicate_total", "rows skipped by the idempotency index")
TABLE_ROWS = Counter("lidar_ingest_table_rows_total", "rows inserted per table", ["table"])
REJECTS = Counter("lidar_ingest_rejects_total", "items quarantined into lidar_ingest_reject", ["reason"])
DB_ERRORS = Counter("lidar_ingest_db_errors_total", "failed DB batch writes (retried)")
KAFKA_ERRORS = Counter("lidar_ingest_kafka_errors_total", "consume/commit failures (retried, not fatal)")
BATCHES = Counter("lidar_ingest_batches_total", "DB batches committed")
BATCH_SECONDS = Histogram(
    "lidar_ingest_batch_seconds", "DB write + commit time per batch",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
BATCH_ROWS = Histogram(
    "lidar_ingest_batch_rows", "rows per DB batch",
    buckets=(100, 200, 500, 1000, 2000, 5000, 10000, 20000),
)
END_TO_END = Histogram(
    "lidar_ingest_end_to_end_seconds", "device content.timestamp → DB commit",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300, 1800),
)
TAG_CATALOG_SIZE = Gauge("lidar_ingest_tag_catalog_rows", "rows loaded from lidar_tag_catalog")
TAG_UNRESOLVED = Counter(
    "lidar_ingest_tag_unresolved_total",
    "items whose content.tid was not in lidar_tag_catalog (device axis fell back)", ["channel"])
TAG_CHANNEL_MISMATCH = Counter(
    "lidar_ingest_tag_channel_mismatch_total",
    "items whose catalog channel disagreed with the Kafka topic (EES message misconfigured)")
KAFKA_LAG = Gauge("lidar_ingest_kafka_lag", "high watermark - consumer position", ["partition"])
LAST_COMMIT = Gauge("lidar_ingest_last_commit_timestamp_seconds", "unix time of the last DB+Kafka commit")
LAST_EVENT = Gauge("lidar_ingest_last_event_timestamp_seconds", "newest content.timestamp seen (unix seconds)")

# ── 채널 ────────────────────────────────────────────────────────────────────
# Kafka 토픽 이름의 마지막 마디로 판정한다. '.' 과 '-' 를 모두 구분자로 보므로
# ot.lidar.status 든 ot-lidar-status 든 같은 채널로 잡힌다.
CH_STATUS = "status"
CH_ACTUAL = "actual"
CH_ARTIFACT = "artifact"
CHANNELS = (CH_STATUS, CH_ACTUAL, CH_ARTIFACT)

# 산출물은 장비 하나가 종류마다 다른 id 로 발행한다 (LDR-…-01-SEGMENTED_PCD).
# 장비 축으로 보려면 접미사를 떼야 한다.
ARTIFACT_TYPES = ("REGISTERED_PCD", "TRANSFORMATION_MATRIX", "SEGMENTED_PCD")

# ── SQL ─────────────────────────────────────────────────────────────────────
# json_populate_recordset 이 JSON 키를 컬럼에 맞춰 캐스팅한다. timestamptz 문자열은
# PostgreSQL 이 직접 파싱하므로 (7자리 소수초도 받는다) 파이썬에서 날짜를 다루지 않는다.
STATUS_COLS = (
    "time, tid, tag_id, param_id, mqtt_topic, device_role, site, zone, shop, bay, status, error_code, "
    "scan_rate_pts_per_sec, temperature_c, connectivity_rssi, fov_mode, last_heartbeat_at, "
    "ingested_at, content_ts, pm_mode, idempotency_key, kafka_offset"
)
ACTUAL_COLS = (
    "time, tid, tag_id, param_id, mqtt_topic, site, zone, shop, bay, stage, record_type, input_method, "
    "source_system, hull_no, block_id, scan_id, scanned_at, pan_tilt, edge_pc, inference_ws, "
    "vision_ocr, event_type, block_progress_rate, reference_cad_id, match_confidence, "
    "model_version, ingested_at, content_ts, pm_mode, idempotency_key, kafka_offset"
)
ARTIFACT_COLS = (
    "time, tid, tag_id, param_id, mqtt_topic, scan_id, artifact_type, hull_no, block_id, segment_id, "
    "storage_uri, file_size_bytes, checksum, transformation_matrix, produced_by_device_id, "
    "model_version, ingested_at, content_ts, pm_mode, idempotency_key, kafka_offset"
)


def insert_sql(table, cols):
    return f"""
        INSERT INTO {table} ({cols})
        SELECT {cols} FROM json_populate_recordset(NULL::{table}, %s::json)
        ON CONFLICT (idempotency_key, time) DO NOTHING
    """


INSERTS = {
    CH_STATUS: ("lidar_status", insert_sql("lidar_status", STATUS_COLS)),
    CH_ACTUAL: ("lidar_scan_actual", insert_sql("lidar_scan_actual", ACTUAL_COLS)),
    CH_ARTIFACT: ("lidar_scan_artifact", insert_sql("lidar_scan_artifact", ARTIFACT_COLS)),
}

STATE_COLS = (
    "tid, device_role, site, zone, shop, bay, status, error_code, scan_rate_pts_per_sec, "
    "temperature_c, connectivity_rssi, fov_mode, last_event_at, last_heartbeat_at"
)
UPSERT_STATE = f"""
    INSERT INTO lidar_device_state ({STATE_COLS}, updated_at)
    SELECT {STATE_COLS}, now() FROM json_populate_recordset(NULL::lidar_device_state, %s::json)
    ON CONFLICT (tid) DO UPDATE SET
        device_role = EXCLUDED.device_role,
        site = EXCLUDED.site,
        zone = EXCLUDED.zone,
        shop = EXCLUDED.shop,
        bay = EXCLUDED.bay,
        status = EXCLUDED.status,
        error_code = EXCLUDED.error_code,
        scan_rate_pts_per_sec = EXCLUDED.scan_rate_pts_per_sec,
        temperature_c = EXCLUDED.temperature_c,
        connectivity_rssi = EXCLUDED.connectivity_rssi,
        fov_mode = EXCLUDED.fov_mode,
        last_event_at = EXCLUDED.last_event_at,
        last_heartbeat_at = EXCLUDED.last_heartbeat_at,
        updated_at = now()
    WHERE EXCLUDED.last_event_at > lidar_device_state.last_event_at
"""
INSERT_MESSAGE = """
    INSERT INTO lidar_status_message
        (kafka_partition, kafka_offset, kafka_ts, kafka_topic, uniqueid, msg_ts, message_version,
         method_id, data_type, project_id, infra_proc_name, task_area_code, send_topic,
         item_count, status_count, actual_count, artifact_count, reject_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""
INSERT_REJECT = """
    INSERT INTO lidar_ingest_reject (kafka_offset, payload, reason) VALUES (%s, %s::jsonb, %s)
"""

# ── 변환 ────────────────────────────────────────────────────────────────────
# 발신 측(EesHeaderBuilderV1_0.ToUnixTimestampOrSeconds)은 ns epoch 를 쓰되 Int64 를 넘치면
# 초로 떨어뜨린다. 그 경계(2262년)를 자릿수로 가른다 — 초 값은 10자리, ns 값은 19자리다.
NS_MIN = 10 ** 15


def ns_to_dt(value):
    """ns epoch(또는 초) → tz-aware datetime (µs 로 절삭. timestamptz 정밀도가 µs 다)."""
    value = int(value)
    if abs(value) < NS_MIN:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    sec, rem = divmod(value, 1_000_000_000)
    return datetime.fromtimestamp(sec, tz=timezone.utc) + timedelta(microseconds=rem // 1000)


def decode_headers(raw):
    """Kafka 헤더 [(key, bytes|None), ...] → dict[str, str]. 빈 값은 None 으로 접는다."""
    out = {}
    for key, val in raw or []:
        if val is None:
            out[key] = None
        else:
            text = val.decode("utf-8", "replace") if isinstance(val, (bytes, bytearray)) else str(val)
            out[key] = text or None
    return out


class Reject(Exception):
    def __init__(self, reason, payload):
        super().__init__(reason)
        self.reason = reason
        self.payload = payload


def split_tag_id(tag_id):
    """
    문자열 TagId → (장비 id, 토픽 조각, 필드). 토픽의 '/' 가 '_' 로 접혀 있어 늘 정확히
    세 조각이다 (MqttTagId 주석 참고).

    지금 계약에서는 이 형태가 오지 않는다 — content.tid 는 숫자다. EES 설정을 되돌려
    문자열 TagId 가 다시 실려 오는 경우를 위해 남겨 둔 경로다.
    """
    parts = tag_id.split(".")
    if len(parts) != 3 or not all(parts):
        return None
    return parts[0], parts[1], parts[2]


def channel_of_topic(topic):
    """Kafka 토픽 이름의 마지막 마디로 채널을 고른다. 모르는 토픽이면 None."""
    if not topic:
        return None
    last = topic.replace("-", ".").rsplit(".", 1)[-1].strip().lower()
    return last if last in CHANNELS else None


class TagCatalog:
    """
    lidar_tag_catalog 를 통째로 들고 있는 조회표. (send_topic, param_id) → 태그 한 줄.

    Kafka 는 숫자만 싣고 장비 id 를 싣지 않는다. 그 숫자를 장비로 되돌리는 유일한 근거가
    이 표다. 2,310행 수준이라 통째로 메모리에 둔다 — 항목마다 DB 를 때리면 초당 426개
    레코드 × 항목 수만큼 조회가 생긴다.

    없는 숫자를 만나면 표를 다시 읽어 본다(최소 간격 TAG_CATALOG_REFRESH_S). 태그를 새로
    등록한 직후를 위한 것이지, 매번 확인하려는 것이 아니다.
    """

    def __init__(self):
        self._by_key = {}
        self._by_param = {}     # param_id → 행. 같은 숫자가 여러 토픽에 있으면 None 을 넣어 둔다
        self._last_load = 0.0
        self._miss_pending = False

    def load(self, conn):
        by_key, by_param = {}, {}
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT send_topic, param_id, tag_id, tid, mqtt_topic_key, channel "
                    "FROM lidar_tag_catalog"
                )
                for send_topic, param_id, tag_id, tid, topic_key, channel in cur:
                    row = (tag_id, tid, topic_key, channel)
                    key = int(param_id)
                    by_key[(send_topic, key)] = row
                    # 같은 숫자가 토픽마다 다른 태그를 가리킬 수 있다. 그런 숫자는
                    # 토픽 없이 찾을 수 없으므로 None 으로 막아 둔다.
                    by_param[key] = row if key not in by_param else None
        finally:
            # 읽기만 했다. autocommit=False 라 트랜잭션이 열린 채로 남는 것을 막는다.
            conn.rollback()
        self._by_key, self._by_param = by_key, by_param
        self._last_load = time.monotonic()
        self._miss_pending = False
        TAG_CATALOG_SIZE.set(len(by_key))
        if by_key:
            log.info("태그 카탈로그 %d행 적재 (장비 %d대)",
                     len(by_key), len({row[1] for row in by_key.values()}))
        else:
            log.warning("태그 카탈로그가 비어 있다 — 장비 축이 '#숫자' 로만 남는다. "
                        "scripts/export-tag-catalog.py 로 03-tag-catalog.sql 을 만든 뒤 "
                        "down.sh -v 로 다시 올려라.")

    def resolve(self, send_topic, param_id):
        """(토픽, 숫자) → (tag_id, tid, mqtt_topic_key, channel). 못 찾으면 None."""
        row = self._by_key.get((send_topic, param_id))
        if row is not None:
            return row
        # 토픽 이름만 바뀐 경우(발행 설정 변경)를 위한 뒷문. 숫자가 유일할 때만 쓴다.
        row = self._by_param.get(param_id)
        if row is None:
            self._miss_pending = True
        return row

    def should_reload(self):
        return self._miss_pending and (time.monotonic() - self._last_load) >= TAG_CATALOG_REFRESH_S


def fallback_tid(channel, value, param_id):
    """
    카탈로그가 못 푼 항목의 장비 축.

    상태 채널만 페이로드에서 장비 id 를 되찾을 수 있다 — idempotency_key 가
    "{장비 id}:{yyyyMMddTHHmmss}" 이기 때문이다. 실적은 "{scan_id 앞 8자}:{stage}:{event}" 라
    장비가 없고, 산출물은 멱등 키 자체가 없다. 그 둘은 '#숫자' 로 남긴다 —
    틀린 장비를 지어내는 것보다 안 푼 것이 드러나는 편이 낫다.
    """
    if channel == CH_STATUS:
        key = value.get("idempotency_key")
        if isinstance(key, str) and ":" in key:
            head = key.split(":", 1)[0].strip()
            if head:
                return head
    return f"#{param_id}" if param_id is not None else "#unknown"


def device_of(artifact_id):
    """LDR-…-01-SEGMENTED_PCD → LDR-…-01. 모르는 접미사면 그대로 둔다."""
    for suffix in ARTIFACT_TYPES:
        tail = "-" + suffix
        if artifact_id.endswith(tail):
            return artifact_id[: -len(tail)]
    return artifact_id


def num(value):
    """숫자만 통과시킨다. 문자열로 온 숫자는 DB 가 캐스팅하므로 그대로 둔다."""
    return value if isinstance(value, (int, float, str)) and not isinstance(value, bool) else None


def require(value, fields, item):
    for field in fields:
        if not value.get(field):
            raise Reject(f"missing {field}", item)


def status_row(value, item):
    require(value, ("status", "occurred_at", "idempotency_key"), item)
    return {
        "time": value["occurred_at"],
        "device_role": value.get("device_role"),
        "site": value.get("site"),
        "zone": value.get("zone"),
        "shop": value.get("shop"),
        "bay": value.get("bay"),
        "status": str(value["status"]),
        "error_code": value.get("error_code") or None,      # '' · null → NULL
        "scan_rate_pts_per_sec": num(value.get("scan_rate_pts_per_sec")),
        "temperature_c": num(value.get("temperature_c")),
        "connectivity_rssi": num(value.get("connectivity_rssi")),
        "fov_mode": value.get("fov_mode"),
        "last_heartbeat_at": value.get("last_heartbeat_at") or None,
        "idempotency_key": str(value["idempotency_key"]),
    }


def actual_row(value, item):
    require(value, ("occurred_at", "scan_id", "idempotency_key"), item)
    return {
        "time": value["occurred_at"],
        "site": value.get("site"),
        "zone": value.get("zone"),
        "shop": value.get("shop"),
        "bay": value.get("bay"),
        "stage": value.get("stage"),
        "record_type": value.get("record_type"),
        "input_method": value.get("input_method"),
        "source_system": value.get("source_system"),
        "hull_no": value.get("hull_no"),
        "block_id": value.get("block_id"),
        "scan_id": str(value["scan_id"]),
        "scanned_at": value.get("scanned_at") or None,
        "pan_tilt": value.get("pan_tilt"),
        "edge_pc": value.get("edge_pc"),
        "inference_ws": value.get("inference_ws"),
        "vision_ocr": value.get("vision_ocr") or None,
        "event_type": value.get("event_type"),
        "block_progress_rate": num(value.get("block_progress_rate")),
        "reference_cad_id": value.get("reference_cad_id"),
        "match_confidence": num(value.get("match_confidence")),
        "model_version": value.get("model_version"),
        "idempotency_key": str(value["idempotency_key"]),
    }


def artifact_row(value, item):
    require(value, ("occurred_at", "scan_id", "artifact_type"), item)
    scan_id = str(value["scan_id"])
    artifact_type = str(value["artifact_type"])
    segment_id = value.get("segment_id") or None
    return {
        "time": value["occurred_at"],
        # 이 채널만 발신 측 멱등 키가 없다. 한 스캔 안에서 (종류, 세그먼트) 가 유일하므로
        # 그걸로 만든다 — 재전달이 와도 같은 키가 나와 유니크 인덱스가 거른다.
        "idempotency_key": f"{scan_id}:{artifact_type}:{segment_id or '-'}",
        "scan_id": scan_id,
        "artifact_type": artifact_type,
        "hull_no": value.get("hull_no"),
        "block_id": value.get("block_id"),
        "segment_id": segment_id,
        "storage_uri": value.get("storage_uri") or None,
        "file_size_bytes": num(value.get("file_size_bytes")),
        "checksum": value.get("checksum") or None,
        "transformation_matrix": value.get("transformation_matrix"),
        "produced_by_device_id": value.get("produced_by_device_id"),
        "model_version": value.get("model_version"),
    }


BUILDERS = {CH_STATUS: status_row, CH_ACTUAL: actual_row, CH_ARTIFACT: artifact_row}


def parse_item(item, channel, send_topic, kafka_offset, catalog):
    """
    배열 원소 하나 → (행 dict, content_ts). 깨졌으면 Reject.

    채널은 이미 Kafka 토픽이 정했다(레코드 단위). 여기서는 숫자 tid 를 카탈로그로 풀어
    장비 축을 붙이는 일만 한다.
    """
    if not isinstance(item, dict) or not isinstance(item.get("content"), dict):
        raise Reject("no content object", item)
    c = item["content"]
    raw_tid = c.get("tid")
    if raw_tid is None or raw_tid == "":
        raise Reject("missing tid", item)
    raw_tid = str(raw_tid).strip()

    param_id, tid, tag_id, topic_key = None, None, None, None
    try:
        param_id = int(raw_tid)
    except ValueError:
        # 숫자가 아니면 옛 계약(문자열 TagId)일 수 있다. 그 형태면 그대로 쓴다.
        parts = split_tag_id(raw_tid)
        if parts is None:
            raise Reject("tid is neither an EES parameter id nor a {device}.{topic}.{field} TagId", item)
        device_id, topic_key, field = parts
        # 세 채널은 tagMode=raw 로 구독한다 — raw_payload 통째가 태그 하나의 값이다.
        # tagMode=fields 로 잡힌 태그는 값이 객체가 아니라 스칼라 하나라 여기 표에 맞지 않는다.
        if field != "raw_payload":
            raise Reject(f"unsupported tag field: {field} (tagMode=fields?)", item)
        tag_id = raw_tid
        tid = device_of(device_id) if channel == CH_ARTIFACT else device_id
    else:
        tag = catalog.resolve(send_topic, param_id)
        if tag is not None:
            tag_id, tid, topic_key, tag_channel = tag
            if tag_channel != channel:
                # 등록부와 실제 발행 토픽이 어긋났다는 뜻이다. 실려 온 토픽을 믿되
                # (그 레코드가 실제로 그 토픽에서 왔다) 어긋남을 세어 둔다.
                TAG_CHANNEL_MISMATCH.inc()
        else:
            TAG_UNRESOLVED.labels(channel=channel).inc()

    value = c.get("value")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as e:
            raise Reject(f"value is not JSON: {e}", item)
    if not isinstance(value, dict):
        raise Reject("value is not an object", item)

    content_ts = None
    ts = c.get("timestamp")
    if ts is not None:
        try:
            content_ts = ns_to_dt(ts)
        except (TypeError, ValueError, OverflowError, OSError):
            raise Reject("content.timestamp is not a ns epoch", item)

    row = BUILDERS[channel](value, item)
    # 카탈로그가 못 푼 항목은 페이로드에서라도 장비를 되찾아 본다(상태 채널만 가능).
    row["tid"] = tid if tid else fallback_tid(channel, value, param_id)
    row["tag_id"] = tag_id
    row["param_id"] = param_id
    row["mqtt_topic"] = topic_key
    row["ingested_at"] = value.get("ingested_at") or None
    row["content_ts"] = content_ts.isoformat() if content_ts else None
    row["pm_mode"] = c.get("pm_mode")
    row["kafka_offset"] = kafka_offset
    return row, content_ts


def parse_record(msg, catalog):
    """
    Kafka 레코드 → (채널별 행 목록, rejects, message_row).
    값 전체가 JSON 이 아니면 레코드 하나가 통째로 reject 한 건이 된다.
    채널은 이 레코드가 실려 온 토픽이 정한다 — 레코드 하나는 한 채널이다.
    """
    offset = msg.offset()
    headers = decode_headers(msg.headers())
    _ts_type, ts_ms = msg.timestamp()
    kafka_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms and ts_ms > 0 else datetime.now(timezone.utc)
    msg_ts = None
    if headers.get("msg_timestamp"):
        try:
            msg_ts = ns_to_dt(headers["msg_timestamp"])
        except (TypeError, ValueError, OverflowError, OSError):
            msg_ts = None

    rows = {channel: [] for channel in CHANNELS}
    rejects = []
    # 채널 판정. 토픽 이름은 소비 대상이 KAFKA_TOPIC 으로 정해져 있으므로 평소에는 늘 맞는다.
    # 발행 설정(send_topic)이 채널과 무관한 이름으로 바뀌면 여기서 레코드째 격리된다 —
    # 조용히 엉뚱한 표에 넣는 것보다 낫다. 헤더 request 도 같은 값이라 대조에 쓴다.
    kafka_topic = msg.topic()
    channel = channel_of_topic(kafka_topic) or channel_of_topic(headers.get("request"))
    raw = msg.value()
    try:
        body = json.loads(raw)
    except (TypeError, ValueError) as e:
        text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        rejects.append((offset, json.dumps({"raw": text[:4000]}), f"record is not JSON: {e}"))
        body = []
    if isinstance(body, dict):
        body = [body]           # 단건으로 오는 경우도 받아 준다
    if not isinstance(body, list):
        rejects.append((offset, json.dumps({"raw": body}), "record is neither array nor object"))
        body = []

    # 격리로 빠져도 "레코드에 항목이 몇 개였나" 는 그대로 남아야 한다.
    item_count = len(body)
    if channel is None:
        for item in body:
            rejects.append((offset, json.dumps(item, ensure_ascii=False, default=str),
                            f"unknown channel for topic: {kafka_topic}"))
        body = []

    send_topic = headers.get("request") or kafka_topic
    for item in body:
        try:
            row, content_ts = parse_item(item, channel, send_topic, offset, catalog)
            rows[channel].append((row, content_ts))
        except Reject as r:
            rejects.append((offset, json.dumps(r.payload, ensure_ascii=False, default=str), r.reason))

    message_row = (
        msg.partition(), offset, kafka_ts, msg.topic(),
        headers.get("uniqueid"), msg_ts,
        headers.get("message_version"), headers.get("method_id"), headers.get("data_type"),
        headers.get("project_id"), headers.get("infra_proc_name"), headers.get("task_area_code"),
        headers.get("request"),
        item_count,
        len(rows[CH_STATUS]), len(rows[CH_ACTUAL]), len(rows[CH_ARTIFACT]),
        len(rejects),
    )
    return rows, rejects, message_row


# ── DB ──────────────────────────────────────────────────────────────────────
def connect_db():
    delay = 1
    while True:
        try:
            conn = psycopg.connect(PG_DSN, autocommit=False, application_name="lidar-ingest")
            log.info("DB 연결 %s", PG_DSN.split("@")[-1])
            return conn
        except psycopg.OperationalError as e:
            log.warning("DB 연결 실패, %ds 후 재시도: %s", delay, e)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def state_rows_of(status_rows):
    """배치 안 장비별 최신 상태 항목만 골라 상태 표에 반영할 행으로 만든다."""
    latest = {}
    for row, content_ts in status_rows:
        key = content_ts.timestamp() if content_ts else 0.0
        prev = latest.get(row["tid"])
        if prev is None or key >= prev[0]:
            latest[row["tid"]] = (key, row)
    return [
        {
            "tid": r["tid"], "device_role": r["device_role"], "site": r["site"], "zone": r["zone"],
            "shop": r["shop"], "bay": r["bay"], "status": r["status"], "error_code": r["error_code"],
            "scan_rate_pts_per_sec": r["scan_rate_pts_per_sec"], "temperature_c": r["temperature_c"],
            "connectivity_rssi": r["connectivity_rssi"], "fov_mode": r["fov_mode"],
            "last_event_at": r["time"], "last_heartbeat_at": r["last_heartbeat_at"],
        }
        for _, r in latest.values()
    ]


def write_batch(conn, rows, rejects, message_rows):
    """한 트랜잭션. 성공하면 (표별 insert 수, 전체 insert 수)."""
    inserted = {}
    with conn.transaction():
        with conn.cursor() as cur:
            for channel in CHANNELS:
                channel_rows = rows[channel]
                if not channel_rows:
                    continue
                table, sql = INSERTS[channel]
                cur.execute(sql, (json.dumps([r for r, _ in channel_rows]),))
                inserted[table] = cur.rowcount
            if rows[CH_STATUS]:
                cur.execute(UPSERT_STATE, (json.dumps(state_rows_of(rows[CH_STATUS])),))
            if rejects:
                cur.executemany(INSERT_REJECT, rejects)
            if message_rows:
                cur.executemany(INSERT_MESSAGE, message_rows)
    return inserted, sum(inserted.values())


# ── Kafka ───────────────────────────────────────────────────────────────────
def make_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "client.id": "tsdb-lidar-ingest",
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
        # 토픽이 max.message.bytes=32MB 로 만들어져 있다. 기본 1MB 면 큰 레코드에서 멈춘다.
        "message.max.bytes": 33554432,
        "fetch.message.max.bytes": 33554432,
        "fetch.max.bytes": 67108864,
        "session.timeout.ms": 30000,
        # DB 가 잠깐 죽어 배치를 재시도하는 동안 그룹에서 쫓겨나지 않도록 넉넉히.
        "max.poll.interval.ms": 600000,
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)
    log.info("Kafka 구독 bootstrap=%s topics=%s group=%s", KAFKA_BOOTSTRAP, ",".join(TOPICS), KAFKA_GROUP_ID)
    return consumer


def update_lag(consumer):
    try:
        for tp in consumer.assignment():
            _lo, hi = consumer.get_watermark_offsets(tp, timeout=1.0, cached=True)
            pos = consumer.position([tp])[0].offset
            if hi >= 0 and pos >= 0:
                KAFKA_LAG.labels(partition=str(tp.partition)).set(max(hi - pos, 0))
    except KafkaException as e:
        log.debug("lag 계산 실패: %s", e)


# ── 메인 루프 ───────────────────────────────────────────────────────────────
running = True


def stop(signum, _frame):
    global running
    log.info("signal %s — 정지", signum)
    running = False


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(METRICS_PORT)
    log.info("metrics :%d/metrics", METRICS_PORT)

    conn = connect_db()
    # 숫자 tid → 장비 id 조회표. 소비를 시작하기 전에 먼저 읽는다 — 비어 있는 채로 돌면
    # 그 사이 들어온 항목이 전부 '#숫자' 로 적재되고, 나중에 표가 채워져도 소급되지 않는다.
    catalog = TagCatalog()
    while True:
        try:
            catalog.load(conn)
            break
        except psycopg.Error as e:
            log.warning("태그 카탈로그 적재 실패, 재시도: %s", e)
            if conn.closed or isinstance(e, psycopg.OperationalError):
                conn = connect_db()
            time.sleep(1)
    consumer = make_consumer()

    while running:
        try:
            msgs = consumer.consume(num_messages=BATCH_MAX_MESSAGES, timeout=BATCH_MAX_WAIT_S)
        except KafkaException as e:
            # 브로커를 못 찾는 동안(aspire 네트워크가 바뀐 경우 등) 죽지 않고 계속 시도한다.
            KAFKA_ERRORS.inc()
            log.error("Kafka consume 실패: %s", e)
            time.sleep(1)
            continue
        update_lag(consumer)
        if not msgs:
            continue

        rows = {channel: [] for channel in CHANNELS}
        rejects, message_rows = [], []
        for msg in msgs:
            if msg.error():
                log.error("Kafka 오류: %s", msg.error())
                continue
            MESSAGES.inc()
            r, j, m = parse_record(msg, catalog)
            for channel in CHANNELS:
                rows[channel].extend(r[channel])
                if r[channel]:
                    ITEMS.labels(channel=channel).inc(len(r[channel]))
            rejects.extend(j)
            message_rows.append(m)
            for _, _, reason in j:
                REJECTS.labels(reason=reason.split(":")[0]).inc()
        if not message_rows:
            continue

        total_rows = sum(len(rows[channel]) for channel in CHANNELS)

        # DB 쓰기. 실패하면 같은 배치를 물고 재시도한다 — 오프셋은 커밋하지 않는다.
        delay = 1
        while running:
            t0 = time.monotonic()
            try:
                per_table, inserted = write_batch(conn, rows, rejects, message_rows)
                break
            except psycopg.Error as e:
                DB_ERRORS.inc()
                log.error("DB 쓰기 실패 (%ds 후 재시도): %s", delay, e)
                if conn.closed or isinstance(e, psycopg.OperationalError):
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = connect_db()
                time.sleep(delay)
                delay = min(delay * 2, 30)
        else:
            break
        elapsed = time.monotonic() - t0

        # DB 는 이미 커밋됐다. 여기서 실패해도 죽지 않는다 — 다음 성공한 커밋이 현재
        # 위치를 통째로 올리고, 그 사이 재시작하면 재전달분은 유니크 인덱스가 거른다.
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            KAFKA_ERRORS.inc()
            log.error("Kafka 오프셋 커밋 실패 (다음 배치에서 재시도): %s", e)

        now = time.time()
        BATCHES.inc()
        BATCH_SECONDS.observe(elapsed)
        BATCH_ROWS.observe(total_rows)
        ROWS_INSERTED.inc(inserted)
        ROWS_DUPLICATE.inc(total_rows - inserted)
        for table, count in per_table.items():
            TABLE_ROWS.labels(table=table).inc(count)
        LAST_COMMIT.set(now)
        newest = None
        for channel in CHANNELS:
            for _, content_ts in rows[channel]:
                if content_ts:
                    END_TO_END.observe(max(now - content_ts.timestamp(), 0.0))
                    if newest is None or content_ts > newest:
                        newest = content_ts
        if newest:
            LAST_EVENT.set(newest.timestamp())

        # 못 푼 숫자를 만났으면 표를 다시 읽어 본다. 트랜잭션 밖(커밋 직후)에서만 한다.
        # 태그를 새로 등록한 직후를 위한 것이라 최소 간격이 걸려 있다.
        if catalog.should_reload():
            try:
                catalog.load(conn)
            except psycopg.Error as e:
                log.warning("태그 카탈로그 재적재 실패 (다음 기회에): %s", e)

        update_lag(consumer)
        log.info(
            "batch records=%d rows=%d (status=%d actual=%d artifact=%d) inserted=%d dup=%d "
            "reject=%d db=%.3fs offset=%d",
            len(message_rows), total_rows,
            len(rows[CH_STATUS]), len(rows[CH_ACTUAL]), len(rows[CH_ARTIFACT]),
            inserted, total_rows - inserted, len(rejects), elapsed, message_rows[-1][1],
        )

    consumer.close()
    conn.close()
    log.info("종료")


if __name__ == "__main__":
    main()
