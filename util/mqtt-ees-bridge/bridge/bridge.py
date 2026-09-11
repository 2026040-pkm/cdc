#!/usr/bin/env python3
"""
MQTT(OT LiDAR 필드 데이터) → Kafka(EES Provider 형식) 브리지.

InterSysLink 의 Agent + EES Kafka Provider 가 하던 일을 그대로 한다. 350대가 EMQX 로
올리는 세 채널을 구독해, **ISL Provider 가 내보내는 것과 같은 레코드**로 바꿔 이 스택의
Kafka 에 넣는다. 수신 측(`src/timescaledb` 의 lidar-ingest)은 원본과 이것을 구분하지 못한다.

  ot/device/{zone}/lidar/status            (1건/1초·대)  → ot.lidar.status
  ot/sensor/{stage}/actual                 (1건/1분·대)  → ot.lidar.actual
  ot/pipeline/{zone}/{shop}/{bay}/artifact (12건/1분·대) → ot.lidar.artifact

─ 레코드 모양 (EES Kafka Provider · data_type=value · V1.0) ─────────────────
Value 는 항목 배열이다. 항목 하나가 태그 하나의 값 하나다.

    [{"content": {"timestamp": <ns epoch>,
                  "tid": "135",                    # 숫자 ParameterId. TagId 가 아니다
                  "value": "{\\"device_role\\":\\"LIDAR\\",...}",   # raw_payload 통째의 JSON 문자열
                  "pm_mode": false}}, ...]

Key 는 project_id 문자열이고, 헤더는 11개로 고정이다 (`EesHeaderBuilderV1_0`).

    project_id  data_type  method_id  msg_timestamp  infra_proc_name  task_area_code
    message_version  uniqueid  request  reply(빈 값)  replycluster(빈 값)

세 채널의 실제 설정값은 ISL 등록부에서 그대로 가져왔다 (`EES_MESSAGE.ConfigJson`).
project_id · infra_proc_name · task_area_code · request 가 모두 send_topic 과 같은 값이다.
method_id 는 sendPolicy.mode=OnChange 라 TRACE 다.

**Key 가 채널마다 상수라 파티션이 하나로 몰린다.** 이건 이 계약의 성질이지 설정 실수가
아니다 — 토픽 파티션을 늘려도 레코드는 전부 한 파티션에 간다. 그래서 토픽을 1파티션으로
만든다 (scripts/up).

─ 숫자 tid ────────────────────────────────────────────────────────────────
Provider 는 태그를 실을 때 문자열 TagId 를 버리고 등록부의 ParameterId 만 싣는다.
그 숫자를 여기서 지어내면 수신 측 카탈로그가 어긋나므로, ISL 등록부에서 뽑은
`tags/tag-catalog.json` 을 그대로 쓴다 (scripts/export-tag-map.py).

등록부에 없는 태그를 만나면 두 갈래다.
  AUTO_REGISTER_TAGS=true  (기본) AUTO_PARAM_ID_BASE 부터 번호를 새로 매겨 내보내고,
                           /state/auto-tags.json 에 적어 재기동해도 같은 번호를 쓴다.
                           /state/auto-tags.sql 에 lidar_tag_catalog 에 넣을 INSERT 도 쓴다.
  AUTO_REGISTER_TAGS=false 버리고 센다 (지금 ISL 이 하는 그대로).

지금 등록부에는 `ot/sensor/fitting/actual` 이 없다 — 조립 stage 4종 중 FITTING 만
등록이 빠져 있어 210대분이 ISL 에서는 조용히 버려진다. 기본값(auto)이면 여기서는 살아서
나가고 그 사실이 지표(ees_bridge_tag_unregistered_total)와 로그에 남는다.

─ 배치 ────────────────────────────────────────────────────────────────────
항목 N개 또는 M ms 중 먼저 오는 쪽에서 레코드 하나로 묶어 보낸다. 채널마다 따로 묶는다 —
레코드 하나는 EES 메시지 하나이고 EES 메시지 하나가 토픽 하나이기 때문이다.

창은 **상태 주기(1초)**에 맞춰 두었다. 350대가 시작을 밀어 가며 발행해 초당 426건이 고르게
들어오므로, 1초 창이면 토픽마다 초당 레코드 하나가 나간다.

    ot.lidar.status    350항목/레코드      ot.lidar.artifact   70항목      ot.lidar.actual  6항목

발행기를 --burst 로 돌리면 350대가 같은 순간에 쏜다. 그러면 1분 주기 채널은 60초에 한 번만
터지고 그때는 시간이 아니라 BATCH_MAX_ITEMS(500) 가 레코드를 끊는다 — 실측(70초·--burst)으로
artifact 는 60초에 레코드 9개(500×8+200), actual 은 1개(350항목)였다.

이 알갱이는 ISL Provider 의 관측값(레코드당 ~210항목 · 1~3초 간격)과 같은 자릿수다 — 수신 측의
배치·지연 눈금을 그대로 쓸 수 있게 맞춘 것이다. 창을 줄이면(예: 200ms) 항목 수는 그대로인데
레코드가 5배로 잘아져 수신 측 Kafka 지연 임계(레코드 수 기준)의 의미가 달라진다.
BATCH_MAX_ITEMS 500 은 장비가 늘어 한 창에 350건을 넘을 때만 걸리는 안전판이다.

전달 보장: at-least-once. Kafka 쪽은 acks=all 에 enable.idempotence 로 중복 없이 재시도하고,
MQTT 쪽은 QoS 1 이라 브로커가 재전달할 수 있다. 같은 항목이 두 번 실려도 수신 측의
(idempotency_key, time) 유니크 인덱스가 걸러 낸다.
"""
import json
import logging
import os
import re
import signal
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ── 설정 ────────────────────────────────────────────────────────────────────
MQTT_HOST = os.environ.get("MQTT_HOST", "host.docker.internal")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1884"))
MQTT_CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", "ees-bridge")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME") or None
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD") or None
MQTT_QOS = int(os.environ.get("MQTT_QOS", "1"))
# 세션을 새로 시작한다. false 로 두면 브리지가 죽어 있는 동안 브로커가 QoS1 을 쌓아 두지만
# 초당 426건이라 큐 한도(EMQX 기본 1000)를 금방 넘긴다 — 쌓지 않고 흘려보내는 쪽이 정직하다.
MQTT_CLEAN_SESSION = os.environ.get("MQTT_CLEAN_SESSION", "true").lower() != "false"
# 구독 패턴. 발행기의 22개 토픽을 세 패턴으로 덮는다.
MQTT_TOPICS = os.environ.get(
    "MQTT_TOPICS",
    "ot/device/+/lidar/status,ot/sensor/+/actual,ot/pipeline/+/+/+/artifact",
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9093")
KAFKA_CLIENT_ID = os.environ.get("KAFKA_CLIENT_ID", "isl-engine")
KAFKA_COMPRESSION = os.environ.get("KAFKA_COMPRESSION", "lz4")
KAFKA_LINGER_MS = int(os.environ.get("KAFKA_LINGER_MS", "20"))

# 채널 → send_topic. ISL 의 EES_MESSAGE.ConfigJson.key 와 같은 값이다.
SEND_TOPICS = {
    "status": os.environ.get("EES_TOPIC_STATUS", "ot.lidar.status"),
    "actual": os.environ.get("EES_TOPIC_ACTUAL", "ot.lidar.actual"),
    "artifact": os.environ.get("EES_TOPIC_ARTIFACT", "ot.lidar.artifact"),
}
MESSAGE_VERSION = os.environ.get("EES_MESSAGE_VERSION", "1.0")
DATA_TYPE = os.environ.get("EES_DATA_TYPE", "value")
# sendPolicy.mode=OnChange → ChangedBatch → TRACE. 주기 스냅샷이면 BATCH 다.
METHOD_ID = os.environ.get("EES_METHOD_ID", "TRACE")

BATCH_MAX_ITEMS = int(os.environ.get("BATCH_MAX_ITEMS", "500"))
BATCH_MAX_WAIT_S = int(os.environ.get("BATCH_MAX_WAIT_MS", "1000")) / 1000.0

TAG_CATALOG_PATH = Path(os.environ.get("TAG_CATALOG", "/app/tags/tag-catalog.json"))
AUTO_REGISTER_TAGS = os.environ.get("AUTO_REGISTER_TAGS", "true").lower() != "false"
AUTO_PARAM_ID_BASE = int(os.environ.get("AUTO_PARAM_ID_BASE", "900000"))
STATE_DIR = Path(os.environ.get("STATE_DIR", "/state"))

METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
REPORT_INTERVAL_S = int(os.environ.get("REPORT_INTERVAL_SECONDS", "30"))
# 토픽이 없을 때 다시 만들어 보는 최소 간격. 초당 세 번 AdminClient 를 두드리지 않으려는 것.
TOPIC_ENSURE_MIN_INTERVAL_S = int(os.environ.get("TOPIC_ENSURE_MIN_INTERVAL_SECONDS", "30"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("ees-bridge")

CHANNELS = ("status", "actual", "artifact")
# 토픽이 사라졌을 때 오는 오류들. 기동할 때부터 없으면 _UNKNOWN_TOPIC 이 produce() 에서
# 곧바로 튀고, 돌아가는 중에 지우면 produce() 는 조용히 받아 두고 (librdkafka 가 메타데이터를
# 들고 있어 파티션 수만 1→0 이 된다) _UNKNOWN_PARTITION 이 전달 콜백으로 온다. 둘 다 잡는다.
TOPIC_GONE = frozenset((
    KafkaError._UNKNOWN_TOPIC, KafkaError._UNKNOWN_PARTITION, KafkaError.UNKNOWN_TOPIC_OR_PART))
ARTIFACT_TYPES = ("REGISTERED_PCD", "TRANSFORMATION_MATRIX", "SEGMENTED_PCD")
RAW_FIELD = "raw_payload"

# ── 지표 ────────────────────────────────────────────────────────────────────
MQTT_MESSAGES = Counter("ees_bridge_mqtt_messages_total", "MQTT messages consumed", ["channel"])
MQTT_DROPS = Counter("ees_bridge_mqtt_drops_total", "MQTT messages not forwarded", ["reason"])
MQTT_CONNECTED = Gauge("ees_bridge_mqtt_connected", "1 while the MQTT session is up")
RECORDS = Counter("ees_bridge_records_total", "Kafka records produced", ["topic"])
ITEMS = Counter("ees_bridge_items_total", "EES items produced", ["topic"])
DELIVERED = Counter("ees_bridge_delivered_total", "Kafka records acked by the broker", ["topic"])
DELIVERY_ERRORS = Counter("ees_bridge_delivery_errors_total", "Kafka delivery failures", ["topic"])
RECORD_ITEMS = Histogram(
    "ees_bridge_record_items", "items per Kafka record",
    buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000),
)
RECORD_BYTES = Histogram(
    "ees_bridge_record_bytes", "Kafka record value size",
    buckets=(512, 1024, 4096, 16384, 65536, 262144, 1048576),
)
HANDOFF = Histogram(
    "ees_bridge_handoff_seconds", "device occurred_at → Kafka produce",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
TAG_CATALOG_SIZE = Gauge("ees_bridge_tag_catalog_rows", "tags loaded from the catalog file")
TAG_UNREGISTERED = Counter(
    "ees_bridge_tag_unregistered_total",
    "messages whose TagId was not in the catalog", ["channel"])
TAG_AUTO = Gauge("ees_bridge_tag_auto_rows", "tags this bridge numbered itself")
QUEUE_DEPTH = Gauge("ees_bridge_pending_items", "items waiting in the batcher", ["channel"])
LAST_PRODUCE = Gauge("ees_bridge_last_produce_timestamp_seconds", "unix time of the last produce")

# ── 시각 ────────────────────────────────────────────────────────────────────
# 발행기는 "O" 서식(7자리 소수초 + 오프셋)으로 쓴다: 2026-09-10T12:59:23.3532050+09:00
# datetime 은 µs 까지라 소수초를 따로 떼어 ns 를 정확히 만든다. ISL 도 100ns 틱을
# 그대로 ns 로 올린다 (EesHeaderBuilderV1_0.ToUnixTimestampOrSeconds).
ISO_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})?$"
)
LOCAL_TZ = datetime.now().astimezone().tzinfo


def iso_to_ns(text):
    """ISO 8601 문자열 → ns epoch. 모양이 아니면 ValueError."""
    m = ISO_RE.match(text.strip())
    if not m:
        raise ValueError(f"not an ISO 8601 timestamp: {text!r}")
    date, clock, frac, tz = m.groups()
    if tz == "Z":
        tz = "+00:00"
    elif tz and ":" not in tz:
        tz = tz[:3] + ":" + tz[3:]
    base = datetime.fromisoformat(f"{date}T{clock}" + (tz or ""))
    if base.tzinfo is None:
        base = base.replace(tzinfo=LOCAL_TZ)
    return int(base.timestamp()) * 1_000_000_000 + int((frac or "").ljust(9, "0")[:9])


def channel_of(mqtt_topic):
    """MQTT 토픽의 마지막 마디가 채널이다. 채널마다 토픽 깊이가 달라 끝만 본다."""
    last = mqtt_topic.rstrip("/").rsplit("/", 1)[-1].lower()
    return last if last in CHANNELS else None


def split_artifact(device_id):
    """LDR-…-01-SEGMENTED_PCD → (LDR-…-01, SEGMENTED_PCD). 접미사가 없으면 (그대로, None)."""
    for suffix in ARTIFACT_TYPES:
        tail = "-" + suffix
        if device_id.endswith(tail):
            return device_id[: -len(tail)], suffix
    return device_id, None


# ── 태그 조회표 ─────────────────────────────────────────────────────────────
class TagCatalog:
    """
    TagId → (send_topic, ParameterId).

    ISL 등록부에서 뽑은 표를 그대로 들고 있다. 없는 태그는 설정에 따라 새 번호를 매기거나
    (그리고 그 번호를 파일에 남겨 재기동해도 유지한다) 버린다.
    """

    def __init__(self, path, state_dir, auto, auto_base):
        self._tags = {}
        self._auto = {}
        self._auto_lock = threading.Lock()
        self._auto_enabled = auto
        self._auto_base = auto_base
        self._state_json = state_dir / "auto-tags.json"
        self._state_sql = state_dir / "auto-tags.sql"
        self._load(path)
        self._load_auto()

    def _load(self, path):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            log.warning("태그 조회표가 없다 (%s) — scripts/export-tag-map.py 로 만들어라. "
                        "지금은 모든 태그가 새 번호를 받는다.", path)
            doc = {}
        except ValueError as e:
            log.error("태그 조회표를 못 읽었다 (%s): %s", path, e)
            doc = {}
        self._tags = {k: (v[0], int(v[1])) for k, v in (doc.get("tags") or {}).items()}
        TAG_CATALOG_SIZE.set(len(self._tags))
        if self._tags:
            log.info("태그 조회표 %d행 (%s, %s)", len(self._tags), path, doc.get("generated_at"))

    def _load_auto(self):
        if not self._auto_enabled:
            return
        try:
            doc = json.loads(self._state_json.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            return
        self._auto = {k: (v[0], int(v[1])) for k, v in (doc.get("tags") or {}).items()}
        TAG_AUTO.set(len(self._auto))
        if self._auto:
            log.info("직접 번호를 매긴 태그 %d행을 %s 에서 이어받았다", len(self._auto), self._state_json)

    def resolve(self, tag_id, send_topic, mqtt_topic, device_id):
        """TagId → (send_topic, param_id, 등록돼 있었나). 버릴 항목이면 None."""
        hit = self._tags.get(tag_id)
        if hit is not None:
            return hit[0], hit[1], True
        hit = self._auto.get(tag_id)
        if hit is not None:
            return hit[0], hit[1], False
        if not self._auto_enabled:
            return None
        return self._assign(tag_id, send_topic, mqtt_topic, device_id)

    def _assign(self, tag_id, send_topic, mqtt_topic, device_id):
        with self._auto_lock:
            hit = self._auto.get(tag_id)          # 잠금 밖에서 다른 스레드가 넣었을 수 있다
            if hit is not None:
                return hit[0], hit[1], False
            param_id = self._auto_base + len(self._auto) + 1
            self._auto[tag_id] = (send_topic, param_id)
            TAG_AUTO.set(len(self._auto))
            log.warning("등록부에 없는 태그 — %s → %s #%d (자동 번호)", tag_id, send_topic, param_id)
            self._persist(tag_id, send_topic, param_id, mqtt_topic, device_id)
            return send_topic, param_id, False

    def _persist(self, tag_id, send_topic, param_id, mqtt_topic, device_id):
        """
        자동 번호를 파일에 남긴다. 둘을 쓴다.
          auto-tags.json  재기동 뒤에도 같은 태그가 같은 번호를 받게 하는 상태
          auto-tags.sql   수신 측 rdb.lidar_tag_catalog 에 그대로 넣을 수 있는 INSERT
        """
        try:
            self._state_json.parent.mkdir(parents=True, exist_ok=True)
            payload = {"tags": {k: [v[0], v[1]] for k, v in sorted(self._auto.items())}}
            self._state_json.write_text(
                json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

            tid, artifact_type = split_artifact(device_id)
            channel = channel_of(mqtt_topic)
            new = self._state_sql.stat().st_size == 0 if self._state_sql.exists() else True
            with self._state_sql.open("a", encoding="utf-8") as fh:
                if new:
                    fh.write(
                        "-- 브리지가 직접 번호를 매긴 태그. 등록부에 없어서 생긴 줄이다.\n"
                        "-- 수신 측에서 장비 축을 살리려면 그대로 실행한다.\n"
                        "-- INSERT INTO rdb.lidar_tag_catalog\n"
                        "--   (send_topic, param_id, tag_id, device_id, tid, channel,\n"
                        "--    artifact_type, mqtt_topic, mqtt_topic_key, field, data_type)\n")
                values = ", ".join(quote(v) for v in (
                    send_topic, param_id, tag_id, device_id, tid, channel,
                    artifact_type, mqtt_topic, mqtt_topic.replace("/", "_"), RAW_FIELD, "STRING"))
                fh.write(f"INSERT INTO rdb.lidar_tag_catalog VALUES ({values})"
                         f" ON CONFLICT DO NOTHING;\n")
        except OSError as e:
            # 상태를 못 써도 적재는 멈추지 않는다. 재기동 뒤 번호가 달라질 뿐이다.
            log.error("자동 태그 상태 쓰기 실패 (%s): %s", self._state_json, e)


def quote(value):
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


# ── 토픽 ────────────────────────────────────────────────────────────────────
def ensure_topics(topics):
    """
    없는 토픽을 만든다. 스택의 kafka-init 이 하는 것과 같은 모양(1파티션 · 24시간 보존)이다.

    브로커는 auto.create.topics.enable=false 다 — 오타 난 토픽이 조용히 생기는 것을 막으려는
    설정이라 그대로 둔다. 다만 그 탓에 토픽이 없으면 produce 가 _UNKNOWN_TOPIC 으로 실패하고,
    그 상태는 **재기동으로 풀리지 않는다**. 토픽을 지우면 브리지가 무한 재기동에 빠지고
    clean session 이라 그동안 MQTT 는 통째로 버려진다 (2026-09-11 에 실제로 겪었다).

    여기서 만드는 이름은 SEND_TOPICS 셋뿐이라 오타가 새 토픽을 만드는 길은 여전히 없다.
    """
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    try:
        existing = set(admin.list_topics(timeout=10).topics)
    except KafkaException as e:
        log.warning("토픽 목록을 못 읽었다 (%s): %s", KAFKA_BOOTSTRAP, e)
        return
    missing = [t for t in topics if t not in existing]
    if not missing:
        return
    log.warning("없는 토픽을 만든다 — %s", ", ".join(missing))
    new = [NewTopic(t, num_partitions=1, replication_factor=1,
                    config={"retention.ms": "86400000", "compression.type": "producer"})
           for t in missing]
    for topic, fut in admin.create_topics(new).items():
        try:
            fut.result(timeout=15)
            log.info("토픽 생성 %s", topic)
        except Exception as e:      # 그 사이 다른 쪽(kafka-init)이 만들었을 수도 있다
            log.warning("토픽 생성 실패 %s: %s", topic, e)


# ── 배치 ────────────────────────────────────────────────────────────────────
class Batcher:
    """
    채널별 항목 버퍼. 항목 N개가 차거나 M ms 가 지나면 레코드 하나로 내보낸다.

    MQTT 수신 스레드가 add() 를, 본 스레드가 tick() 을 부른다. 잠금 하나로 묶고
    실제 produce 는 잠금 밖에서 한다 — Producer.produce 는 큐가 차면 막힐 수 있어
    그 사이 수신 스레드를 세우지 않으려는 것이다.
    """

    def __init__(self, emit, max_items, max_wait_s):
        self._emit = emit
        self._max_items = max_items
        self._max_wait = max_wait_s
        self._lock = threading.Lock()
        self._items = {ch: [] for ch in CHANNELS}
        self._since = {ch: 0.0 for ch in CHANNELS}

    def add(self, channel, item):
        with self._lock:
            bucket = self._items[channel]
            if not bucket:
                self._since[channel] = time.monotonic()
            bucket.append(item)
            QUEUE_DEPTH.labels(channel=channel).set(len(bucket))
            if len(bucket) < self._max_items:
                return
            ready = self._take(channel)
        self._emit(channel, ready)

    def tick(self):
        """대기 시간이 찬 채널을 내보낸다. 본 루프가 주기적으로 부른다."""
        now = time.monotonic()
        due = []
        with self._lock:
            for channel in CHANNELS:
                if self._items[channel] and (now - self._since[channel]) >= self._max_wait:
                    due.append((channel, self._take(channel)))
        for channel, ready in due:
            self._emit(channel, ready)

    def drain(self):
        with self._lock:
            due = [(ch, self._take(ch)) for ch in CHANNELS if self._items[ch]]
        for channel, ready in due:
            self._emit(channel, ready)

    def _take(self, channel):
        ready, self._items[channel] = self._items[channel], []
        QUEUE_DEPTH.labels(channel=channel).set(0)
        return ready


# ── 브리지 ──────────────────────────────────────────────────────────────────
class Bridge:
    def __init__(self):
        self.catalog = TagCatalog(TAG_CATALOG_PATH, STATE_DIR, AUTO_REGISTER_TAGS, AUTO_PARAM_ID_BASE)
        self.batcher = Batcher(self._publish, BATCH_MAX_ITEMS, BATCH_MAX_WAIT_S)
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": KAFKA_CLIENT_ID,
            # 재시도가 순서를 흐트러뜨리지도, 같은 레코드를 두 번 남기지도 않게 한다.
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": KAFKA_COMPRESSION,
            "linger.ms": KAFKA_LINGER_MS,
            # 브로커가 없는 동안 쌓아 두는 한도. 넘치면 produce 가 막혀 역압이 걸린다.
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.kbytes": 262144,
        })
        self._records = 0
        self._items = 0
        self._last_ensure = -1e9        # 첫 실패는 곧바로 다시 만들어 본다
        ensure_topics(sorted(set(SEND_TOPICS.values())))

    # ── MQTT 수신 ──
    def on_message(self, _client, _userdata, msg):
        topic = msg.topic
        channel = channel_of(topic)
        if channel is None:
            MQTT_DROPS.labels(reason="unknown_topic").inc()
            return
        MQTT_MESSAGES.labels(channel=channel).inc()
        try:
            payload = json.loads(msg.payload)
        except ValueError:
            MQTT_DROPS.labels(reason="not_json").inc()
            return
        if not isinstance(payload, dict):
            MQTT_DROPS.labels(reason="not_object").inc()
            return

        device_id = payload.get("id")
        raw = payload.get(RAW_FIELD)
        if not isinstance(device_id, str) or not device_id or not isinstance(raw, dict):
            MQTT_DROPS.labels(reason="missing_id_or_raw_payload").inc()
            return

        # TagId 는 발행기·UI 와 같은 규칙으로 만든다: {id}.{토픽의 / 를 _ 로}.raw_payload
        tag_id = f"{device_id}.{topic.replace('/', '_')}.{RAW_FIELD}"
        resolved = self.catalog.resolve(tag_id, SEND_TOPICS[channel], topic, device_id)
        if resolved is None:
            TAG_UNREGISTERED.labels(channel=channel).inc()
            MQTT_DROPS.labels(reason="tag_not_registered").inc()
            return
        send_topic, param_id, registered = resolved
        if not registered:
            TAG_UNREGISTERED.labels(channel=channel).inc()

        # 태그의 changedAt 이 occurred_at 이다 — 그것이 항목의 timestamp 가 된다.
        occurred = raw.get("occurred_at")
        try:
            ts_ns = iso_to_ns(occurred) if isinstance(occurred, str) else time.time_ns()
        except ValueError:
            MQTT_DROPS.labels(reason="bad_occurred_at").inc()
            return

        # tagMode=raw — raw_payload 통째가 태그 하나의 STRING 값이다.
        # 다시 직렬화하되 키 순서는 받은 그대로 두고 공백만 없앤다.
        value = json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
        self.batcher.add(channel, {
            "content": {"timestamp": ts_ns, "tid": str(param_id), "value": value, "pm_mode": False}
        })

    # ── Kafka 송신 ──
    def _publish(self, channel, items):
        if not items:
            return
        send_topic = SEND_TOPICS[channel]
        body = json.dumps(items, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = self._headers(send_topic)
        try:
            self.producer.produce(
                topic=send_topic,
                key=send_topic,          # Provider 는 정규화한 project_id 를 Key 로 쓴다
                value=body,
                headers=headers,
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # 큐가 찼다. 비워지길 기다렸다가 한 번 더 — 그래도 안 되면 예외가 올라가
            # 프로세스가 죽고 컨테이너가 재기동한다 (조용히 버리지 않는다). 이건 브로커가
            # 돌아오면 풀리는 종류라 재기동이 답이 된다.
            log.warning("Kafka 송신 큐가 찼다 — 비우고 재시도한다 (%s, %d건)", send_topic, len(items))
            self.producer.flush(10)
            self.producer.produce(
                topic=send_topic, key=send_topic, value=body,
                headers=headers, on_delivery=self._on_delivery)
        except KafkaException as e:
            # 토픽이 없는 등 브로커 쪽 사정. 여기서 죽으면 재기동해도 같은 이유로 또 죽으므로
            # (auto.create.topics.enable=false — 토픽은 저절로 생기지 않는다) 세면서 버티고,
            # 토픽을 다시 만들어 본다. 그 사이 레코드 하나를 잃는 것은 로그와 지표에 남는다.
            DELIVERY_ERRORS.labels(topic=send_topic).inc()
            log.error("Kafka 송신 거부 (%s, %d건): %s", send_topic, len(items), e)
            self._recover_topics()
            return

        self._records += 1
        self._items += len(items)
        RECORDS.labels(topic=send_topic).inc()
        ITEMS.labels(topic=send_topic).inc(len(items))
        RECORD_ITEMS.observe(len(items))
        RECORD_BYTES.observe(len(body))
        LAST_PRODUCE.set(time.time())
        # 오래된 항목 기준으로 재는 것이 손해가 큰 쪽을 보여 준다.
        oldest = items[0]["content"]["timestamp"] / 1e9
        HANDOFF.observe(max(0.0, time.time() - oldest))
        self.producer.poll(0)

    def _recover_topics(self):
        now = time.monotonic()
        if now - self._last_ensure < TOPIC_ENSURE_MIN_INTERVAL_S:
            return
        self._last_ensure = now
        ensure_topics(sorted(set(SEND_TOPICS.values())))

    def _headers(self, send_topic):
        """V1.0 의 11개 헤더. 순서·값 모두 EesHeaderBuilderV1_0 과 같다."""
        return [
            ("project_id", send_topic.encode()),
            ("data_type", DATA_TYPE.encode()),
            ("method_id", METHOD_ID.encode()),
            ("msg_timestamp", str(time.time_ns()).encode()),
            ("infra_proc_name", send_topic.encode()),
            ("task_area_code", send_topic.encode()),
            ("message_version", MESSAGE_VERSION.encode()),
            ("uniqueid", str(uuid.uuid4()).encode()),
            ("request", send_topic.encode()),
            ("reply", b""),
            ("replycluster", b""),
        ]

    def _on_delivery(self, err, msg):
        topic = msg.topic() if msg is not None else "?"
        if err is None:
            DELIVERED.labels(topic=topic).inc()
            return
        DELIVERY_ERRORS.labels(topic=topic).inc()
        log.error("Kafka 전달 실패 (%s): %s", topic, err)
        if err.code() in TOPIC_GONE:
            self._recover_topics()

    def counters(self):
        return self._records, self._items


# ── 본체 ────────────────────────────────────────────────────────────────────
running = True


def stop(signum, _frame):
    global running
    running = False
    log.info("신호 %s — 정리하고 종료한다", signum)


def make_mqtt(bridge):
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv311,          # Agent 와 같은 조건. v5 면 EMQX 가 CONNACK 0x01 로 끊는다
        clean_session=MQTT_CLEAN_SESSION,
    )
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    topics = [(t.strip(), MQTT_QOS) for t in MQTT_TOPICS.split(",") if t.strip()]

    def on_connect(cl, _userdata, _flags, reason, _props=None):
        # paho 2.x 는 ReasonCode 를 준다. 정수로 비교하지 않고 성질을 묻는다.
        if getattr(reason, "is_failure", reason != 0):
            MQTT_CONNECTED.set(0)
            log.error("MQTT 접속 거부: %s", reason)
            return
        MQTT_CONNECTED.set(1)
        cl.subscribe(topics)
        log.info("MQTT 접속 %s:%s — 구독 %s", MQTT_HOST, MQTT_PORT, ", ".join(t for t, _ in topics))

    def on_disconnect(_cl, _userdata, _flags, reason, _props=None):
        MQTT_CONNECTED.set(0)
        log.warning("MQTT 끊김 (%s) — 재접속한다", reason)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = bridge.on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    start_http_server(METRICS_PORT)
    log.info("지표 :%d/metrics · Kafka %s · 배치 %d건/%dms · 자동 태그 번호 %s",
             METRICS_PORT, KAFKA_BOOTSTRAP, BATCH_MAX_ITEMS, int(BATCH_MAX_WAIT_S * 1000),
             "on" if AUTO_REGISTER_TAGS else "off")

    bridge = Bridge()
    client = make_mqtt(bridge)
    while running:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            break
        except OSError as e:
            log.warning("MQTT 접속 실패 (%s:%s): %s — 3초 뒤 재시도", MQTT_HOST, MQTT_PORT, e)
            time.sleep(3)
    client.loop_start()

    last_report = time.monotonic()
    last_counts = (0, 0)
    while running:
        bridge.batcher.tick()
        bridge.producer.poll(0)
        time.sleep(min(BATCH_MAX_WAIT_S, 0.05))

        now = time.monotonic()
        if now - last_report >= REPORT_INTERVAL_S:
            records, items = bridge.counters()
            elapsed = now - last_report
            log.info("레코드 %d (+%.1f/s) · 항목 %d (+%.1f/s) · 대기 %d",
                     records, (records - last_counts[0]) / elapsed,
                     items, (items - last_counts[1]) / elapsed,
                     len(bridge.producer))
            last_report, last_counts = now, (records, items)

    client.loop_stop()
    client.disconnect()
    bridge.batcher.drain()
    remaining = bridge.producer.flush(30)
    records, items = bridge.counters()
    log.info("종료 — 레코드 %d · 항목 %d · 미전송 %d", records, items, remaining)


if __name__ == "__main__":
    main()
