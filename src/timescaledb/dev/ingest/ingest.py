#!/usr/bin/env python3
"""
ot-lidar-status (Kafka) → TimescaleDB 소비자.

레코드 하나의 값은 장비 약 200대의 상태 배열이다.

    [{"content": {"timestamp": <ns epoch>, "tid": "58",
                  "value": "{\"status\":\"ONLINE\",\"error_code\":\"\",...}",   # JSON 문자열
                  "pm_mode": false}}, ...]

하는 일은 넷이다.
  1. 배열을 풀어 항목마다 lidar_status 한 행 (멱등 키 충돌은 DO NOTHING)
  2. 배치 안의 장비별 최신 항목으로 lidar_device_state UPSERT (오래된 이벤트는 못 덮는다)
  3. 레코드 자체를 lidar_status_message 한 행 (헤더·오프셋·항목 수)
  4. 파싱이 안 되는 항목은 lidar_ingest_reject 로 격리 — 나머지 항목은 그대로 적재한다

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
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ot-lidar-status")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "tsdb-lidar-ingest")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")
PG_DSN = os.environ.get("PG_DSN", "postgresql://postgres:postgres@timescaledb:5432/lidar")
BATCH_MAX_MESSAGES = int(os.environ.get("BATCH_MAX_MESSAGES", "50"))
BATCH_MAX_WAIT_S = int(os.environ.get("BATCH_MAX_WAIT_MS", "1000")) / 1000.0
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("lidar-ingest")

# ── 지표 ────────────────────────────────────────────────────────────────────
MESSAGES = Counter("lidar_ingest_messages_total", "Kafka records consumed")
ITEMS = Counter("lidar_ingest_items_total", "items parsed from records", ["status"])
ROWS_INSERTED = Counter("lidar_ingest_rows_inserted_total", "rows inserted into lidar_status")
ROWS_DUPLICATE = Counter("lidar_ingest_rows_duplicate_total", "rows skipped by the idempotency index")
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
KAFKA_LAG = Gauge("lidar_ingest_kafka_lag", "high watermark - consumer position", ["partition"])
LAST_COMMIT = Gauge("lidar_ingest_last_commit_timestamp_seconds", "unix time of the last DB+Kafka commit")
LAST_EVENT = Gauge("lidar_ingest_last_event_timestamp_seconds", "newest content.timestamp seen (unix seconds)")

# ── SQL ─────────────────────────────────────────────────────────────────────
STATUS_COLS = (
    "time, tid, status, error_code, scan_rate_pts_per_sec, point_cloud_quality_score, "
    "temperature_c, connectivity_rssi, last_heartbeat_at, ingested_at, content_ts, pm_mode, "
    "idempotency_key, kafka_offset"
)
# json_populate_recordset 이 JSON 키를 컬럼에 맞춰 캐스팅한다. timestamptz 문자열은
# PostgreSQL 이 직접 파싱하므로 (7자리 소수초도 받는다) 파이썬에서 날짜를 다루지 않는다.
INSERT_STATUS = f"""
    INSERT INTO lidar_status ({STATUS_COLS})
    SELECT {STATUS_COLS} FROM json_populate_recordset(NULL::lidar_status, %s::json)
    ON CONFLICT (idempotency_key, time) DO NOTHING
"""
STATE_COLS = (
    "tid, status, error_code, scan_rate_pts_per_sec, point_cloud_quality_score, "
    "temperature_c, connectivity_rssi, last_event_at, last_heartbeat_at"
)
UPSERT_STATE = f"""
    INSERT INTO lidar_device_state ({STATE_COLS}, updated_at)
    SELECT {STATE_COLS}, now() FROM json_populate_recordset(NULL::lidar_device_state, %s::json)
    ON CONFLICT (tid) DO UPDATE SET
        status = EXCLUDED.status,
        error_code = EXCLUDED.error_code,
        scan_rate_pts_per_sec = EXCLUDED.scan_rate_pts_per_sec,
        point_cloud_quality_score = EXCLUDED.point_cloud_quality_score,
        temperature_c = EXCLUDED.temperature_c,
        connectivity_rssi = EXCLUDED.connectivity_rssi,
        last_event_at = EXCLUDED.last_event_at,
        last_heartbeat_at = EXCLUDED.last_heartbeat_at,
        updated_at = now()
    WHERE EXCLUDED.last_event_at > lidar_device_state.last_event_at
"""
INSERT_MESSAGE = """
    INSERT INTO lidar_status_message
        (kafka_partition, kafka_offset, kafka_ts, uniqueid, msg_ts, message_version, method_id,
         item_count, reject_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""
INSERT_REJECT = """
    INSERT INTO lidar_ingest_reject (kafka_offset, payload, reason) VALUES (%s, %s::jsonb, %s)
"""

# ── 변환 ────────────────────────────────────────────────────────────────────
def ns_to_dt(ns):
    """ns epoch → tz-aware datetime (µs 로 절삭. timestamptz 정밀도가 µs 다)."""
    ns = int(ns)
    sec, rem = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(sec, tz=timezone.utc) + timedelta(microseconds=rem // 1000)


def decode_headers(raw):
    """Kafka 헤더 [(key, bytes|None), ...] → dict[str, str]."""
    out = {}
    for key, val in raw or []:
        if val is None:
            out[key] = None
        else:
            out[key] = val.decode("utf-8", "replace") if isinstance(val, (bytes, bytearray)) else str(val)
    return out


class Reject(Exception):
    def __init__(self, reason, payload):
        super().__init__(reason)
        self.reason = reason
        self.payload = payload


def parse_item(item, kafka_offset):
    """배열 원소 하나 → lidar_status 행(dict). 깨졌으면 Reject."""
    if not isinstance(item, dict) or not isinstance(item.get("content"), dict):
        raise Reject("no content object", item)
    c = item["content"]
    tid = c.get("tid")
    if tid is None or str(tid) == "":
        raise Reject("missing tid", item)
    value = c.get("value")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as e:
            raise Reject(f"value is not JSON: {e}", item)
    if not isinstance(value, dict):
        raise Reject("value is not an object", item)
    for field in ("status", "occurred_at", "idempotency_key"):
        if not value.get(field):
            raise Reject(f"missing {field}", item)

    content_ts = None
    ts = c.get("timestamp")
    if ts is not None:
        try:
            content_ts = ns_to_dt(ts)
        except (TypeError, ValueError, OverflowError, OSError):
            raise Reject("content.timestamp is not a ns epoch", item)

    row = {
        "time": value["occurred_at"],
        "tid": str(tid),
        "status": str(value["status"]),
        "error_code": value.get("error_code") or None,           # '' → NULL
        "scan_rate_pts_per_sec": value.get("scan_rate_pts_per_sec"),
        "point_cloud_quality_score": value.get("point_cloud_quality_score"),
        "temperature_c": value.get("temperature_c"),
        "connectivity_rssi": value.get("connectivity_rssi"),
        "last_heartbeat_at": value.get("last_heartbeat_at") or None,
        "ingested_at": value.get("ingested_at") or None,
        "content_ts": content_ts.isoformat() if content_ts else None,
        "pm_mode": c.get("pm_mode"),
        "idempotency_key": str(value["idempotency_key"]),
        "kafka_offset": kafka_offset,
    }
    return row, content_ts


def parse_record(msg):
    """
    Kafka 레코드 → (rows, rejects, message_row).
    값 전체가 JSON 이 아니면 레코드 하나가 통째로 reject 한 건이 된다.
    """
    offset = msg.offset()
    headers = decode_headers(msg.headers())
    ts_type, ts_ms = msg.timestamp()
    kafka_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms and ts_ms > 0 else datetime.now(timezone.utc)
    msg_ts = None
    if headers.get("msg_timestamp"):
        try:
            msg_ts = ns_to_dt(headers["msg_timestamp"])
        except (TypeError, ValueError, OverflowError, OSError):
            msg_ts = None

    rows, rejects = [], []
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

    for item in body:
        try:
            row, content_ts = parse_item(item, offset)
            rows.append((row, content_ts))
        except Reject as r:
            rejects.append((offset, json.dumps(r.payload, ensure_ascii=False, default=str), r.reason))

    message_row = (
        msg.partition(), offset, kafka_ts,
        headers.get("uniqueid") or None, msg_ts,
        headers.get("message_version"), headers.get("method_id"),
        len(body), len(rejects),
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


def write_batch(conn, rows, rejects, message_rows):
    """한 트랜잭션. 성공하면 (inserted, duplicates)."""
    with conn.transaction():
        with conn.cursor() as cur:
            inserted = 0
            if rows:
                cur.execute(INSERT_STATUS, (json.dumps([r for r, _ in rows]),))
                inserted = cur.rowcount
                # 배치 안 장비별 최신 항목만 골라 상태 표에 반영한다.
                latest = {}
                for row, content_ts in rows:
                    key = content_ts.timestamp() if content_ts else 0.0
                    prev = latest.get(row["tid"])
                    if prev is None or key >= prev[0]:
                        latest[row["tid"]] = (key, row)
                state_rows = [
                    {
                        "tid": r["tid"], "status": r["status"], "error_code": r["error_code"],
                        "scan_rate_pts_per_sec": r["scan_rate_pts_per_sec"],
                        "point_cloud_quality_score": r["point_cloud_quality_score"],
                        "temperature_c": r["temperature_c"], "connectivity_rssi": r["connectivity_rssi"],
                        "last_event_at": r["time"], "last_heartbeat_at": r["last_heartbeat_at"],
                    }
                    for _, r in latest.values()
                ]
                cur.execute(UPSERT_STATE, (json.dumps(state_rows),))
            if rejects:
                cur.executemany(INSERT_REJECT, rejects)
            if message_rows:
                cur.executemany(INSERT_MESSAGE, message_rows)
    return inserted, len(rows) - inserted


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
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Kafka 구독 bootstrap=%s topic=%s group=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC, KAFKA_GROUP_ID)
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

        rows, rejects, message_rows = [], [], []
        for msg in msgs:
            if msg.error():
                log.error("Kafka 오류: %s", msg.error())
                continue
            MESSAGES.inc()
            r, j, m = parse_record(msg)
            rows.extend(r)
            rejects.extend(j)
            message_rows.append(m)
            for row, _ in r:
                ITEMS.labels(status=row["status"]).inc()
            for _, _, reason in j:
                REJECTS.labels(reason=reason.split(":")[0]).inc()
        if not message_rows:
            continue

        # DB 쓰기. 실패하면 같은 배치를 물고 재시도한다 — 오프셋은 커밋하지 않는다.
        delay = 1
        while running:
            t0 = time.monotonic()
            try:
                inserted, dups = write_batch(conn, rows, rejects, message_rows)
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
        BATCH_ROWS.observe(len(rows))
        ROWS_INSERTED.inc(inserted)
        ROWS_DUPLICATE.inc(dups)
        LAST_COMMIT.set(now)
        newest = None
        for _, content_ts in rows:
            if content_ts:
                END_TO_END.observe(max(now - content_ts.timestamp(), 0.0))
                if newest is None or content_ts > newest:
                    newest = content_ts
        if newest:
            LAST_EVENT.set(newest.timestamp())
        update_lag(consumer)
        log.info(
            "batch records=%d rows=%d inserted=%d dup=%d reject=%d db=%.3fs offset=%d",
            len(message_rows), len(rows), inserted, dups, len(rejects), elapsed, message_rows[-1][1],
        )

    consumer.close()
    conn.close()
    log.info("종료")


if __name__ == "__main__":
    main()
