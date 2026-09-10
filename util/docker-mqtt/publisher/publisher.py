"""장비 heartbeat 시뮬레이터 (MQTT Agent 수신 테스트용).

브로커 / 토픽 구분자 / 페이로드 스키마를 환경변수로 바꿔가며 발행한다.
검증 목적상 다음 3가지를 실제로 태울 수 있게 만들어져 있다.

  * TOPIC_SEPARATOR=dot|slash  → NATS 경유 시 subject 가 어떻게 보이는지 실측
  * QOS=2                      → NATS 가 QoS 2 를 거부하는지 실측
  * RETAIN / LWT               → 재접속·비정상 종료 시 상태 공백이 메워지는지 실측
"""

import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

KST = timezone(timedelta(hours=9))


def env(key, default):
    return os.environ.get(key, default)


def env_bool(key, default):
    return env(key, str(default)).strip().lower() in ("1", "true", "yes", "y")


BROKER_HOST = env("BROKER_HOST", "mosquitto")
BROKER_PORT = int(env("BROKER_PORT", "1883"))
DEVICE_ID = env("DEVICE_ID", "lidar-deviceid-01")
ZONE = env("ZONE", "fabrication")
DEVICE_TYPE = env("DEVICE_TYPE", "lidar")
LOCATION = env("LOCATION", "가공")
SEPARATOR = env("TOPIC_SEPARATOR", "slash").strip().lower()
SCHEMA = env("PAYLOAD_SCHEMA", "generic").strip().lower()
INTERVAL_SEC = float(env("INTERVAL_SEC", "5"))
QOS = int(env("QOS", "1"))
RETAIN = env_bool("RETAIN", True)
CLEAN_SESSION = env_bool("CLEAN_SESSION", False)


def build_topic():
    """토픽 구분자를 슬래시/점 중 하나로 조립한다.

    슬래시가 MQTT 표준 계층 구분자이고, 점은 단일 레벨 토픽이 된다.
    NATS 경유 시 어떤 subject 로 변환되는지 비교하려고 둘 다 지원한다.
    """
    parts = ["ot", "device", ZONE, DEVICE_TYPE, "status"]
    if SEPARATOR == "dot":
        return ".".join(parts)
    return "/".join(parts)


TOPIC = build_topic()


def now_iso(with_ms=False):
    fmt = "%Y-%m-%dT%H:%M:%S.%f%z" if with_ms else "%Y-%m-%dT%H:%M:%S%z"
    stamp = datetime.now(KST).strftime(fmt)
    if with_ms:
        # microsecond(6자리) → millisecond(3자리) 로 절삭.
        # 뒤쪽 5자는 타임존 오프셋(+0900)이므로 그 앞의 3자리만 잘라낸다.
        stamp = stamp[:-8] + stamp[-5:]
    # +0900 → +09:00
    return stamp[:-2] + ":" + stamp[-2:]


def raw_metrics():
    return {
        "scan_rate_pts_per_sec": random.randint(300000, 340000),
        "point_cloud_quality_score": round(random.uniform(0.85, 0.99), 2),
        "temperature_c": round(random.uniform(38.0, 46.0), 1),
        "connectivity_rssi": random.randint(-75, -50),
    }


def build_payload(status="ONLINE", error_code=None):
    occurred_at = now_iso()

    if SCHEMA == "device":
        # 문서 08 5-2 "장비 → 브로커(raw publish) 계약".
        # ingested_at / idempotency_key 는 Agent 가 채우므로 장비는 보내지 않는다.
        return {
            "schema_version": 1,
            "device_id": DEVICE_ID,
            "location": LOCATION,
            "occurred_at": occurred_at,
            "status": status,
            "error_code": error_code,
            "raw_payload": raw_metrics(),
        }

    # generic: "MQTT Agent 개발.md" 의 범용 메시지 형식 {id, raw_payload}
    payload = {
        "id": DEVICE_ID,
        "raw_payload": {
            "last_heartbeat_at": occurred_at,
            "error_code": error_code,
            "occurred_at": occurred_at,
            "ingested_at": now_iso(with_ms=True),
            "idempotency_key": f"{DEVICE_ID}:{occurred_at}",
            "status": status,
        },
    }
    payload["raw_payload"].update(raw_metrics())
    return payload


def encode(payload):
    # 이중 인코딩 금지 — 페이로드는 JSON 문자열이 아니라 JSON 그 자체여야 한다.
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print(f"[connected] {BROKER_HOST}:{BROKER_PORT} "
              f"session_present={flags.session_present}", flush=True)
    else:
        print(f"[connect-failed] rc={reason_code}", flush=True)


def on_disconnect(client, userdata, flags, reason_code, properties=None):
    print(f"[disconnected] rc={reason_code}", flush=True)


def main():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"sim-{DEVICE_ID}",
        protocol=mqtt.MQTTv311,          # NATS 는 3.1.1 까지만 지원
        clean_session=CLEAN_SESSION,
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # LWT: 프로세스가 비정상 종료되면 브로커가 대신 OFFLINE 을 발행한다.
    client.will_set(
        TOPIC,
        encode(build_payload(status="OFFLINE", error_code="UNEXPECTED_DISCONNECT")),
        qos=QOS,
        retain=RETAIN,
    )

    print(f"[config] topic={TOPIC} schema={SCHEMA} qos={QOS} "
          f"retain={RETAIN} clean_session={CLEAN_SESSION} interval={INTERVAL_SEC}s",
          flush=True)

    client.connect(BROKER_HOST, BROKER_PORT, keepalive=30)
    client.loop_start()

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    seq = 0
    while running:
        seq += 1
        # 20 회에 한 번 ERROR 를 섞어 상태 전이도 확인할 수 있게 한다.
        if seq % 20 == 0:
            payload = build_payload(status="ERROR", error_code="SENSOR_TIMEOUT")
        else:
            payload = build_payload()

        info = client.publish(TOPIC, encode(payload), qos=QOS, retain=RETAIN)
        info.wait_for_publish(timeout=10)
        print(f"[pub #{seq}] {TOPIC} rc={info.rc}", flush=True)

        for _ in range(int(INTERVAL_SEC * 10)):
            if not running:
                break
            time.sleep(0.1)

    # 정상 종료 시에는 LWT 가 발행되지 않으므로 직접 OFFLINE 을 남긴다.
    client.publish(
        TOPIC,
        encode(build_payload(status="OFFLINE", error_code=None)),
        qos=QOS,
        retain=RETAIN,
    ).wait_for_publish(timeout=10)

    client.loop_stop()
    client.disconnect()
    print("[stopped]", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
