#!/usr/bin/env sh
set -eu

CLI_CONTAINER="${CLI_CONTAINER:-mqtt-cli}"
BROKER_HOST="${BROKER_HOST:-host.containers.internal}"
BROKER_PORT="${BROKER_PORT:-1884}"
TOPIC="${TOPIC:-ot.device.fabrication.emqx.status}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-2}"
DEVICE_IDS="${DEVICE_IDS:-emqx-device-1 emqx-device-2 emqx-device-3}"

random_int() {
    min="$1"
    max="$2"
    random_value="$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')"
    awk -v min="$min" -v max="$max" -v random_value="$random_value" \
        'BEGIN { print min + (random_value % (max - min + 1)) }'
}

random_decimal() {
    min="$1"
    max="$2"
    scale="$3"
    random_value="$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')"
    awk -v min="$min" -v max="$max" -v scale="$scale" -v random_value="$random_value" \
        'BEGIN { printf "%.*f", scale, min + (random_value / 4294967295) * (max - min) }'
}

pick_status() {
    case "$(random_int 0 3)" in
        0) printf 'ONLINE' ;;
        1) printf 'OFFLINE' ;;
        2) printf 'ERROR' ;;
        *) printf 'CALIBRATING' ;;
    esac
}

make_key() {
    timestamp="$(date -u +%Y%m%dT%H%M%S)"
    printf '%s-%s-%s' "$1" "$timestamp" "$(random_int 100000 999999)"
}

publish_one() {
    id="$1"
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    key="$(make_key emqx)"
    status="$(pick_status)"
    if [ "$status" = "ERROR" ]; then
        error_code="\"ERR_$(random_int 100 999)\""
    else
        error_code="null"
    fi

    payload=$(printf '{"id":"%s","raw_payload":{"last_heartbeat_at":"%s","error_code":%s,"occurred_at":"%s","ingested_at":"%s","idempotency_key":"%s","status":"%s","scan_rate_pts_per_sec":%s,"point_cloud_quality_score":%s}}' \
        "$id" "$now" "$error_code" "$now" "$now" "$key" "$status" \
        "$(random_int 1000 100000)" "$(random_decimal 0.70 1.00 3)")

    podman exec "$CLI_CONTAINER" mosquitto_pub \
        -h "$BROKER_HOST" -p "$BROKER_PORT" -t "$TOPIC" -m "$payload"
    printf '[EMQX] %s %s\n' "$now" "$payload"
}

printf 'Publishing fixed device IDs every %ss to mqtt://%s:%s/%s: %s (Ctrl+C to stop)\n' \
    "$INTERVAL_SECONDS" "$BROKER_HOST" "$BROKER_PORT" "$TOPIC" "$DEVICE_IDS"

while :; do
    for id in $DEVICE_IDS; do
        publish_one "$id"
    done
    sleep "$INTERVAL_SECONDS"
done
