#!/usr/bin/env sh
set -eu

CLI_CONTAINER="${CLI_CONTAINER:-mqtt-cli}"
BROKER_HOST="${BROKER_HOST:-host.containers.internal}"
# Dot-separated topics are a single MQTT level, so prefix wildcards do not apply.
# Subscribe to everything and narrow down with TOPIC_PREFIX instead.
TOPIC_FILTER="${TOPIC_FILTER:-#}"
TOPIC_PREFIX="${TOPIC_PREFIX:-ot.device.fabrication.}"
SUBSCRIBE_TIMEOUT_SECONDS="${SUBSCRIBE_TIMEOUT_SECONDS:-2}"
BROKER_PORTS="${BROKER_PORTS:-1883 1884 1885}"

tmp_dir="$(mktemp -d)"
trap 'rm -f "$tmp_dir"/retained-*.txt; rmdir "$tmp_dir" 2>/dev/null || true' EXIT HUP INT TERM

printf 'Clearing retained topics starting with %s\n' "$TOPIC_PREFIX"

for port in $BROKER_PORTS; do
    retained_topics="$tmp_dir/retained-$port.txt"

    # A timeout is expected after the broker has delivered its retained messages.
    podman exec "$CLI_CONTAINER" mosquitto_sub \
        -h "$BROKER_HOST" -p "$port" -t "$TOPIC_FILTER" \
        -F '%r|%t' -W "$SUBSCRIBE_TIMEOUT_SECONDS" 2>/dev/null \
        | awk -F '|' -v prefix="$TOPIC_PREFIX" \
            '$1 == "1" { sub(/^[^|]*\|/, ""); if (index($0, prefix) == 1) print }' \
        | sort -u > "$retained_topics" || true

    if [ ! -s "$retained_topics" ]; then
        printf '[%s] No retained topics found\n' "$port"
        continue
    fi

    while IFS= read -r topic; do
        [ -n "$topic" ] || continue
        podman exec "$CLI_CONTAINER" mosquitto_pub \
            -h "$BROKER_HOST" -p "$port" -t "$topic" -r -n
        printf '[%s] Cleared %s\n' "$port" "$topic"
    done < "$retained_topics"
done

printf 'Retained topic initialization complete\n'
