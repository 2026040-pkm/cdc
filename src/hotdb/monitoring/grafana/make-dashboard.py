#!/usr/bin/env python3
"""HotDB CDC 대시보드 생성기 → dashboards/hotdb-cdc.json

    python make-dashboard.py

패널은 아래 ROWS 표만 고친다. Grafana 화면에서 고친 것은 이 파일에 옮기지 않으면 다음 생성 때 사라진다.
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "hotdb-prom"}

# (제목, 종류, 단위, [(쿼리, 범례)], 설명)
ROWS = [
    ("한눈에", [
        ("권역 반영 지연 p99 (DB 시계)", "stat", "s",
         [("max by (zone) (hotdb_zone_apply_lag_p99_seconds)", "{{zone}}")],
         "tsdb 수신(received_at) → 권역 RDB 반영(applied_at). 둘 다 DB 시계라 서버 간 시계 편차가 없다. 합격 기준 ≤ 2초"),
        ("tsdb 적재 행/초", "stat", "short",
         [("sum(rate(hotdb_hypertable_inserted_rows[1m]))", "")], "HotDB Provider(현재는 시뮬레이터)가 넣는 속도. 기준 ≈ 426"),
        ("CDC 반영 행/초", "stat", "short",
         [("sum(rate(hotdb_cdc_route_applied_total[1m]))", "")], "모든 권역 서비스 · 라우트의 합"),
        ("슬롯 최대 보존 WAL", "stat", "bytes",
         [("max(hotdb_slot_retained_bytes)", "")], "1GB 경고 · 10GB 에서 슬롯 무효화"),
        ("dead letter", "stat", "short",
         [("sum(hotdb_dead_letter_total) or vector(0)", "")], "0 이 아니면 ops.cdc_dead_letter 의 error 를 본다"),
        ("권역 서비스 UP", "stat", "short",
         [("sum(up{job=\"zone-service\"})", "UP"), ("count(up{job=\"zone-service\"})", "전체")], ""),
    ]),
    ("CDC 처리량 · 지연", [
        ("tsdb 적재 행/초 (하이퍼테이블별)", "timeseries", "short",
         [("rate(hotdb_hypertable_inserted_rows[1m])", "{{hypertable}}")], ""),
        ("CDC 수신 이벤트/초 (서비스별)", "timeseries", "short",
         [("sum by (pipeline) (rate(hotdb_cdc_events_total[1m]))", "{{pipeline}}")],
         "서비스마다 tsdb 전체 WAL 을 받는다 — 권역 필터는 서비스 안에서 한다. 서비스 수만큼 배로 는다"),
        ("라우트 반영 행/초", "timeseries", "short",
         [("sum by (pipeline, route) (rate(hotdb_cdc_route_applied_total[1m]))", "{{pipeline}} · {{route}}")], ""),
        ("반영 지연 (DB 시계)", "timeseries", "s",
         [("hotdb_zone_apply_lag_p50_seconds", "{{zone}} p50"), ("hotdb_zone_apply_lag_p99_seconds", "{{zone}} p99"),
          ("hotdb_zone_apply_lag_max_seconds", "{{zone}} max")], "device_status_current 의 applied_at - src_received_at"),
        ("원천 커밋 → 반영 p99 (앱 시계)", "timeseries", "s",
         [("histogram_quantile(0.99, sum by (le, pipeline) (rate(hotdb_cdc_lag_commit_seconds_bucket[1m])))", "{{pipeline}}")],
         "Debezium source.ts_ms 기준. 서버가 다르면 시계 편차가 섞인다"),
        ("배치 처리 시간 p99 · 평균 배치 크기", "timeseries", "s",
         [("histogram_quantile(0.99, sum by (le, pipeline) (rate(hotdb_cdc_batch_seconds_bucket[1m])))", "{{pipeline}} p99"),
          ("sum by (pipeline) (rate(hotdb_cdc_batch_size_sum[1m])) / sum by (pipeline) (rate(hotdb_cdc_batch_size_count[1m]))", "{{pipeline}} 크기(건)")], ""),
        ("버린 · 거른 이벤트/초", "timeseries", "short",
         [("sum by (pipeline, reason) (rate(hotdb_cdc_events_ignored_total[1m]))", "{{pipeline}} 버림 {{reason}}"),
          ("sum by (pipeline) (rate(hotdb_cdc_route_filtered_total[1m]))", "{{pipeline}} 다른 권역")],
         "no_route = 라우트 없는 표(heartbeat 등) · unknown_chunk = 압축 뭉치 등"),
        ("재시도 · lookup 다시 읽기", "timeseries", "short",
         [("sum by (pipeline) (rate(hotdb_cdc_apply_retry_total[1m]))", "{{pipeline}} 재시도"),
          ("sum by (pipeline, reason) (rate(hotdb_cdc_lookup_reload_total[1m]))", "{{pipeline}} reload {{reason}}")], ""),
    ]),
    ("HotDB · WAL · 슬롯", [
        ("슬롯별 WAL", "timeseries", "bytes",
         [("hotdb_slot_retained_bytes", "{{slot_name}} 보존"), ("hotdb_slot_confirmed_lag_bytes", "{{slot_name}} 미확인")],
         "소비자가 멈추면 계속 오른다"),
        ("WAL 생성률", "timeseries", "Bps", [("rate(hotdb_wal_written_bytes[1m])", "WAL")], ""),
        ("walsender 미전송", "timeseries", "bytes",
         [("hotdb_walsender_sent_lag_bytes", "{{slot_name}} ({{state}})")], ""),
        ("heartbeat 경과", "timeseries", "s", [("hotdb_heartbeat_age_seconds", "{{pipeline}}")], "60초 넘으면 경보"),
        ("하이퍼테이블 크기", "timeseries", "bytes", [("hotdb_hypertable_bytes", "{{hypertable}}")], ""),
        ("청크 수 (압축)", "timeseries", "short",
         [("hotdb_hypertable_chunks", "{{hypertable}}"), ("hotdb_hypertable_compressed_chunks", "{{hypertable}} 압축")], ""),
        ("DB 접속", "timeseries", "short",
         [("sum by (state, usename) (pg_stat_activity_count{datname=\"hotdb\"})", "{{usename}} {{state}}")], ""),
        ("DB 트랜잭션/초", "timeseries", "short",
         [("rate(pg_stat_database_xact_commit{datname=\"hotdb\"}[1m])", "commit"),
          ("rate(pg_stat_database_xact_rollback{datname=\"hotdb\"}[1m])", "rollback")], ""),
    ]),
    ("리소스 (CPU · 메모리)", [
        ("컨테이너 CPU (코어)", "timeseries", "short",
         [("hotdb:container_cpu_cores{name=~\"hotdb-.*\"}", "{{name}}")], "1 = 코어 하나를 다 쓴 것"),
        ("컨테이너 메모리", "timeseries", "bytes",
         [("hotdb:container_mem_bytes{name=~\"hotdb-.*\"}", "{{name}}")], ""),
        ("호스트 CPU 사용률", "timeseries", "percentunit",
         [("1 - avg by (server) (rate(node_cpu_seconds_total{mode=\"idle\"}[1m]))", "{{server}}")], ""),
        ("호스트 메모리 사용", "timeseries", "bytes",
         [("node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes", "{{server}}")], ""),
        ("JVM 힙 사용", "timeseries", "bytes",
         [("sum by (pipeline) (jvm_memory_used_bytes{area=\"heap\"})", "{{pipeline}}")], ""),
        ("프로세스 CPU (JVM)", "timeseries", "percentunit",
         [("process_cpu_usage", "{{pipeline}}")], "JVM 이 보는 자기 프로세스 CPU (호스트 전체 대비)"),
        ("네트워크 (서버 간)", "timeseries", "Bps",
         [("sum by (server) (rate(node_network_receive_bytes_total{device!~\"lo|veth.*|cni.*|podman.*\"}[1m]))", "{{server}} 수신"),
          ("sum by (server) (rate(node_network_transmit_bytes_total{device!~\"lo|veth.*|cni.*|podman.*\"}[1m]))", "{{server}} 송신")],
         "서버 to 서버 시험에서 WAL 스트림이 서버 수만큼 나간다"),
        ("GC 일시정지/초", "timeseries", "s",
         [("sum by (pipeline) (rate(jvm_gc_pause_seconds_sum[1m]))", "{{pipeline}}")], ""),
    ]),
    ("2단 CDC — 권역 RDB → RFC Provider → SAP", [
        ("전 구간 지연 p99 (DB 시계)", "stat", "s",
         [("max by (zone) (hotdb_rfc_lag_e2e_p99_seconds)", "{{zone}}")],
         "Provider 수신(received_at) → SAP 송신(sent_at). CDC 를 두 번 거친다"),
        ("2단 지연 p99 (DB 시계)", "stat", "s",
         [("max by (zone) (hotdb_rfc_lag_hop2_p99_seconds)", "{{zone}}")], "권역 RDB 반영(applied_at) → SAP 송신"),
        ("SAP 송신/초", "stat", "short", [("sum(rate(hotdb_rfc_sent_total[1m])) or vector(0)", "")], "dry-run 이어도 같이 센다"),
        ("RFC Provider UP", "stat", "short", [("sum(up{job=\"rfc-provider\"})", "UP")], ""),
        ("송신 · 중복 건/초 (권역별)", "timeseries", "short",
         [("sum by (zone) (rate(hotdb_rfc_sent_total[1m]))", "{{zone}} 송신"),
          ("sum by (zone) (rate(hotdb_rfc_duplicate_total[1m]))", "{{zone}} 중복(이미 보냄)")],
         "중복 = 권역 서비스의 재전송 UPSERT 가 낸 UPDATE. ops.rfc_sent 가 걸러 SAP 로는 한 번만 간다"),
        ("지연 (DB 시계)", "timeseries", "s",
         [("hotdb_rfc_lag_hop2_p99_seconds", "{{zone}} 2단 p99"), ("hotdb_rfc_lag_e2e_p99_seconds", "{{zone}} 전 구간 p99"),
          ("hotdb_rfc_lag_e2e_max_seconds", "{{zone}} 전 구간 max")], ""),
    ]),
    ("레거시 폴링 (RFC Service · DB Agent)", [
        ("폴링 작업 실패", "stat", "short", [("sum(hotdb_poll_failing) or vector(0)", "")], "ops.poll_state.last_error"),
        ("가장 오래된 성공", "stat", "s", [("max(hotdb_poll_ok_age_seconds)", "")], "주기 60초 · 5분 넘으면 경보"),
        ("옮긴 행/분", "timeseries", "short",
         [("sum by (job) (rate(hotdb_poll_rows_total[5m])) * 60", "{{job}}")], "원천 → 레거시 DB"),
        ("작업별 마지막 성공 이후", "timeseries", "s",
         [("hotdb_poll_ok_age_seconds", "{{agent}} · {{job}}")], ""),
        ("한 주기 시간 p99", "timeseries", "s",
         [("histogram_quantile(0.99, sum by (le, job) (rate(hotdb_poll_run_seconds_bucket[5m])))", "{{job}}")], ""),
        ("폴링 오류/분", "timeseries", "short",
         [("sum by (job) (rate(hotdb_poll_errors_total[5m])) * 60", "{{job}}")], ""),
    ]),
    ("시뮬레이터", [
        ("발행 행/초 (채널별)", "timeseries", "short",
         [("sum by (channel) (rate(hotdb_sim_rows_total[1m]))", "{{channel}}")], ""),
        ("배속", "stat", "short", [("hotdb_sim_speed", "x")], "POST /sim/speed?value=N"),
        ("적재 한 주기 p99", "timeseries", "s",
         [("histogram_quantile(0.99, sum by (le, channel) (rate(hotdb_sim_tick_seconds_bucket[1m])))", "{{channel}}")],
         "주기(1초)보다 길어지면 발행기가 밀린다 — 그 배속은 HotDB 적재 한계다"),
        ("적재 실패", "timeseries", "short",
         [("sum by (channel) (rate(hotdb_sim_errors_total[1m]))", "{{channel}}")], ""),
    ]),
]


def panel(pid, title, kind, unit, queries, desc, x, y, w, h):
    p = {
        "id": pid, "type": kind, "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "targets": [{"refId": chr(65 + i), "datasource": DS, "expr": q, "legendFormat": lg}
                    for i, (q, lg) in enumerate(queries)],
    }
    if kind == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 1, "fillOpacity": 8, "showPoints": "never"}
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
    else:
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area"}
    return p


def main():
    panels, pid, y = [], 1, 0
    for row_title, items in ROWS:
        panels.append({"id": pid, "type": "row", "title": row_title, "collapsed": False,
                       "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
        pid += 1
        y += 1
        stats = [i for i in items if i[1] == "stat"]
        series = [i for i in items if i[1] != "stat"]
        if stats:
            w = 24 // len(stats)
            for n, (t, k, u, q, d) in enumerate(stats):
                panels.append(panel(pid, t, k, u, q, d, n * w, y, w, 4))
                pid += 1
            y += 4
        for n, (t, k, u, q, d) in enumerate(series):
            panels.append(panel(pid, t, k, u, q, d, (n % 2) * 12, y + (n // 2) * 8, 12, 8))
            pid += 1
        y += ((len(series) + 1) // 2) * 8

    dash = {
        "uid": "hotdb-cdc", "title": "HotDB CDC", "tags": ["hotdb", "cdc"], "timezone": "browser",
        "schemaVersion": 39, "version": 1, "refresh": "5s",
        "time": {"from": "now-30m", "to": "now"}, "panels": panels,
    }
    out = Path(__file__).resolve().parent / "dashboards" / "hotdb-cdc.json"
    out.write_text(json.dumps(dash, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{out.name}: 패널 {pid - 1}")


if __name__ == "__main__":
    main()
