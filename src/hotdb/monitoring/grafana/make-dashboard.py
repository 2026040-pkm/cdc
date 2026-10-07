#!/usr/bin/env python3
"""HotDB CDC 대시보드 생성기 → dashboards/hotdb-cdc.json · dashboards/hotdb-engine.json

    python make-dashboard.py

패널은 아래 ROWS · ENGINE_ROWS 표만 고친다. Grafana 화면에서 고친 것은 이 파일에 옮기지 않으면 다음 생성 때 사라진다.
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
         [("max(hotdb_slot_retained_bytes)", "")], "1GB 경고 · 20GB(max_slot_wal_keep_size) 에서 슬롯 무효화"),
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
    ("2단 CDC — 권역 RDB → RFC Service → SAP", [
        ("전 구간 지연 p99 (DB 시계)", "stat", "s",
         [("max by (zone) (hotdb_rfc_lag_e2e_p99_seconds)", "{{zone}}")],
         "Provider 수신(received_at) → SAP 송신(sent_at). CDC 를 두 번 거친다"),
        ("2단 지연 p99 (DB 시계)", "stat", "s",
         [("max by (zone) (hotdb_rfc_lag_hop2_p99_seconds)", "{{zone}}")], "권역 RDB 반영(applied_at) → SAP 송신"),
        ("SAP 송신/초", "stat", "short", [("sum(rate(hotdb_rfc_sent_total[1m])) or vector(0)", "")], "dry-run 이어도 같이 센다"),
        ("RFC Service UP", "stat", "short", [("sum(up{job=\"rfc-service\"})", "UP")], ""),
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
    ("레거시 발행기 (SAP · Oracle 대역 원천 표)", [
        ("원천 변경 행/분 (대역 · 종류별)", "timeseries", "short",
         [("sum by (source, op) (rate(hotdb_lsim_rows_total[1m])) * 60", "{{source}} {{op}}")],
         "주기마다 표마다 UPDATE(워터마크 = 지금) · INSERT. 이 값이 '레거시 폴링 > 옮긴 행/분' 으로 따라와야 한다"),
        ("배속 · 표 수", "stat", "short", [("hotdb_lsim_speed", "배속"), ("hotdb_lsim_tables", "표")], "POST /sim/speed?value=N"),
        ("한 주기 시간 p99", "timeseries", "s",
         [("histogram_quantile(0.99, sum by (le) (rate(hotdb_lsim_tick_seconds_bucket[1m])))", "p99")],
         "주기(10초)보다 길어지면 대역 DB 가 못 따라가는 것"),
        ("실패/분 (대역별)", "timeseries", "short",
         [("sum by (source) (rate(hotdb_lsim_errors_total[1m])) * 60", "{{source}}")], "대역이 내려가 있으면 표마다 하나씩"),
    ]),
]

# ── HotDB 엔진 자원 — Embedded 엔진이 서비스 프로세스 안에서 쓰는 몫과 그 영향 ──────────────────────
# $pipeline 은 대시보드 변수 (zone-asm · rfc-service …)
P = 'pipeline=~"$pipeline"'
HEAP_MAX = f'sum by (pipeline) (jvm_memory_max_bytes{{area="heap",{P}}} > 0)'
HEAP_USED = f'sum by (pipeline) (jvm_memory_used_bytes{{area="heap",{P}}})'
DISK = 'min(max_over_time(node_filesystem_avail_bytes{hotdb_db="1", mountpoint="/"}[5m]))'
SLOT_CAP = 'max(pg_settings_max_slot_wal_keep_size_bytes{job="hotdb"})'
ENGINE_ROWS = [
    ("① 엔진이 프로세스 안에서 쓰는 자원 (평시 · 1만 건 배치 · 적체)", [
        ("엔진 CPU 몫 (프로세스 대비)", "stat", "percentunit",
         [(f'hotdb:engine_cpu_share{{{P}}}', "{{pipeline}}")],
         "Debezium 스레드 CPU / 프로세스 CPU. 배치 콜백(우리 반영)은 빠진 순수 캡처 몫"),
        ("엔진 힙 할당 몫", "stat", "percentunit",
         [(f'sum by (pipeline) (rate(hotdb_cdc_engine_alloc_bytes_total{{part="engine",{P}}}[1m])) / '
           f'sum by (pipeline) (rate(hotdb_cdc_engine_alloc_bytes_total{{part="process",{P}}}[1m]))', "{{pipeline}}")],
         "GC 는 할당한 쪽에 안 붙는다 — 할당 몫이 GC 를 누가 부르는지 말해 준다"),
        ("엔진 스레드", "stat", "short",
         [(f'hotdb_cdc_engine_threads{{{P}}}', "{{pipeline}}")], "엔진 스레드 그룹의 살아 있는 스레드"),
        ("프로세스 CPU 몫별 (코어)", "timeseries", "short",
         [(f'hotdb:proc_cpu_cores{{{P}, part!="process"}}', "{{pipeline}} {{part}}"),
          (f'sum by (pipeline) (hotdb:proc_cpu_cores{{{P}, part="process"}}) - '
           f'sum by (pipeline) (hotdb:proc_cpu_cores{{{P}, part!="process"}})', "{{pipeline}} JVM 나머지")],
         "engine = 슬롯 읽기 · 해석 · 큐 / handler = 배치 콜백(라우트 반영 · RFC 전달) / work:judge = 판정 · "
         "work:poll = 레거시 폴링 / JVM 나머지 = GC · JIT · HTTP"),
        ("힙 할당률 몫별", "timeseries", "Bps",
         [(f'sum by (pipeline, part) (rate(hotdb_cdc_engine_alloc_bytes_total{{{P}, part!="process"}}[1m]))', "{{pipeline}} {{part}}"),
          (f'sum by (pipeline, part) (rate(hotdb_work_alloc_bytes_total{{{P}}}[1m]))', "{{pipeline}} work:{{part}}"),
          (f'sum by (pipeline) (rate(hotdb_cdc_engine_alloc_bytes_total{{{P}, part="process"}}[1m]))', "{{pipeline}} 프로세스")], ""),
        ("스레드", "timeseries", "short",
         [(f'hotdb_cdc_engine_threads{{{P}}}', "{{pipeline}} 엔진"),
          (f'hotdb_work_threads{{{P}}}', "{{pipeline}} work:{{part}}"),
          (f'jvm_threads_live_threads{{{P}}}', "{{pipeline}} JVM 전체")], ""),
        ("힙 — 사용 · GC 뒤 살아 있는 양 · 상한", "timeseries", "bytes",
         [(HEAP_USED, "{{pipeline}} 사용"),
          (f'jvm_gc_live_data_size_bytes{{{P}}}', "{{pipeline}} GC 뒤 생존"),
          (HEAP_MAX, "{{pipeline}} 상한(-Xmx)")],
         "힙은 스레드별로 못 나눈다. 엔진만의 메모리는 엔진을 끈 쌍둥이와의 차이로 본다 (engine-resource.py)"),
        ("GC 일시정지/초", "timeseries", "s",
         [(f'sum by (pipeline) (rate(jvm_gc_pause_seconds_sum{{{P}}}[1m]))', "{{pipeline}}")],
         "멈추는 동안 엔진과 본연의 일이 같이 선다"),
    ]),
    ("② 본연의 일(반영 · 판정 · RFC 전달 · 폴링) ↔ CDC 캡처 — 같은 프로세스에서의 상호 영향", [
        ("캡처 vs 본연의 일 CPU (코어)", "timeseries", "short",
         [(f'hotdb:proc_cpu_cores{{{P}, part="engine"}}', "{{pipeline}} 캡처(엔진)"),
          (f'sum by (pipeline) (hotdb:proc_cpu_cores{{{P}, part=~"handler|work:.*"}})', "{{pipeline}} 본연의 일")],
         "한쪽이 오를 때 다른 쪽이 눌리면(합이 코어 한도에 붙음) 서로 자원을 다투는 것"),
        ("캡처 지연 — 원천 대비", "timeseries", "s",
         [(f'hotdb_cdc_engine_behind_source_seconds{{{P}}}', "{{pipeline}} Debezium"),
          (f'histogram_quantile(0.99, sum by (le, pipeline) (rate(hotdb_cdc_lag_commit_seconds_bucket{{{P}}}[1m])))', "{{pipeline}} 커밋→반영 p99")],
         "본연의 일이 무거워져 캡처가 밀리면 여기가 오른다"),
        ("본연의 일 지연", "timeseries", "s",
         [(f'histogram_quantile(0.99, sum by (le, pipeline) (rate(hotdb_cdc_batch_seconds_bucket{{{P}}}[1m])))', "{{pipeline}} 배치 반영 p99"),
          (f'hotdb_judge_pending_oldest_seconds{{{P}}}', "{{pipeline}} 판정 대기 최장"),
          ('max by (zone) (hotdb_rfc_lag_hop2_p99_seconds)', "RFC 전달 p99 ({{zone}})"),
          (f'histogram_quantile(0.99, sum by (le, pipeline) (rate(hotdb_poll_run_seconds_bucket{{{P}}}[5m])))', "{{pipeline}} 폴링 한 주기 p99")],
         "캡처가 몰릴 때(적체 따라잡기) 이 값들이 오르면 캡처가 본연의 일을 누르는 것"),
        ("판정 적체 · 처리량", "timeseries", "short",
         [(f'hotdb_judge_pending{{{P}}}', "{{pipeline}} 대기"),
          (f'sum by (pipeline) (rate(hotdb_judge_judged_total{{{P}}}[1m])) * 60', "{{pipeline}} 판정/분")], ""),
    ]),
    ("③ 원천 DB 부하 — 복제 슬롯 · WAL 보존 · 디스크 · 장기 다운 허용 한계", [
        ("다운 허용 — 슬롯 무효화까지", "stat", "s",
         [('min(hotdb:slot_downtime_budget_seconds)', "")],
         "소비자가 지금 멈추면, 지금 WAL 속도로 이 시간 뒤 슬롯이 무효화된다 (max_slot_wal_keep_size). 넘으면 재동기화"),
        ("다운 허용 — 디스크 가득까지", "stat", "s",
         [('hotdb:disk_downtime_budget_seconds', "")], "이게 위보다 짧으면 슬롯보다 디스크가 먼저 차서 DB 가 멈춘다"),
        ("pg_wal 크기", "stat", "bytes", [('max(hotdb_wal_dir_bytes)', "")], "슬롯이 붙잡으면 커진다"),
        ("디스크 여유", "stat", "bytes", [(DISK, "")], "Hot DB 데이터 디스크 (node-exporter 대상 라벨 hotdb_db=1)"),
        ("슬롯 WAL 상한", "stat", "bytes", [(SLOT_CAP, "")], "max_slot_wal_keep_size"),
        ("슬롯별 보존 WAL vs 상한", "timeseries", "bytes",
         [('hotdb_slot_retained_bytes', "{{slot_name}}"), (SLOT_CAP, "상한")], "소비자가 멈춘 슬롯만 계속 오른다"),
        ("디스크 — pg_wal · DB · 여유", "timeseries", "bytes",
         [('max(hotdb_wal_dir_bytes)', "pg_wal"), ('pg_database_size_bytes{job="hotdb", datname="hotdb"}', "DB hotdb"),
          (DISK, "디스크 여유")], ""),
        ("WAL 생성률", "timeseries", "Bps",
         [('hotdb:wal_rate_bytes', "15분 평균"), ('sum(rate(hotdb_wal_written_bytes[1m]))', "1분")],
         "다운 허용 시간 = 남은 여유 / 이 값"),
        ("슬롯별 다운 허용 남은 시간", "timeseries", "s",
         [('hotdb:slot_downtime_budget_seconds', "{{slot_name}}"), ('hotdb:disk_downtime_budget_seconds', "디스크")],
         "1시간 밑으로 5분 이어지면 경보 (CdcDowntimeBudgetLow)"),
    ]),
    ("④ 적체 — 메모리 상한 · backpressure", [
        ("엔진 큐 사용률 최대", "stat", "percentunit",
         [(f'max(hotdb:engine_queue_fill{{{P}}})', "건"), (f'max(hotdb:engine_queue_bytes_fill{{{P}}})', "바이트")],
         "1 에 붙으면 엔진이 슬롯 읽기를 멈춘 것 — backpressure 동작"),
        ("backpressure 중인 파이프라인", "stat", "short",
         [(f'count(hotdb:engine_queue_fill{{{P}}} > 0.9 or hotdb:engine_queue_bytes_fill{{{P}}} > 0.9) or vector(0)', "")], ""),
        ("힙 사용률 최대", "stat", "percentunit",
         [(f'max({HEAP_USED} / {HEAP_MAX})', "")], "적체 중에도 이게 평평하면 메모리 상한이 지켜지는 것"),
        ("엔진 큐 (건)", "timeseries", "short",
         [(f'hotdb_cdc_engine_queue{{{P}, state="used"}}', "{{pipeline}} 사용"),
          (f'max(hotdb_cdc_engine_queue{{{P}, state="capacity"}})', "상한 (max.queue.size)")], ""),
        ("엔진 큐 (바이트)", "timeseries", "bytes",
         [(f'hotdb_cdc_engine_queue_bytes{{{P}, state="used"}}', "{{pipeline}} 사용"),
          (f'max by (pipeline) (hotdb_cdc_engine_queue_bytes{{{P}, state="capacity"}})', "{{pipeline}} 상한 (max.queue.size.in.bytes)")],
         "상한 0 = 건수로만 막는다"),
        ("backpressure 증거 — 큐가 차면 밀림은 원천 슬롯으로", "timeseries", "bytes",
         [('hotdb_slot_confirmed_lag_bytes', "{{slot_name}} 미확인 WAL")],
         "큐 사용률이 1 에 붙어 있는 동안 이 값은 오르고 힙은 평평해야 한다 — 밀린 것이 프로세스가 아니라 원천에 쌓인다는 뜻"),
        ("힙 사용 vs 상한", "timeseries", "bytes",
         [(HEAP_USED, "{{pipeline}} 사용"), (HEAP_MAX, "{{pipeline}} 상한")], ""),
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


def build(rows):
    panels, pid, y = [], 1, 0
    for row_title, items in rows:
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
    return panels


def write(uid, title, tags, rows, **extra):
    panels = build(rows)
    dash = {
        "uid": uid, "title": title, "tags": tags, "timezone": "browser",
        "schemaVersion": 39, "version": 1, "refresh": "5s",
        "time": {"from": "now-30m", "to": "now"}, "panels": panels, **extra,
    }
    out = Path(__file__).resolve().parent / "dashboards" / f"{uid}.json"
    out.write_text(json.dumps(dash, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{out.name}: 패널 {len(panels)}")


def main():
    write("hotdb-cdc", "HotDB CDC", ["hotdb", "cdc"], ROWS)
    write("hotdb-engine", "HotDB 엔진 자원", ["hotdb", "cdc", "engine"], ENGINE_ROWS,
          templating={"list": [{
              "name": "pipeline", "label": "파이프라인", "type": "query", "datasource": DS,
              "query": {"query": "label_values(hotdb_cdc_state, pipeline)", "refId": "pipeline"},
              "definition": "label_values(hotdb_cdc_state, pipeline)",
              "includeAll": True, "multi": True, "allValue": ".*", "refresh": 2,
              "current": {"text": "All", "value": "$__all"}}]},
          # 측정 스크립트(scripts/engine-resource.py)가 구간마다 남기는 주석 — 평시 · 1만 건 배치 · 적체가 띠로 보인다
          annotations={"list": [{
              "name": "측정 구간", "enable": True, "iconColor": "orange",
              "datasource": {"type": "grafana", "uid": "-- Grafana --"},
              "target": {"type": "tags", "tags": ["engine-resource"], "matchAny": True, "limit": 200}}]})


if __name__ == "__main__":
    main()
