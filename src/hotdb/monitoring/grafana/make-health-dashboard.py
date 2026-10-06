#!/usr/bin/env python3
"""HotDB 상태 대시보드 생성기 → dashboards/hotdb-health.json

    python make-health-dashboard.py

"HotDB 에 과부하가 얼마나 걸리나 — CPU 인가 메모리인가 지연인가, CDC 에 이슈가 있나 없나" 를 한 화면에서 답한다.
맨 위 판정은 Prometheus 기록 규칙 hotdb:check · hotdb:health (rules/hotdb.yml 의 hotdb-health 그룹)를 읽는다.
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "hotdb-prom"}
STATE = [  # 0 정상 · 1 주의 · 2 이슈
    {"type": "value", "options": {
        "0": {"text": "정상", "color": "green", "index": 0},
        "1": {"text": "주의", "color": "yellow", "index": 1},
        "2": {"text": "이슈", "color": "red", "index": 2}}}]
STATE_THR = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 1},
                                           {"color": "red", "value": 2}]}

panels = []
_id = [0]


def nid():
    _id[0] += 1
    return _id[0]


def tgt(expr, legend="", instant=False):
    t = {"refId": chr(65 + len(tgt.buf)), "datasource": DS, "expr": expr, "legendFormat": legend}
    if instant:
        t["instant"] = True
        t["range"] = False
    tgt.buf.append(t)
    return t


tgt.buf = []


def targets(*qs, instant=False):
    tgt.buf = []
    return [tgt(q, lg, instant) for q, lg in qs]


def row(title, y):
    panels.append({"id": nid(), "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})


def stat(title, qs, x, y, w, h, unit="short", thr=None, mappings=None, desc="", text="value", orient="auto",
         decimals=None):
    d = {"unit": unit, "thresholds": thr or {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
         "mappings": mappings or [], "color": {"mode": "thresholds"}}
    if decimals is not None:
        d["decimals"] = decimals
    panels.append({"id": nid(), "type": "stat", "title": title, "description": desc, "datasource": DS,
                   "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets(*qs, instant=True),
                   "fieldConfig": {"defaults": d, "overrides": []},
                   "options": {"reduceOptions": {"calcs": ["lastNotNull"], "values": False},
                               "colorMode": "background", "graphMode": "none", "textMode": text,
                               "orientation": orient, "justifyMode": "center", "wideLayout": True}})


def series(title, qs, x, y, w, h, unit="short", desc="", thr=None, maxv=None):
    d = {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never",
                                  "thresholdsStyle": {"mode": "dashed" if thr else "off"}}}
    if thr:
        d["thresholds"] = thr
    if maxv is not None:
        d["max"] = maxv
        d["min"] = 0
    panels.append({"id": nid(), "type": "timeseries", "title": title, "description": desc, "datasource": DS,
                   "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets(*qs),
                   "fieldConfig": {"defaults": d, "overrides": []},
                   "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}})


def timeline(title, qs, x, y, w, h, desc=""):
    panels.append({"id": nid(), "type": "state-timeline", "title": title, "description": desc, "datasource": DS,
                   "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets(*qs),
                   "fieldConfig": {"defaults": {"mappings": STATE, "thresholds": STATE_THR,
                                                "color": {"mode": "thresholds"}, "custom": {"fillOpacity": 80}},
                                   "overrides": []},
                   "options": {"showValue": "never", "rowHeight": 0.8, "mergeValues": True,
                               "legend": {"showLegend": False}}})


def text(title, md, x, y, w, h):
    panels.append({"id": nid(), "type": "text", "title": title, "gridPos": {"x": x, "y": y, "w": w, "h": h},
                   "options": {"mode": "markdown", "content": md}})


PCT = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 0.6},
                                     {"color": "red", "value": 0.8}]}
MEM = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 0.8},
                                     {"color": "red", "value": 0.9}]}
LAG = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 2},
                                     {"color": "red", "value": 5}]}
WAL = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 1e9},
                                     {"color": "red", "value": 5e9}]}

# ── 1. 판정 ──────────────────────────────────────────────────────────────
y = 0
row("① 판정 — 지금 문제가 있나", y)
y += 1
stat("CDC · HotDB 전체", [("hotdb:health", "")], 0, y, 6, 7, mappings=STATE, thr=STATE_THR,
     desc="아래 13개 점검 중 가장 나쁜 값. '정상' 이면 CDC · HotDB 에 이슈가 없다.")
stat("점검 항목 (13)", [("hotdb:check", "{{check}}")], 6, y, 18, 7, mappings=STATE, thr=STATE_THR,
     text="value_and_name", orient="auto",
     desc="0 정상 · 1 주의 · 2 이슈. 임계값은 rules/hotdb.yml 의 hotdb-health 그룹.")
y += 7
timeline("지난 판정 이력 — 언제 어떤 항목이 나빴나", [("hotdb:check", "{{check}}")], 0, y, 24, 9,
         desc="빨강 · 노랑 띠가 있는 구간이 문제가 있던 시간. 아래 패널의 같은 시간대를 보면 원인이 보인다.")
y += 9

# ── 2. 부하 ──────────────────────────────────────────────────────────────
row("② HotDB 부하 — 무엇이 오르나", y)
y += 1
stat("HotDB CPU", [("max(hotdb:pg_cpu_ratio)", "")], 0, y, 4, 4, unit="percentunit", thr=PCT, decimals=1,
     desc="hotdb-pg 컨테이너 CPU / 서버 코어 수. 60% 주의 · 80% 이슈")
stat("서버 메모리 사용", [("max(hotdb:host_mem_ratio)", "")], 4, y, 4, 4, unit="percentunit", thr=MEM, decimals=1,
     desc="HotDB 서버(로컬은 podman VM) 의 사용 중 메모리. 80% 주의 · 90% 이슈")
stat("CDC 반영 지연 p99", [("max(hotdb_zone_apply_lag_p99_seconds)", "")], 8, y, 4, 4, unit="s", thr=LAG, decimals=2,
     desc="tsdb 수신 → 모듈 RDB 반영 (DB 시계). 2초 주의 · 5초 이슈")
stat("슬롯이 붙잡은 WAL", [("max(hotdb_slot_retained_bytes)", "")], 12, y, 4, 4, unit="bytes", thr=WAL,
     desc="가장 뒤처진 CDC 소비자 때문에 지우지 못한 WAL. 20GB 에서 슬롯 무효화")
stat("WAL 생성", [("sum(rate(hotdb_wal_written_bytes[1m]))", "")], 16, y, 4, 4, unit="Bps",
     desc="HotDB 가 쓰는 양. 적재량 · UPSERT 에 비례 — CDC 소비자가 읽어야 할 양")
stat("tsdb 적재", [("sum(rate(hotdb_hypertable_inserted_rows[1m]))", "")], 20, y, 4, 4, unit="short", decimals=0,
     desc="초당 들어오는 필드 데이터 행 (×1 ≈ 426)")
y += 4
series("CPU — 누가 쓰나 (코어)", [
    ('sum(hotdb:container_cpu_cores{name="hotdb-pg"})', "HotDB (PostgreSQL)"),
    ('sum(hotdb:container_cpu_cores{name=~"hotdb-zone-.*"})', "판별 모듈 합"),
    ('sum(hotdb:container_cpu_cores{name="hotdb-rfc-provider"})', "RFC Service"),
    ('sum(hotdb:container_cpu_cores{name="hotdb-db-agent"})', "DB Agent")], 0, y, 12, 8, unit="short",
    desc="HotDB 만 오르면 DB 쪽(디코딩 · 쓰기), 모듈만 오르면 소비자 쪽(해석 · 반영) 부하")
series("메모리 — 누가 쓰나", [
    ('sum(hotdb:container_mem_bytes{name="hotdb-pg"})', "HotDB (캐시 포함)"),
    ('sum(hotdb:container_mem_bytes{name=~"hotdb-zone-.*"})', "판별 모듈 합"),
    ('sum(hotdb:container_mem_bytes{name=~"hotdb-(rfc-provider|db-agent)"})', "RFC · DB Agent"),
    ("max(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)", "서버 사용 중")], 12, y, 12, 8,
    unit="bytes", desc="PostgreSQL 은 페이지 캐시까지 잡혀 높게 보인다 — 계속 오르기만 하는지가 중요")
y += 8
series("지연 — 어디서 늦나 (초)", [
    ("max by (zone) (hotdb_zone_apply_lag_p99_seconds)", "1단 tsdb → 모듈 {{zone}}"),
    ("max(hotdb_rfc_lag_hop2_p99_seconds)", "2단 모듈 → SAP"),
    ("max(hotdb_heartbeat_age_seconds)", "heartbeat 경과 (참고)")], 0, y, 12, 8, unit="s", thr=LAG,
    desc="1단만 늦으면 판별 모듈, 2단만 늦으면 RFC Service · SAP 쪽")
series("CDC 뒤처짐 — 슬롯별 미확인 WAL", [
    ("hotdb_slot_confirmed_lag_bytes", "{{slot_name}}"),
    ("hotdb_walsender_sent_lag_bytes", "{{slot_name}} 미전송")], 12, y, 12, 8, unit="bytes",
    desc="평소엔 0 근처에서 톱니. 한 슬롯만 계속 오르면 그 소비자가 멈췄거나 느리다")
y += 8
series("DB 안 — 접속 · 트랜잭션", [
    ('sum by (state) (pg_stat_activity_count{datname="hotdb"})', "접속 {{state}}"),
    ('rate(pg_stat_database_xact_commit{datname="hotdb"}[1m])', "commit/초"),
    ('rate(pg_stat_database_xact_rollback{datname="hotdb"}[1m])', "rollback/초")], 0, y, 12, 8,
    desc="active 가 계속 늘면 쿼리가 쌓이는 중. rollback 이 늘면 반영 실패 · 재시도")
series("들어오는 양 — 원인 쪽", [
    ("sum by (hypertable) (rate(hotdb_hypertable_inserted_rows[1m]))", "적재 {{hypertable}}"),
    ("sum(rate(hotdb_cdc_route_applied_total[1m]))", "모듈 반영 행/초"),
    ("sum(rate(hotdb_poll_rows_total[5m]))", "레거시 폴링 행/초")], 12, y, 12, 8,
    desc="CPU · 지연이 오를 때 여기도 같이 올랐으면 '양이 늘어서', 그대로면 다른 원인")
y += 8

# ── 2-1. 판별 모듈 4개 — WAL 을 받고 있나 ─────────────────────────────────
CDC_STATE = [{"type": "value", "options": {
    "0": {"text": "STOPPED", "color": "red", "index": 0}, "1": {"text": "STARTING", "color": "yellow", "index": 1},
    "2": {"text": "RUNNING", "color": "green", "index": 2}, "3": {"text": "FAILED", "color": "red", "index": 3},
    "4": {"text": "HALTED", "color": "red", "index": 4}}}]
SLOT_ON = [{"type": "value", "options": {"1": {"text": "연결", "color": "green", "index": 0},
                                         "0": {"text": "끊김", "color": "red", "index": 1}}}]
GREEN = {"mode": "absolute", "steps": [{"color": "green", "value": None}]}
LAGB = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 64e6},
                                      {"color": "red", "value": 1e9}]}
AGE = {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 30},
                                     {"color": "red", "value": 120}]}
ZONE = 'pipeline=~"zone-.*"'
ZSLOT = 'slot_name=~"zone_.*"'
row("②-1 판별 모듈 4개 — 각자 WAL 을 받고 있나", y)
y += 1
stat("모듈 상태", [(f"hotdb_cdc_state{{{ZONE}}}", "{{zone}}")], 0, y, 6, 4, mappings=CDC_STATE, thr=GREEN,
     text="value_and_name", desc="서비스 안 CDC 엔진 상태. RUNNING 이 아니면 그 모듈의 /actuator/health 에 failure")
stat("슬롯 연결", [(f"hotdb_slot_active{{{ZSLOT}}}", "{{slot_name}}")], 6, y, 6, 4, mappings=SLOT_ON, thr=GREEN,
     text="value_and_name", desc="Hot DB 쪽에서 본 것 — 소비자가 슬롯에 붙어 있으면 연결. 끊기면 WAL 이 쌓이기 시작")
stat("미확인 WAL", [(f"hotdb_slot_confirmed_lag_bytes{{{ZSLOT}}}", "{{slot_name}}")], 12, y, 6, 4, unit="bytes",
     thr=LAGB, text="value_and_name", desc="그 모듈이 아직 처리 확인을 안 한 WAL. 64MB 주의 · 1GB 이슈 — 커지면 그 모듈이 밀리는 중")
stat("마지막 배치 이후", [(f"hotdb_cdc_last_batch_age_seconds{{{ZONE}}}", "{{zone}}")], 18, y, 6, 4, unit="s",
     thr=AGE, decimals=0, text="value_and_name", desc="마지막으로 배치를 반영한 지 몇 초. heartbeat 가 10초마다 오므로 평소 0~15초")
y += 4
timeline("모듈 상태 이력 — 언제 멈췄나", [
    (f"(hotdb_cdc_state{{{ZONE}}} == bool 1) + 2 * ((hotdb_cdc_state{{{ZONE}}} != bool 2) * (hotdb_cdc_state{{{ZONE}}} != bool 1))",
     "{{zone}}")], 0, y, 12, 6, desc="초록 RUNNING · 노랑 STARTING · 빨강 STOPPED / FAILED / HALTED (또는 지표 없음 = 서비스 down)")
timeline("슬롯 연결 이력 — 언제 끊겼나", [(f"(1 - hotdb_slot_active{{{ZSLOT}}}) * 2", "{{slot_name}}")], 12, y, 12, 6,
         desc="빨강 구간 = 소비자가 슬롯에서 떨어져 있던 시간. 그동안의 WAL 은 재접속 뒤 이어 받는다")
y += 6
series("수신 이벤트/초 — 모듈별", [(f"sum by (zone) (rate(hotdb_cdc_events_total{{{ZONE}}}[1m]))", "{{zone}}")],
       0, y, 12, 8, desc="네 모듈이 같은 값이어야 정상 — 각자 tsdb 전체 WAL 을 받는다. 하나만 낮으면 그 모듈이 밀리는 중")
series("반영 행/초 — 모듈별 (자기 권역만)", [
    (f"sum by (zone) (rate(hotdb_cdc_route_applied_total{{{ZONE}}}[1m]))", "{{zone}}")], 12, y, 12, 8,
    desc="받은 것 중 자기 권역 것만 쓴다. 장비 수에 비례 (조립 210 · 의장 140 · 도장 30 · 가공 30)")
y += 8
series("미확인 WAL 추이 — 슬롯별", [(f"hotdb_slot_confirmed_lag_bytes{{{ZSLOT}}}", "{{slot_name}}")], 0, y, 12, 8,
       unit="bytes", thr=LAGB, desc="톱니 모양으로 0 근처가 정상. 한 슬롯만 계속 오르면 그 모듈을 본다")
series("반영 지연 p99 — 모듈별 (DB 시계)", [("max by (zone) (hotdb_zone_apply_lag_p99_seconds)", "{{zone}}")],
       12, y, 12, 8, unit="s", thr=LAG, desc="tsdb 수신 → 그 모듈 RDB 반영. 2초 주의 · 5초 이슈")
y += 8

# ── 3. 읽는 법 ───────────────────────────────────────────────────────────
row("③ 읽는 법", y)
y += 1
text("증상 → 원인", """
| 보이는 것 | 뜻 | 볼 곳 |
|---|---|---|
| 판정 **정상**, 그래프 평평 | CDC 이슈 없음 · 여유 있음 | — |
| CPU 오름 + 적재량도 오름, 지연 그대로 | 양이 늘었지만 따라가는 중 | CPU 가 80% 를 넘는 배속이 한계 |
| HotDB CPU 오름 + 슬롯 미확인 WAL 은 0 | WAL 디코딩 부하 (슬롯 수만큼 전체 WAL 을 읽음) | 슬롯 수 · BRD Q2 |
| 지연 오름 + 한 슬롯의 미확인 WAL 증가 | 그 소비자(모듈 · RFC)가 못 따라감 · 멈춤 | 서비스 응답 · 그 서비스 로그 |
| 슬롯 WAL 이 계속 쌓임 + 서비스 down | 소비자 정지 — 살리면 따라잡음 | 20GB 전에 복구 (②-1 에서 어느 모듈인지) |
| dead letter 증가 | 데이터 · 스키마가 안 맞아 반영 실패 | `ops.cdc_dead_letter.error` — 고친 뒤 `status = 'RETRY_REQUESTED'` 로 재처리 |
| CDC 정지 · 캡처 갭 | 소비자가 스스로 멈춤 (표 · 권한 문제, 대부분 실패, 되받을 수 없는 구간) | 그 서비스 `/actuator/health` 의 failure |
| 메모리만 계속 오름 | 캐시라면 정상, JVM 이면 누수 의심 | 컨테이너별 메모리 |
| 레거시 폴링 주의 | SAP · Oracle 접속 또는 표 변경 | `ops.poll_state.last_error` |
""", 0, y, 24, 10)

dash = {"uid": "hotdb-health", "title": "HotDB 상태 — 부하 · CDC 판정", "tags": ["hotdb", "health"],
        "timezone": "browser", "schemaVersion": 39, "version": 1, "refresh": "10s",
        "time": {"from": "now-1h", "to": "now"}, "panels": panels}
out = Path(__file__).resolve().parent / "dashboards" / "hotdb-health.json"
out.write_text(json.dumps(dash, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"{out.name}: 패널 {len(panels)}")
