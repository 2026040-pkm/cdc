#!/usr/bin/env python3
"""
비교 대시보드 JSON 생성기 → infra/monitoring/grafana/dashboards/lidar-compare.json

패널을 손으로 JSON 에 늘어놓으면 경로마다 쿼리가 한쪽만 고쳐지는 일이 생긴다. 여기서는 경로 목록
(PIPELINES)을 한 번만 적고, 경로마다 같은 쿼리를 찍어 낸다 — 비교는 "같은 질문을 경로 수만큼" 이어야 한다.

    python scripts/build-dashboard.py
Grafana 가 10초마다 파일을 다시 읽으므로 재기동은 필요 없다.
"""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "infra" / "monitoring" / "grafana" / "dashboards" / "lidar-compare.json"

PROM = {"type": "prometheus", "uid": "cmp-prom"}
MIXED = {"type": "datasource", "uid": "-- Mixed --"}
# (Prometheus pipeline 레이블, 화면 이름, hot DB 데이터소스, 색)
PIPELINES = [
    ("isl-kafka", "A · ISL Engine→Kafka", {"type": "grafana-postgresql-datasource", "uid": "cmp-db-kafka"}, "#F2994A"),
    ("mqtt-direct", "B · MQTT 직결 서비스", {"type": "grafana-postgresql-datasource", "uid": "cmp-db-mqtt"}, "#2F80ED"),
    ("isl-db", "C · ISL Engine→Db Provider", {"type": "grafana-postgresql-datasource", "uid": "cmp-db-isldb"}, "#27AE60"),
]

panels = []
_id = [0]
_y = [0]


def nid():
    _id[0] += 1
    return _id[0]


def row(title):
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": _y[0]}, "panels": []})
    _y[0] += 1


def color_overrides():
    """시리즈 이름(= 경로 화면 이름으로 시작) 마다 같은 색을 모든 패널에 건다."""
    out = []
    for _key, name, _ds, color in PIPELINES:
        out.append({"matcher": {"id": "byRegexp", "options": f"^{name}.*"},
                    "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]})
    return out


def ts(title, targets, x, w, h=8, unit="short", desc="", ds=PROM, stack=False, decimals=None):
    p = {
        "type": "timeseries", "title": title, "id": nid(), "datasource": ds, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": _y[0]},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "lineWidth": 2, "fillOpacity": 10 if not stack else 40, "showPoints": "never",
            "stacking": {"mode": "normal" if stack else "none"}}},
            "overrides": color_overrides()},
        "options": {"legend": {"displayMode": "table", "placement": "bottom",
                               "calcs": ["lastNotNull", "mean", "max"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": targets,
    }
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    panels.append(p)
    return p


def stat(title, targets, x, w, h=4, unit="short", desc="", ds=PROM, decimals=None, thresholds=None):
    p = {
        "type": "stat", "title": title, "id": nid(), "datasource": ds, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": _y[0]},
        "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "fixed", "fixedColor": "text"},
                                     "thresholds": thresholds or {"mode": "absolute", "steps": [{"color": "text", "value": None}]}},
                        "overrides": color_overrides()},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "area", "textMode": "value_and_name",
                    "justifyMode": "center", "orientation": "vertical"},
        "targets": targets,
    }
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    panels.append(p)
    return p


def table(title, targets, x, w, h=8, desc="", ds=MIXED, transformations=None, overrides=None):
    panels.append({
        "type": "table", "title": title, "id": nid(), "datasource": ds, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": _y[0]},
        "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": overrides or []},
        "options": {"showHeader": True, "cellHeight": "sm"},
        "targets": targets,
        "transformations": transformations or [],
    })


def prom(expr, legend, ref="A", instant=False):
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": ref,
            "instant": instant, "range": not instant}


def per_pipeline(expr_tpl, legend_suffix=""):
    """{p} 자리에 경로 레이블을 넣어 경로마다 쿼리 하나씩."""
    out = []
    for i, (key, name, _ds, _c) in enumerate(PIPELINES):
        out.append(prom(expr_tpl.format(p=key), f"{name}{legend_suffix}", chr(65 + i)))
    return out


def sql(ds, raw, ref, fmt="time_series"):
    return {"datasource": ds, "rawSql": raw, "format": fmt, "refId": ref, "editorMode": "code", "rawQuery": True}


def per_db(sql_tpl, fmt="time_series"):
    """경로마다 자기 hot DB 에 같은 SQL. {name} 은 열/시리즈 이름."""
    return [sql(ds, sql_tpl.format(name=name), chr(65 + i), fmt) for i, (_k, name, ds, _c) in enumerate(PIPELINES)]


def advance(h):
    _y[0] += h


# ─────────────────────────────────────────────────────────────────────────────
row("한눈에 — 같은 MQTT 입력, 세 경로의 결과")
w = 4
stat("적재 행/초 (1분 평균)", per_pipeline('sum(cmp:rows_inserted:rate1m{{pipeline="{p}"}})'), 0, w, unit="short", decimals=0,
     desc="DB 에 새로 들어간 행/초. 입력은 발행기 ~426 msg/s(상태 350 + 실적 5.8 + 산출물 70). A 는 EES Kafka Provider 가 태그당 최신값만 보내 산출물이 합쳐진다.")
stat("end-to-end p95", per_pipeline('cmp:e2e_seconds:p95{{pipeline="{p}"}}'), 4, w, unit="s", decimals=2,
     desc="장비 occurred_at → DB 커밋. A 는 Agent 가 occurred_at 으로 정한 content.timestamp, B 는 메시지의 occurred_at, C 는 Engine 태그 변경의 changedAt — 같은 값이다.")
stat("대기 레코드 (소스 lag)", per_pipeline('max(lidar_ingest_source_lag{{pipeline="{p}"}})'), 8, w, decimals=0,
     desc="A = Kafka 파티션 lag 합(레코드 · 레코드 하나에 항목 수백 개), B = 받았지만 아직 DB 에 안 쓴 MQTT 메시지 수(메시지 하나 = 항목 하나), C = Db.Provider 큐에 쌓인 태그 변경 수. 알갱이가 달라 절대값보다 추세를 본다.")
stat("최근 1분 장비 수 (status)", per_db(
    "SELECT count(DISTINCT tid) AS \"{name}\" FROM tsdb.lidar_status WHERE time > now() - interval '1 minute'", "table"),
     12, w, ds=MIXED, decimals=0,
     desc="hot DB 를 직접 센다. 350 이어야 한다. A 는 태그 카탈로그로 장비를 푸는데 못 풀면 '#숫자' 로 남아 수가 달라진다.")
stat("미해석 장비 축 (#숫자)", per_db(
    "SELECT count(DISTINCT tid) AS \"{name}\" FROM tsdb.lidar_status WHERE time > now() - interval '5 minutes' AND tid LIKE '#%'", "table"),
     16, w, ds=MIXED, decimals=0,
     thresholds={"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]},
     desc="장비 id 를 못 찾아 숫자 ParameterId 로 적재된 수. B 는 MQTT 메시지의 id 를 그대로 써서 구조적으로 0 이다.")
stat("격리(reject)/분", per_pipeline('sum(rate(lidar_ingest_rejects_total{{pipeline="{p}"}}[5m])) * 60 or vector(0)'), 20, w, decimals=1,
     thresholds={"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 0.1}]})
advance(4)

# ─────────────────────────────────────────────────────────────────────────────
row("처리량")
ts("적재 행/초", per_pipeline('cmp:rows_inserted:rate1m{{pipeline="{p}"}}'), 0, 12, unit="short",
   desc="rows_inserted 는 멱등 인덱스에 걸린 중복을 뺀 값이다.")
ts("채널별 수신 항목/초", [prom('cmp:items:rate1m{pipeline="%s"}' % key, f"{name} · {{{{channel}}}}", chr(65 + i))
                         for i, (key, name, _d, _c) in enumerate(PIPELINES)], 12, 12, unit="short")
advance(8)
ts("분당 적재 행 (hot DB 직접 · 이벤트 시각 기준)", per_db(
    "SELECT time_bucket('1 minute', time) AS time, count(*) AS \"{name}\" FROM tsdb.lidar_status "
    "WHERE $__timeFilter(time) GROUP BY 1 ORDER BY 1"), 0, 12, ds=MIXED,
   desc="status 하이퍼테이블 행을 occurred_at 분 단위로 센다. 정상이면 선이 350×60=21,000 에서 겹친다. 마지막 분은 아직 차는 중이다.")
ts("분당 적재 행 — 실적·산출물 (hot DB 직접)", [
    sql(ds, "SELECT time_bucket('1 minute', time) AS time, count(*) AS \"%s · actual\" FROM tsdb.lidar_scan_actual "
            "WHERE $__timeFilter(time) GROUP BY 1 ORDER BY 1" % name, chr(65 + 2 * i))
    for i, (_k, name, ds, _c) in enumerate(PIPELINES)] + [
    sql(ds, "SELECT time_bucket('1 minute', time) AS time, count(*) AS \"%s · artifact\" FROM tsdb.lidar_scan_artifact "
            "WHERE $__timeFilter(time) GROUP BY 1 ORDER BY 1" % name, chr(66 + 2 * i))
    for i, (_k, name, ds, _c) in enumerate(PIPELINES)], 12, 12, ds=MIXED,
   desc="1분 주기 채널. 기대값 actual 350/분, artifact 4,200/분.")
advance(8)
table("표별 행 수 (hot DB 직접)", per_db(
    "SELECT '{name}' AS \"경로\", "
    "(SELECT count(*) FROM tsdb.lidar_status WHERE time > now() - interval '5 minutes') AS \"status 5분\", "
    "(SELECT count(*) FROM tsdb.lidar_scan_actual WHERE time > now() - interval '5 minutes') AS \"actual 5분\", "
    "(SELECT count(*) FROM tsdb.lidar_scan_artifact WHERE time > now() - interval '5 minutes') AS \"artifact 5분\", "
    "approximate_row_count('tsdb.lidar_status') AS \"status 누적(근사)\", "
    "(SELECT count(*) FROM rdb.lidar_device_state) AS \"장비 상태 행\", "
    "(SELECT count(*) FROM rdb.lidar_ingest_reject) AS \"격리 누적\", "
    "(SELECT count(*) FROM rdb.lidar_status_message) AS \"수신 레코드 누적\"", "table"),
      0, 24, h=5, transformations=[{"id": "merge", "options": {}}],
      desc="같은 SQL 을 세 hot DB 에 던진 결과. 5분 창은 이벤트 시각(time) 기준이라 늦게 도착한 항목까지 센다. "
           "'수신 레코드' 는 A = Kafka 레코드, B = 소비자가 1초마다 묶은 MQTT 메시지 묶음, C = Db.Provider 가 1초마다 묶은 태그 변경 묶음이라 알갱이가 다르다.")
advance(5)

# ─────────────────────────────────────────────────────────────────────────────
row("지연")
ts("end-to-end 지연 (occurred_at → DB 커밋)",
   [prom('cmp:e2e_seconds:p50{pipeline="%s"}' % key, f"{name} p50", chr(65 + 3 * i))
    for i, (key, name, _d, _c) in enumerate(PIPELINES)] +
   [prom('cmp:e2e_seconds:p95{pipeline="%s"}' % key, f"{name} p95", chr(66 + 3 * i))
    for i, (key, name, _d, _c) in enumerate(PIPELINES)] +
   [prom('cmp:e2e_seconds:p99{pipeline="%s"}' % key, f"{name} p99", chr(67 + 3 * i))
    for i, (key, name, _d, _c) in enumerate(PIPELINES)], 0, 12, unit="s",
   desc="히스토그램 버킷(0.1 · 0.25 · 0.5 · 1 · 2 · 5 · 10 · 30 …) 경계라 분위수는 버킷 사이 선형 보간값이다. 정밀 비교는 오른쪽 DB 직접 값을 본다.")
ts("적재 지연 평균 (hot DB 직접 · received_at − time)", per_db(
    "SELECT time_bucket('1 minute', time) AS time, avg(extract(epoch FROM received_at - time)) AS \"{name}\" "
    "FROM tsdb.lidar_status WHERE $__timeFilter(time) GROUP BY 1 ORDER BY 1"), 12, 12, ds=MIXED, unit="s",
   desc="행마다 DB 기본값 now()(트랜잭션 시작 시각)와 장비 occurred_at 의 차. 히스토그램 보간이 없는 실측 평균.")
advance(8)
ts("소스 대기 (lag)", per_pipeline('max(lidar_ingest_source_lag{{pipeline="{p}"}})'), 0, 8, unit="short")
ts("DB 배치 시간 p95", per_pipeline('cmp:batch_seconds:p95{{pipeline="{p}"}}'), 8, 8, unit="s")
ts("배치당 행 수 (평균)", per_pipeline('cmp:rows_per_batch:avg{{pipeline="{p}"}}'), 16, 8, unit="short")
advance(8)

# ─────────────────────────────────────────────────────────────────────────────
row("정합성 · 품질")
ts("중복 스킵/초", per_pipeline('sum(rate(lidar_ingest_rows_duplicate_total{{pipeline="{p}"}}[1m]))'), 0, 8,
   desc="멱등 인덱스가 거른 행. MQTT QoS1 재전달·소비자 재시작 뒤 재전달이 여기 보인다. 유실이 아니다.")
ts("격리/분 (사유별)", [prom('sum by (reason) (rate(lidar_ingest_rejects_total{pipeline="%s"}[5m])) * 60' % key,
                         f"{name} · {{{{reason}}}}", chr(65 + i)) for i, (key, name, _d, _c) in enumerate(PIPELINES)] +
   # 격리가 한 번도 없으면 카운터 자체가 없어 패널이 빈다. 0 선을 같이 그려 "없음" 을 보이게 한다.
   [prom('sum(rate(lidar_ingest_rejects_total{pipeline="%s"}[5m])) * 60 or vector(0)' % key,
         f"{name} · 합계", chr(65 + len(PIPELINES) + i)) for i, (key, name, _d, _c) in enumerate(PIPELINES)], 8, 8)
ts("장비 축 미해석/분 (A 전용)", [prom('sum(rate(lidar_ingest_tag_unresolved_total{pipeline="isl-kafka"}[5m])) * 60 or vector(0)',
                               f"{PIPELINES[0][1]}", "A")], 16, 8,
   desc="Kafka content.tid(숫자)가 lidar_tag_catalog 에 없어 장비를 못 푼 항목. B 경로에는 이 단계 자체가 없다.")
advance(8)
table("장비별 최근 상태 차이 (최근 1분 status 건수)", [
    sql(PIPELINES[0][2], "SELECT tid AS \"장비\", count(*) AS \"A 건수\" FROM tsdb.lidar_status "
                         "WHERE time > now() - interval '1 minute' GROUP BY 1", "A", "table"),
    sql(PIPELINES[1][2], "SELECT tid AS \"장비\", count(*) AS \"B 건수\" FROM tsdb.lidar_status "
                         "WHERE time > now() - interval '1 minute' GROUP BY 1", "B", "table"),
    sql(PIPELINES[2][2], "SELECT tid AS \"장비\", count(*) AS \"C 건수\" FROM tsdb.lidar_status "
                         "WHERE time > now() - interval '1 minute' GROUP BY 1", "C", "table"),
], 0, 24, h=8,
      transformations=[
          {"id": "merge", "options": {}},
          {"id": "calculateField", "options": {"mode": "binary", "alias": "차이 (A−B)",
                                               "binary": {"left": "A 건수", "operator": "-", "right": "B 건수"},
                                               "replaceFields": False}},
          {"id": "calculateField", "options": {"mode": "binary", "alias": "차이 (C−B)",
                                               "binary": {"left": "C 건수", "operator": "-", "right": "B 건수"},
                                               "replaceFields": False}},
          {"id": "sortBy", "options": {"sort": [{"field": "차이 (A−B)", "desc": True}]}},
      ],
      desc="세 DB 를 장비로 맞춰 본다. B(MQTT 직결)를 기준으로 A·C 의 차이를 낸다. A−B 가 큰 장비가 위로 온다. 빈칸은 한쪽에만 있는 장비다(A 의 '#숫자' 등).")
advance(8)

# ─────────────────────────────────────────────────────────────────────────────
row("리소스 — 경로마다 무엇이 돌고 있나")
ts("ISL 호스트 프로세스 CPU (코어)", [prom('cmp:isl_process_cpu_cores', "{{process}}", "A")], 0, 12, unit="short", decimals=2,
   desc="Mqtt.Agent · Engine(primary+secondary) 는 A·C 가 같이 쓴다. EES.Kafka.Provider(3개)는 A 전용, Db.Provider 는 C 전용이다. B 는 ISL 을 쓰지 않는다.")
ts("ISL 호스트 프로세스 메모리", [prom('cmp:isl_process_mem_bytes', "{{process}}", "A")], 12, 12, unit="bytes")
advance(8)
A_CTR = 'name=~"tsdb-lidar-ingest|tsdb-lidar-pg|kafka-[a-z0-9]+|nats-(primary|secondary)-[a-z0-9]+"'
B_CTR = 'name=~"cmp-lidar-direct-ingest|cmp-direct-pg"'
C_CTR = 'name=~"cmp-provider-pg"'
ts("컨테이너 CPU (코어)", [
    prom('cmp:container_cpu_cores{%s}' % A_CTR, PIPELINES[0][1] + " · {{name}}", "A"),
    prom('cmp:container_cpu_cores{%s}' % B_CTR, PIPELINES[1][1] + " · {{name}}", "B"),
    prom('cmp:container_cpu_cores{%s}' % C_CTR, PIPELINES[2][1] + " · {{name}}", "C"),
    prom('cmp:container_cpu_cores{name="cmp-emqx"}', "공통 · {{name}}", "D"),
], 0, 12, unit="short", decimals=2,
   desc="A: 소비자 · hot DB · Kafka · NATS(Engine↔Provider). B: 소비자 · hot DB. C: hot DB(행으로 펼치는 적재 함수가 DB 안에서 돈다). NATS 는 A·C 공통이라 A 쪽에만 그린다. EMQX 는 세 경로 공통 브로커다.")
ts("컨테이너 메모리", [
    prom('cmp:container_mem_bytes{%s}' % A_CTR, PIPELINES[0][1] + " · {{name}}", "A"),
    prom('cmp:container_mem_bytes{%s}' % B_CTR, PIPELINES[1][1] + " · {{name}}", "B"),
    prom('cmp:container_mem_bytes{%s}' % C_CTR, PIPELINES[2][1] + " · {{name}}", "C"),
    prom('cmp:container_mem_bytes{name="cmp-emqx"}', "공통 · {{name}}", "D"),
], 12, 12, unit="bytes")
advance(8)
ts("경로 전용 CPU 합계 (코어)", [
    prom('sum(cmp:isl_process_cpu_cores{process=~"Mqtt.Agent|Engine|EES.Kafka.Provider"}) + sum(cmp:container_cpu_cores{%s})' % A_CTR,
         PIPELINES[0][1] + " (Agent+Engine+Provider+NATS+Kafka+소비자+DB)", "A"),
    prom('sum(cmp:container_cpu_cores{%s})' % B_CTR, PIPELINES[1][1] + " (소비자+DB)", "B"),
    prom('sum(cmp:isl_process_cpu_cores{process=~"Mqtt.Agent|Engine|Db.Provider"}) + '
         'sum(cmp:container_cpu_cores{name=~"cmp-provider-pg|nats-(primary|secondary)-[a-z0-9]+"})',
         PIPELINES[2][1] + " (Agent+Engine+Provider+NATS+DB)", "C"),
], 0, 12, unit="short", decimals=2,
   desc="경로를 혼자 돌린다고 칠 때 필요한 구성요소의 CPU 합. Agent · Engine · NATS 는 A·C 가 실제로는 같이 쓰므로 두 선에 모두 들어간다. 공통인 EMQX · 발행기는 빠져 있다.")
ts("hot DB 크기", per_pipeline('sum(tsdb_db_size_bytes{{pipeline="{p}"}}) or sum(pg_database_size_bytes{{pipeline="{p}",datname="lidar"}})'),
   12, 12, unit="bytes")
advance(8)

dashboard = {
    "uid": "lidar-compare",
    "title": "LiDAR 적재 경로 비교 — ISL Kafka vs MQTT 직결 vs ISL Db Provider",
    "tags": ["lidar", "compare"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "10s",
    "time": {"from": "now-30m", "to": "now"},
    "editable": True,
    "graphTooltip": 1,
    "description": "같은 MQTT 입력(lidar-sim 350대)을 A(Agent→Engine→EES Kafka Provider→Kafka→소비자), "
                   "B(EMQX 를 서비스가 직접 구독), C(Agent→Engine→Db.Provider→DB 함수) 세 경로로 각자의 hot DB 에 적재한다. "
                   "세 경로 모두 같은 표·같은 행 규칙·같은 지표 이름이다.",
    "panels": panels,
}
OUT.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"{OUT} - 패널 {len([p for p in panels if p['type'] != 'row'])}개")
