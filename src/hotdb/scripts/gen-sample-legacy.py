#!/usr/bin/env python3
"""레거시 DB 시험용 표 생성기 → db/legacy-db/V2__sample_tables.sql

    python scripts/gen-sample-legacy.py

이름 · 컬럼 · 값은 전부 지어낸 것이다. 실제 레거시와 맞춘 것은 규모뿐이다 — 스키마별 표 수 · PK 있는 표 수 ·
PK 길이 분포 · 컬럼 합 · 숫자 / 날짜 컬럼 수 (아래 PROFILE, 집계 숫자만). 그래야 폴링 · 모니터링 · 문서가
실제와 비슷한 크기에서 시험된다. 같은 시드로 늘 같은 파일이 나온다.

폴링 작업이 쓰는 7표(POLLED)는 손으로 정한 모양 그대로 넣고, 나머지를 생성해 표 수를 채운다.
"""
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "db" / "legacy-db" / "V2__sample_tables.sql"
JOBS = ROOT / "app" / "db-agent" / "src" / "main" / "resources" / "poll-jobs.yml"   # DB Agent 작업 목록 (Oracle 사본 전부)
FAST = {"lgs.tracking"}   # 자주 바뀌는 표 — 10초 주기

# 스키마 → 규모 (실제 레거시의 집계 숫자만). pk = {PK 컬럼 수: 표 수}
PROFILE = {
    "erp": {"desc": "SAP 사본", "writer": "rfc_agent", "tables": 114, "pk": {0: 29, 2: 23, 3: 23, 4: 18, 5: 9, 6: 6, 7: 2, 9: 2, 10: 1, 11: 1},
            "cols": 4090, "min": 7, "max": 241, "num": 456, "date": 0},
    "mes": {"desc": "Oracle 사본 · 생산", "writer": "db_agent", "tables": 60, "pk": {0: 23, 1: 23, 2: 6, 3: 5, 4: 1, 5: 1, 9: 1},
            "cols": 960, "min": 3, "max": 47, "num": 112, "date": 42},
    "lgs": {"desc": "Oracle 사본 · 물류", "writer": "db_agent", "tables": 3, "pk": {0: 1, 1: 1, 2: 1},
            "cols": 63, "min": 14, "max": 25, "num": 3, "date": 0},
    "geo": {"desc": "Oracle 사본 · 구역 지도", "writer": "db_agent", "tables": 1, "pk": {2: 1},
            "cols": 22, "min": 22, "max": 22, "num": 4, "date": 1},
}

# 폴링 작업 대상 (RFC Service · DB Agent 의 application.yml, legacy-sim.py, 통합 테스트가 이 이름을 쓴다)
POLLED = {
    "erp.item": [("item_no", "text", True), ("item_name", "text"), ("item_group", "text"), ("weight", "numeric"),
                 ("upd_date", "text"), ("upd_time", "text")],
    "erp.part": [("part_no", "text", True), ("item_no", "text"), ("part_name", "text"), ("material", "text"),
                 ("qty", "numeric"), ("upd_date", "text"), ("upd_time", "text")],
    "erp.order_line": [("order_no", "text", True), ("line_no", "text", True), ("part_no", "text"), ("qty", "numeric"),
                       ("due_date", "date"), ("status", "text"), ("upd_date", "text"), ("upd_time", "text")],
    "mes.work_log": [("work_id", "text", True), ("equip_code", "text"), ("work_type", "text"), ("work_date", "date"),
                     ("operator_id", "text"), ("upd_date", "text"), ("upd_time", "text")],
    "mes.alarm": [("alarm_id", "text", True), ("equip_code", "text"), ("alarm_code", "text"), ("message", "text"),
                  ("upd_date", "text"), ("upd_time", "text")],
    "lgs.shipment": [("ship_id", "text", True), ("vehicle_no", "text"), ("origin", "text"), ("destination", "text"),
                     ("status", "text"), ("upd_date", "text"), ("upd_time", "text")],
    "lgs.tracking": [("object_id", "text", True), ("pos_x", "numeric"), ("pos_y", "numeric"), ("area_code", "text"),
                     ("upd_date", "text"), ("upd_time", "text")],
}

NOUN = {
    "erp": ["account", "asset", "batch", "budget", "contract", "cost", "customer", "delivery", "document", "employee",
            "equipment", "invoice", "ledger", "material", "plan", "price", "project", "purchase", "quality", "receipt",
            "route", "schedule", "stock", "supplier", "task", "unit", "vendor", "warehouse", "payment", "voucher"],
    "mes": ["line", "cell", "machine", "tool", "shift", "operator", "recipe", "lot", "process", "inspection",
            "defect", "downtime", "sensor", "station", "fixture", "routing"],
    "lgs": ["carrier", "dock", "yard"],
    "geo": ["zone_area"],
}
SUFFIX = ["master", "detail", "hist", "log", "map", "stat", "req", "rslt", "cfg", "if"]
TXT = ["code", "name", "type", "status", "remark", "grade", "spec", "unit", "grp", "class", "flag", "ref", "owner",
       "area", "seq_no", "dept", "user_id", "note", "kind", "src"]
NUM = ["qty", "amt", "rate", "weight", "length", "cnt", "price", "ratio", "score", "size"]
DT = ["start_dt", "end_dt", "plan_dt", "reg_dt", "due_dt"]


def split(total, n, lo, hi, rnd):
    """합이 total 에 가깝고 [lo, hi] 안인 n 개 — 작은 표가 많고 큰 표가 몇 개인 모양"""
    w = [rnd.lognormvariate(0, 0.9) for _ in range(n)]
    s = sum(w)
    out = [max(lo, min(hi, round(total * x / s))) for x in w]
    for _ in range(10000):           # 합 보정
        d = total - sum(out)
        if d == 0:
            break
        i = rnd.randrange(n)
        if d > 0 and out[i] < hi:
            out[i] += 1
        elif d < 0 and out[i] > lo:
            out[i] -= 1
    return out


def pad(cols, n):
    """폴링 표도 그 스키마의 평균 컬럼 수까지 값 컬럼(attr_NN)을 채운다 — 워터마크 두 컬럼은 맨 뒤 그대로"""
    head, tail = cols[:-2], cols[-2:]
    extra = [(f"attr_{j + 1:02d}", "text") for j in range(max(0, n - len(cols)))]
    return head + extra + tail


def build():
    rnd = random.Random(20261006)
    sql = ["-- [레거시 DB] 시험용 사본 표 — scripts/gen-sample-legacy.py 가 만든다. 손으로 고치지 않는다.",
           "-- 이름 · 컬럼 · 값은 지어낸 것이고, 규모(표 수 · PK · 컬럼 수)만 실제 레거시와 비슷하게 맞췄다.",
           "-- 워터마크 upd_date || upd_time (YYYYMMDD · HHMMSS 문자열). 폴링 대상 7표는 생성기의 POLLED.", ""]
    for s in PROFILE:
        sql.append(f"CREATE SCHEMA IF NOT EXISTS {s};")
    sql.append("")
    stats = {}
    made = []   # (스키마, 표, PK 있음)
    for s, p in PROFILE.items():
        polled = {k.split(".")[1]: pad(v, round(p["cols"] / p["tables"])) for k, v in POLLED.items() if k.startswith(s + ".")}
        n_gen = p["tables"] - len(polled)
        pk_left = dict(p["pk"])
        for cols in polled.values():                 # 폴링 표가 쓴 PK 칸을 뺀다
            k = sum(1 for c in cols if len(c) > 2)
            pk_left[k] = pk_left.get(k, 0) - 1
            if pk_left[k] <= 0:
                pk_left.pop(k)
        pks = [k for k, c in pk_left.items() for _ in range(c)]
        pks += [0] * (n_gen - len(pks))
        rnd.shuffle(pks)
        used = sum(len(c) for c in polled.values())
        sizes = split(p["cols"] - used, n_gen, p["min"], p["max"], rnd) if n_gen else []
        names = [f"{a}_{b}" for a in NOUN[s] for b in SUFFIX] if s != "geo" else NOUN[s]
        rnd.shuffle(names)
        num_left, date_left = p["num"] - sum(1 for c in sum(polled.values(), []) if c[1] == "numeric"), p["date"]
        tables = [(t, cols) for t, cols in polled.items()]
        for i in range(n_gen):
            n, k = sizes[i], pks[i]
            k = min(k, n - 2)
            rest = n - k - 2                                     # 키 · 워터마크 2 를 뺀 값 컬럼
            nn = min(num_left, round(p["num"] / p["tables"] * n / (p["cols"] / p["tables"])), rest)
            nd = min(date_left, round(p["date"] / p["tables"] * n / (p["cols"] / p["tables"])), rest - nn)
            num_left -= nn
            date_left -= nd
            cols = [(f"key_{j + 1}", "text", True) for j in range(k)]
            cols += [(f"{NUM[j % len(NUM)]}_{j // len(NUM) + 1:02d}", "numeric") for j in range(nn)]
            cols += [(f"{DT[j % len(DT)]}_{j // len(DT) + 1:02d}", "date") for j in range(nd)]
            cols += [(f"{TXT[j % len(TXT)]}_{j // len(TXT) + 1:02d}", "text") for j in range(rest - nn - nd)]
            cols += [("upd_date", "text"), ("upd_time", "text")]
            tables.append((names[i], cols))
        tables.sort(key=lambda x: x[0])
        for t, cols in tables:
            body = [f"    {c[0]} {c[1]}" + (" NOT NULL" if len(c) > 2 else "") for c in cols]
            pk = [c[0] for c in cols if len(c) > 2]
            if pk:
                body.append(f"    PRIMARY KEY ({', '.join(pk)})")
            sql.append(f"CREATE TABLE IF NOT EXISTS {s}.{t} (\n" + ",\n".join(body) + "\n);")
            made.append((s, t, bool(pk)))
        stats[s] = (len(tables), sum(1 for _, c in tables if any(len(x) > 2 for x in c)), sum(len(c) for _, c in tables))
        sql.append("")
    erp_w = [s for s, p in PROFILE.items() if p["writer"] == "rfc_agent"]
    ora_w = [s for s, p in PROFILE.items() if p["writer"] == "db_agent"]
    sql += [f"GRANT USAGE ON SCHEMA {', '.join(PROFILE)} TO svc_reader, hotdb_monitor;",
            f"GRANT USAGE ON SCHEMA {', '.join(erp_w)} TO rfc_agent;",
            f"GRANT USAGE ON SCHEMA {', '.join(ora_w)} TO db_agent;",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {', '.join(PROFILE)} TO svc_reader, hotdb_monitor;",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {', '.join(erp_w)} TO rfc_agent;",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {', '.join(ora_w)} TO db_agent;", ""]
    OUT.write_text("\n".join(sql), encoding="utf-8")
    for s, (t, k, c) in stats.items():
        print(f"{s}: 표 {t} · PK 있는 표 {k} · 컬럼 {c}")
    write_jobs([m for m in made if PROFILE[m[0]]["writer"] == "db_agent"])


def write_jobs(made):
    """Oracle 사본 표 전부 → DB Agent 작업. PK 있는 표는 워터마크 증분, 없는 표는 replace(지우고 다시)"""
    counts = " · ".join(f"{s} {sum(1 for m in made if m[0] == s)}" for s in dict.fromkeys(m[0] for m in made))
    out = ["# DB Agent 작업 목록 — scripts/gen-sample-legacy.py 가 만든다. 손으로 고치지 않는다 (application.yml 이 import)",
           "# 원천: Oracle 사용자(스키마)마다 표 — 소유자 이름은 ORACLE_SCHEMA_<스키마> 로 바꾼다",
           "# PK 있는 표: incremental (upd_date || upd_time) · PK 없는 표: replace (매 주기 지우고 다시, 한 트랜잭션)",
           f"# 표 {len(made)}개 = {counts}",
           "hotdb:", "  poll:", "    jobs:"]
    for s, t, pk in made:
        q = f"{s}.{t}"
        out += [f"      - target: {q}", "        source: oracle", f"        from: ${{ORACLE_SCHEMA_{s.upper()}:{s}}}.{t}"]
        out += ["        watermark: upd_date || upd_time"] if pk else ["        mode: replace"]
        if q in FAST:
            out.append("        interval-ms: ${POLL_FAST_INTERVAL_MS:10000}")
    JOBS.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"DB Agent 작업 {len(made)}개 → {JOBS.relative_to(ROOT)} (replace {sum(1 for m in made if not m[2])}개)")


if __name__ == "__main__":
    build()
