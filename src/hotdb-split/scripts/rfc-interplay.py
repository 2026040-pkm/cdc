#!/usr/bin/env python3
"""운영판(src/hotdb) 실측 — ① RFC Service 안에서 엔진 · RFC 송신 · 레거시 폴링이 자원을 다툴 때  ② 운영 설정(바이트 상한)의 적체 backpressure

    python scripts/rfc-interplay.py        # 약 40분. 결과: app/build/rfc-interplay/<시각>/samples.csv · phases.json

구간: 평시(현장 x1 · 레거시 x1) → 폴링 x5 → 현장 x3 + 폴링 x5 → 같은 부하에 RFC Service 1코어 → 2코어
      → 조립 판별 모듈(hotdb-zone-asm) 5분 정지(현장 x5) 뒤 따라잡기
"""
import csv, json, re, subprocess, sys, threading, time, urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIELD, LEGACY = "http://localhost:59480", "http://localhost:59493"
RFC, ZONE = ("hotdb-rfc-service", "http://localhost:59485"), ("hotdb-zone-asm", "http://localhost:59481")
M = [  # (열, 지표, 라벨 조건)
    ("cpu", "hotdb_cdc_engine_cpu_seconds_total", 'part="process"'),
    ("cpu_engine", "hotdb_cdc_engine_cpu_seconds_total", 'part="engine"'),
    ("cpu_handler", "hotdb_cdc_engine_cpu_seconds_total", 'part="handler"'),
    ("cpu_poll", "hotdb_work_cpu_seconds_total", 'part="poll"'),
    ("cpu_judge", "hotdb_work_cpu_seconds_total", 'part="judge"'),
    ("heap", "jvm_memory_used_bytes", 'area="heap"'),
    ("heap_live", "jvm_gc_live_data_size_bytes", ""),
    ("gc_s", "jvm_gc_pause_seconds_sum", ""),
    ("threads", "jvm_threads_live_threads", ""),
    ("queue", "hotdb_cdc_engine_queue", 'state="used"'),
    ("queue_bytes", "hotdb_cdc_engine_queue_bytes", 'state="used"'),
    ("events", "hotdb_cdc_events_total", ""),
    ("poll_s", "hotdb_poll_run_seconds_sum", ""),
    ("poll_n", "hotdb_poll_run_seconds_count", ""),
    ("poll_rows", "hotdb_poll_rows_total", ""),
    ("sent", "hotdb_rfc_sent_total", ""),
]


def sh(*a, check=True):
    r = subprocess.run(a, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and r.returncode:
        sys.exit(f"실패: {' '.join(a)}\n{r.stderr}")
    return r.stdout.strip()


def sql(q):
    return sh("podman", "exec", "hotdb-pg", "psql", "-U", "postgres", "-d", "hotdb", "-Atc", q)


def http(url, method="GET"):
    with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=3) as r:
        return r.read().decode()


def scrape(url):
    try:
        t = http(f"{url}/actuator/prometheus")
    except Exception:
        return {}
    out = {}
    for name, metric, label in M:
        tot, hit = 0.0, False
        for m in re.finditer(rf"^{metric}(\{{[^}}]*\}})? (\S+)$", t, re.M):
            if label and label not in (m.group(1) or ""):
                continue
            v = float(m.group(2))
            if v == v:
                tot, hit = tot + v, True
        if hit:
            out[name] = tot
    return out


def log(m):
    print(f"[{datetime.now():%H:%M:%S}] {m}", flush=True)


def speed(url, v, run=True):
    http(f"{url}/sim/speed?value={v}", "POST")
    http(f"{url}/sim/{'resume' if run else 'pause'}", "POST")


out = ROOT / "app" / "build" / "rfc-interplay" / datetime.now().strftime("%Y%m%d-%H%M%S")
out.mkdir(parents=True)
rows, phase, stop = [], ["-"], threading.Event()


def loop():
    while not stop.is_set():
        t = time.time()
        r = {"t": round(t, 2), "phase": phase[0]}
        for k, (_, url) in (("rfc", RFC), ("zone", ZONE)):
            for n, v in scrape(url).items():
                r[f"{k}_{n}"] = v
        try:
            r["zone_slot_mb"] = float(sql("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)/1e6 FROM pg_replication_slots WHERE slot_name='zone_asm'") or 0)
        except Exception:
            pass
        rows.append(r)
        stop.wait(max(0, 1 - (time.time() - t)))


threading.Thread(target=loop, daemon=True).start()
ev = []


def rec(name, sec):
    phase[0] = name
    t0 = time.time()
    log(f"{name} — {sec}초")
    time.sleep(sec)
    ev.append({"phase": name, "t0": t0, "t1": time.time()})
    phase[0] = "-"


try:
    speed(FIELD, 1); speed(LEGACY, 1); time.sleep(20)
    rec("평시 (현장 x1 · 폴링 x1)", 300)
    speed(LEGACY, 5); time.sleep(20)
    rec("폴링 x5", 300)
    speed(FIELD, 3); time.sleep(20)
    rec("현장 x3 + 폴링 x5", 300)
    sh("podman", "update", "--cpus", "1", RFC[0]); time.sleep(20)
    rec("RFC 1코어 (현장 x3 + 폴링 x5)", 300)
    sh("podman", "update", "--cpus", "2", RFC[0]); time.sleep(20)
    rec("RFC 2코어 (현장 x3 + 폴링 x5)", 300)
    sh("podman", "update", "--cpus", "12", RFC[0])
    speed(LEGACY, 1); speed(FIELD, 1); time.sleep(20)
    log("운영판 적체 — hotdb-zone-asm 정지, 현장 x5 300초")
    sh("podman", "stop", ZONE[0]); speed(FIELD, 5)
    rec("운영판 적체 쌓기 (zone-asm 정지)", 300)
    speed(FIELD, 1)
    sh("podman", "start", ZONE[0])
    phase[0] = "운영판 적체 따라잡기"; t0 = time.time()
    time.sleep(15)
    while time.time() - t0 < 1500:
        if rows and (rows[-1].get("zone_slot_mb") or 99) < 2:
            break
        time.sleep(1)
    ev.append({"phase": "운영판 적체 따라잡기", "t0": t0, "t1": time.time(), "caught_s": round(time.time() - t0, 1)})
    log(f"따라잡음 {time.time() - t0:.0f}초")
    phase[0] = "-"; time.sleep(30)
finally:
    stop.set(); time.sleep(1.5)
    sh("podman", "update", "--cpus", "12", RFC[0], check=False)
    try:
        speed(FIELD, 1); speed(LEGACY, 1)
    except Exception:
        pass
    sh("podman", "start", ZONE[0], check=False)
    cols = sorted({k for r in rows for k in r}, key=lambda k: (k not in ("t", "phase"), k))
    with open(out / "samples.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, cols); w.writeheader(); w.writerows(rows)
    json.dump(ev, open(out / "events.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    log(f"결과: {out}")
