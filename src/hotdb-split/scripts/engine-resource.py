#!/usr/bin/env python3
"""Embedded 엔진 자원 측정 — 판별 모듈(zone-asm) 프로세스 안에서 Debezium 엔진이 쓰는 CPU · 메모리 · 스레드.

    python scripts/engine-resource.py all        # 기준선 + 세 시나리오 (약 25분)
    python scripts/engine-resource.py idle       # 평시 — 유휴(발행 멈춤) 2분 · 평시 x1 5분
    python scripts/engine-resource.py batch      # 1만 건 배치 — 한 트랜잭션 10,000행을 3번
    python scripts/engine-resource.py backlog    # 적체 — 모듈을 멈추고 x5 로 3분 쌓은 뒤 다시 띄워 따라잡기

결과: app/build/engine-resource/<시각>/samples.csv (1초 표본) · summary.json (구간별 요약)

어떻게 나누나
  CPU · 할당 · 스레드 — 프로세스가 직접 낸다 (hotdb_cdc_engine_*, cdc-core EngineResources).
      엔진 스레드를 한 스레드 그룹에 넣어 세므로 이름과 상관없이 엔진 몫만 잡힌다.
      engine = Debezium 스레드 전체 - handler, handler = 배치 콜백(우리 해석 + DB 반영), process = 프로세스 전체
  메모리 — 힙은 스레드별로 나눌 수 없다. 같은 이미지 · 같은 -Xmx 로 엔진만 끈 쌍둥이(hotdb-zone-asm-base,
      hotdb.cdc.engine-enabled=false)를 옆에 띄우고 둘의 차이를 엔진(+ 반영 경로) 몫으로 본다.
"""
import csv
import json
import re
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ZONE = "asm"
ZONE_CODE = "ASSEMBLY"
TARGET = f"hotdb-zone-{ZONE}"
TARGET_URL = "http://localhost:59481"
BASE = f"{TARGET}-base"
BASE_PORT = 59494
BASE_URL = f"http://localhost:{BASE_PORT}"
SIM_URL = "http://localhost:59480"
IMAGE = "localhost/hotdb/zone-service:local"
BATCH_ROWS = 10_000

# (열 이름, 지표, 라벨 조건) — 같은 이름이 여러 줄이면 더한다
METRICS = [
    ("cpu_engine", "hotdb_cdc_engine_cpu_seconds_total", 'part="engine"'),
    ("cpu_handler", "hotdb_cdc_engine_cpu_seconds_total", 'part="handler"'),
    ("cpu_process", "hotdb_cdc_engine_cpu_seconds_total", 'part="process"'),
    ("alloc_engine", "hotdb_cdc_engine_alloc_bytes_total", 'part="engine"'),
    ("alloc_handler", "hotdb_cdc_engine_alloc_bytes_total", 'part="handler"'),
    ("alloc_process", "hotdb_cdc_engine_alloc_bytes_total", 'part="process"'),
    ("threads_engine", "hotdb_cdc_engine_threads", ""),
    ("queue_used", "hotdb_cdc_engine_queue", 'state="used"'),
    ("behind_s", "hotdb_cdc_engine_behind_source_seconds", ""),
    ("events", "hotdb_cdc_events_total", ""),
    ("threads_jvm", "jvm_threads_live_threads", ""),
    ("heap_used", "jvm_memory_used_bytes", 'area="heap"'),
    ("nonheap_used", "jvm_memory_used_bytes", 'area="nonheap"'),
    ("heap_live", "jvm_gc_live_data_size_bytes", ""),
    ("gc_pause_s", "jvm_gc_pause_seconds_sum", ""),
    ("gc_count", "jvm_gc_pause_seconds_count", ""),
]
CSV_COLS = (["t", "phase"] + [f"{n}" for n, _, _ in METRICS]
            + ["base_heap_used", "base_heap_live", "base_nonheap_used", "base_threads_jvm", "base_cpu_process",
               "rss_mb", "base_rss_mb", "pids", "base_pids", "slot_lag_bytes"])


def sh(*args, check=True):
    r = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and r.returncode != 0:
        sys.exit(f"실패: {' '.join(args)}\n{r.stderr}")
    return r.stdout.strip()


def sql(q):
    return sh("podman", "exec", "hotdb-pg", "psql", "-U", "postgres", "-d", "hotdb", "-Atc", q)


def http(url, method="GET", timeout=3):
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8")


def sim(path):
    http(f"{SIM_URL}/sim/{path}", "POST")


def scrape(url):
    """지표 → {열 이름: 값}. 프로세스가 없으면 빈 dict"""
    try:
        text = http(f"{url}/actuator/prometheus")
    except Exception:
        return {}
    out = {}
    for name, metric, label in METRICS:
        total, hit = 0.0, False
        for m in re.finditer(rf"^{metric}(\{{[^}}]*\}})? (\S+)$", text, re.M):
            if label and label not in (m.group(1) or ""):
                continue
            try:
                v = float(m.group(2))
            except ValueError:
                continue
            if v == v:   # NaN 은 건너뛴다
                total, hit = total + v, True
        if hit:
            out[name] = total
    return out


def mb(s):
    m = re.match(r"([\d.]+)\s*([kMG]?)B", s)
    if not m:
        return None
    return float(m.group(1)) * {"": 1 / 1e6, "k": 1 / 1e3, "M": 1, "G": 1e3}[m.group(2)]


class Sampler:
    """1초마다 두 프로세스의 지표, 3초마다 컨테이너 RSS · 슬롯 지연을 찍는다"""

    def __init__(self, out_dir):
        self.out = open(out_dir / "samples.csv", "w", newline="", encoding="utf-8")
        self.w = csv.DictWriter(self.out, CSV_COLS)
        self.w.writeheader()
        self.phase = "-"
        self.rows = []
        self.slow = {}
        self.stop = threading.Event()
        self.lock = threading.Lock()

    def slow_loop(self):
        while not self.stop.is_set():
            vals = {}
            try:
                stats = json.loads(sh("podman", "stats", "--no-stream", "--format", "json", TARGET, BASE, check=False) or "[]")
                for s in stats:
                    pre = "base_" if s["name"] == BASE else ""
                    vals[pre + "rss_mb"] = mb(s["mem_usage"].split("/")[0].strip())
                    vals[pre + "pids"] = int(s["pids"])
            except Exception:
                pass
            try:
                vals["slot_lag_bytes"] = float(sql(
                    f"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots "
                    f"WHERE slot_name = 'zone_{ZONE}'") or "nan")
            except Exception:
                pass
            with self.lock:
                self.slow = vals
            self.stop.wait(3)

    def fast_loop(self):
        while not self.stop.is_set():
            t = time.time()
            row = {"t": round(t, 2), "phase": self.phase}
            row.update(scrape(TARGET_URL))
            b = scrape(BASE_URL)
            for k in ("heap_used", "heap_live", "nonheap_used", "threads_jvm", "cpu_process"):
                if k in b:
                    row["base_" + k] = b[k]
            with self.lock:
                row.update(self.slow)
            self.rows.append(row)
            self.w.writerow(row)
            self.out.flush()
            self.stop.wait(max(0.0, 1 - (time.time() - t)))

    def start(self):
        for f in (self.slow_loop, self.fast_loop):
            threading.Thread(target=f, daemon=True).start()

    def close(self):
        self.stop.set()
        time.sleep(1.5)
        self.out.close()

    def last(self, key):
        for r in reversed(self.rows):
            if r.get(key) is not None:
                return r[key]
        return None


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def record(s, phase, seconds):
    s.phase = phase
    log(f"{phase} — {seconds}초")
    time.sleep(seconds)
    s.phase = "-"


def wait_up(url, seconds=180):
    t0 = time.time()
    while time.time() - t0 < seconds:
        try:
            if '"state":"RUNNING"' in http(f"{url}/actuator/health") or '"state":"STARTING"' in http(f"{url}/actuator/health"):
                return
        except Exception:
            pass
        time.sleep(1)
    sys.exit(f"{url} 이 {seconds}초 안에 뜨지 않았다")


# ── 기준선 (엔진만 끈 쌍둥이) ─────────────────────────────────────────────────

def start_base():
    sh("podman", "rm", "-f", BASE, check=False)
    env = sh("podman", "inspect", TARGET, "--format", "{{range .Config.Env}}{{println .}}{{end}}")
    java_opts = next((l.split("=", 1)[1] for l in env.splitlines() if l.startswith("JAVA_TOOL_OPTIONS=")), "-Xmx256m")
    net = next(iter(json.loads(sh("podman", "inspect", TARGET, "--format", "{{json .NetworkSettings.Networks}}"))))
    sh("podman", "run", "-d", "--name", BASE, "--network", net, "-p", f"{BASE_PORT}:8080",
       "-e", f"ZONE={ZONE}", "-e", "HOTDB_HOST=hotdb-pg", "-e", "HOTDB_PORT=5432", "-e", "CDC_OFFSET_DIR=/tmp",
       "-e", f"JAVA_TOOL_OPTIONS={java_opts}",
       "-e", "HOTDB_CDC_ENGINE_ENABLED=false",
       "-e", "HOTDB_CDC_DEAD_LETTER_REPROCESS_ENABLED=false",   # 쌍둥이가 진짜 모듈의 dead letter 를 집지 않게
       IMAGE)
    t0 = time.time()
    while time.time() - t0 < 120:
        try:
            http(f"{BASE_URL}/actuator/prometheus")
            log(f"기준선 {BASE} 기동 ({java_opts})")
            return
        except Exception:
            time.sleep(2)
    sys.exit("기준선 컨테이너가 뜨지 않았다")


# ── 시나리오 ───────────────────────────────────────────────────────────────

def idle(s):
    sim("pause")
    time.sleep(30)
    record(s, "유휴", 120)
    sim("speed?value=1")
    sim("resume")
    time.sleep(30)
    record(s, "평시 x1", 300)


BATCH_SQL = f"""
INSERT INTO tsdb.status_history (event_time, tag_key, status, error_code, last_heartbeat_at, scan_rate_pts_per_sec,
                                 temperature_c, connectivity_rssi, fov_mode, source_time_text, source_ingested_at, received_at)
SELECT now() + g.n * interval '1 millisecond', t.tag_key,
       CASE WHEN g.n % 7 = 0 THEN 'CALIBRATING' ELSE 'ONLINE' END, NULL, now(), 100000 + g.n, 35.5, -60, 'WIDE',
       to_char(now() + g.n * interval '1 millisecond', 'YYYY-MM-DD"T"HH24:MI:SS.US') || '0', now(), clock_timestamp()
  FROM (SELECT c.tag_key FROM tsdb.tag_catalog c JOIN tsdb.device d ON d.site = c.site AND d.device_id = c.device_id
         WHERE c.channel = 'status' AND d.zone = '{ZONE_CODE}') t
 CROSS JOIN generate_series(1, 1000) g(n)
 ORDER BY g.n, t.tag_key
 LIMIT {BATCH_ROWS}
"""


def batch(s, rounds=3):
    sim("pause")
    time.sleep(30)
    for i in range(1, rounds + 1):
        s.phase = f"배치 {i}"
        before = s.last("events") or 0
        log(f"배치 {i} — {BATCH_ROWS:,}행 한 트랜잭션")
        sql(BATCH_SQL)
        t0 = time.time()
        while time.time() - t0 < 300:
            time.sleep(1)
            if (s.last("events") or 0) - before >= BATCH_ROWS and (s.last("queue_used") or 0) == 0:
                break
        log(f"배치 {i} 다 받음 ({time.time() - t0:.1f}초)")
        time.sleep(20)   # 뒤끝 (GC · 오프셋 기록)
        s.phase = "-"
        time.sleep(30)
    sim("speed?value=1")
    sim("resume")


def backlog(s, stop_seconds=180, speed=5):
    sim("speed?value=1")
    sim("resume")
    time.sleep(20)
    log(f"{TARGET} 멈춤 · 발행 x{speed} {stop_seconds}초")
    sh("podman", "stop", TARGET)
    sim(f"speed?value={speed}")
    s.phase = "적체 쌓는 중"
    time.sleep(stop_seconds)
    sim("speed?value=1")
    log(f"슬롯 미확인 WAL {s.last('slot_lag_bytes') / 1e6:.0f} MB — 다시 띄움")
    s.phase = "적체 따라잡기"
    t0 = time.time()
    sh("podman", "start", TARGET)
    wait_up(TARGET_URL)
    calm = 0
    while time.time() - t0 < 900:
        time.sleep(1)
        lag = s.last("slot_lag_bytes")
        calm = calm + 1 if lag is not None and lag < 2e6 and (s.last("queue_used") or 0) < 100 else 0
        if calm >= 10:
            break
    log(f"따라잡음 ({time.time() - t0:.0f}초)")
    s.phase = "적체 뒤 평시"
    time.sleep(60)
    s.phase = "-"


# ── 요약 ──────────────────────────────────────────────────────────────────

def summarize(rows):
    phases = []
    for r in rows:
        if r["phase"] != "-" and r["phase"] not in phases:
            phases.append(r["phase"])
    out = {}
    for p in phases:
        rs = [r for r in rows if r["phase"] == p]
        dur = rs[-1]["t"] - rs[0]["t"]
        if dur <= 0:
            continue

        def delta(k):
            vs = [r[k] for r in rs if r.get(k) is not None]
            return vs[-1] - vs[0] if len(vs) > 1 else None

        def stat(k):
            vs = [r[k] for r in rs if r.get(k) is not None]
            return {"avg": sum(vs) / len(vs), "max": max(vs), "min": min(vs)} if vs else None

        def rate(k):
            d = delta(k)
            return d / dur if d is not None else None

        def pct(k):   # 코어 1개 = 100%
            r = rate(k)
            return r * 100 if r is not None else None

        cpu_p = pct("cpu_process")
        cpu_e = pct("cpu_engine")
        cpu_h = pct("cpu_handler")
        # 1초 표본으로 본 최고 순간 CPU
        peak = {}
        for k in ("cpu_engine", "cpu_handler", "cpu_process"):
            vs = [(r["t"], r[k]) for r in rs if r.get(k) is not None]
            peak[k] = max(((b[1] - a[1]) / (b[0] - a[0]) * 100 for a, b in zip(vs, vs[1:]) if b[0] > a[0]), default=None)
        out[p] = {
            "seconds": round(dur, 1),
            "events": delta("events"),
            "events_per_s": rate("events"),
            "cpu_pct": {"engine": cpu_e, "handler": cpu_h, "process": cpu_p,
                        "other": None if None in (cpu_p, cpu_e, cpu_h) else cpu_p - cpu_e - cpu_h},
            "cpu_pct_peak": {k.removeprefix("cpu_"): v for k, v in peak.items()},
            "cpu_seconds": {"engine": delta("cpu_engine"), "handler": delta("cpu_handler"), "process": delta("cpu_process")},
            "alloc_mb_per_s": {k: (rate(f"alloc_{k}") or 0) / 1e6 for k in ("engine", "handler", "process")},
            "alloc_bytes_per_event": {k: (delta(f"alloc_{k}") / delta("events")) if delta("events") else None
                                      for k in ("engine", "handler")},
            "threads_engine": stat("threads_engine"),
            "threads_jvm": stat("threads_jvm"),
            "base_threads_jvm": stat("base_threads_jvm"),
            "pids": stat("pids"),
            "base_pids": stat("base_pids"),
            "queue_used": stat("queue_used"),
            "heap_used_mb": scale(stat("heap_used")),
            "heap_live_mb": scale(stat("heap_live")),
            "nonheap_mb": scale(stat("nonheap_used")),
            "base_heap_used_mb": scale(stat("base_heap_used")),
            "base_heap_live_mb": scale(stat("base_heap_live")),
            "base_nonheap_mb": scale(stat("base_nonheap_used")),
            "rss_mb": stat("rss_mb"),
            "base_rss_mb": stat("base_rss_mb"),
            "gc_pause_ms": (delta("gc_pause_s") or 0) * 1000,
            "gc_count": delta("gc_count"),
            "slot_lag_mb": scale(stat("slot_lag_bytes")),
        }
    return out


def scale(st):
    return {k: v / 1e6 for k, v in st.items()} if st else None


def main():
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    steps = {"idle": [idle], "batch": [batch], "backlog": [backlog], "all": [idle, batch, backlog]}.get(what)
    if not steps:
        sys.exit(__doc__)
    out_dir = ROOT / "app" / "build" / "engine-resource" / datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True)
    start_base()
    s = Sampler(out_dir)
    s.start()
    time.sleep(5)
    try:
        for step in steps:
            step(s)
    finally:
        s.close()
        sh("podman", "rm", "-f", BASE, check=False)
        try:
            sim("speed?value=1")
            sim("resume")
        except Exception:
            pass
    summary = {"when": datetime.now().isoformat(timespec="seconds"), "target": TARGET,
               "phases": summarize(s.rows)}
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    log(f"결과: {out_dir}")
    for p, v in summary["phases"].items():
        c = v["cpu_pct"]
        print(f"  {p:10s} {v['seconds']:6.0f}s  이벤트/초 {v['events_per_s'] or 0:7.0f}  "
              f"CPU% 엔진 {c['engine'] or 0:5.1f} · 반영 {c['handler'] or 0:5.1f} · 프로세스 {c['process'] or 0:5.1f}  "
              f"엔진 스레드 {v['threads_engine']['max'] if v['threads_engine'] else '-'}  "
              f"힙 {v['heap_used_mb']['max'] if v['heap_used_mb'] else 0:.0f}MB (기준선 {v['base_heap_used_mb']['max'] if v['base_heap_used_mb'] else 0:.0f})")


if __name__ == "__main__":
    main()
