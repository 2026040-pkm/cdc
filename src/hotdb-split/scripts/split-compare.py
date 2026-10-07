#!/usr/bin/env python3
"""일체형 vs 분리형(캡처부 · 전달부) 비교 — 같은 WAL · 같은 부하를 나란히 받게 하고 자원 · 장애 · 정합성을 잰다.

    python scripts/split-compare.py setup      # svc_int · svc_split 스키마 (svc 와 같은 표, RLS · FK 없음)
    python scripts/split-compare.py all        # 아래 전부 (약 30분)
    python scripts/split-compare.py load       # 평시(유휴 · x1) · 1만 건 배치 · 적체
    python scripts/split-compare.py faults     # 장애 주입 — 죽였다 바로 띄우기 · 재배포
    python scripts/split-compare.py verify     # 정합성 — svc_int 와 svc_split 를 행 단위로
    python scripts/split-compare.py teardown   # 컨테이너 · 슬롯(int_asm · capture_asm) · 스키마 정리
    python scripts/split-compare.py summarize <결과 디렉터리>   # samples.csv 만으로 summary.json 다시

결과: app/build/split-compare/<시각>/samples.csv · events.json · summary.json

대상 (compose.split.yml, 전부 이 복사본 코드):
  int   hotdb-split-int-asm      일체형 — 엔진 + 반영 + 판별 한 프로세스, 슬롯 int_asm → svc_int
  base  hotdb-split-int-base     일체형에서 엔진만 끈 쌍둥이 (메모리 기준선)
  cap   hotdb-split-capture-asm  캡처부 — 엔진 + 변경 로그, 슬롯 capture_asm
  app   hotdb-split-apply-asm    전달부 — 로그 당겨오기 + 반영 + 판별 → svc_split

발행기(field-simulator)는 이 스크립트가 잡는다. 다른 작업이 배속을 바꾸면 표본에 sim_ok=0 이 찍히고 그 구간은 요약에서 무효로 표시된다.
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
SIM_URL = "http://localhost:59480"
ZONE_CODE = "ASSEMBLY"
BATCH_ROWS = 10_000
TARGETS = {
    "int": ("hotdb-split-int-asm", "http://localhost:59497"),
    "base": ("hotdb-split-int-base", "http://localhost:59498"),
    "cap": ("hotdb-split-capture-asm", "http://localhost:59495"),
    "app": ("hotdb-split-apply-asm", "http://localhost:59496"),
}
SLOTS = {"int": "int_asm", "cap": "capture_asm"}
SCHEMAS = {"int": "svc_int", "app": "svc_split"}

# (열, 지표, 라벨 조건) — 같은 이름 여러 줄이면 더한다
METRICS = [
    ("cpu", "process_cpu_time_ns_total", "", 1e-9),
    ("cpu_engine", "hotdb_cdc_engine_cpu_seconds_total", 'part="engine"', 1),
    ("cpu_handler", "hotdb_cdc_engine_cpu_seconds_total", 'part="handler"', 1),
    ("alloc", "hotdb_cdc_engine_alloc_bytes_total", 'part="process"', 1),
    ("alloc_engine", "hotdb_cdc_engine_alloc_bytes_total", 'part="engine"', 1),
    ("alloc_handler", "hotdb_cdc_engine_alloc_bytes_total", 'part="handler"', 1),
    ("threads_engine", "hotdb_cdc_engine_threads", "", 1),
    ("threads", "jvm_threads_live_threads", "", 1),
    ("heap", "jvm_memory_used_bytes", 'area="heap"', 1e-6),
    ("heap_live", "jvm_gc_live_data_size_bytes", "", 1e-6),
    ("nonheap", "jvm_memory_used_bytes", 'area="nonheap"', 1e-6),
    ("gc_s", "jvm_gc_pause_seconds_sum", "", 1),
    ("events", "hotdb_cdc_events_total", "", 1),
    ("queue", "hotdb_cdc_engine_queue", 'state="used"', 1),
    ("log_last", "hotdb_log_last_seq", "", 1),
    ("log_mb", "hotdb_log_bytes", "", 1e-6),
    ("log_blocked", "hotdb_log_blocked", "", 1),
    ("behind", "hotdb_log_behind_records", "", 1),
    ("applied", "hotdb_log_applied_seq", "", 1),
    ("batch_s", "hotdb_cdc_batch_seconds_sum", "", 1),
    ("batch_n", "hotdb_cdc_batch_seconds_count", "", 1),
]
COLS = (["t", "phase", "sim_ok", "sim_speed"]
        + [f"{k}_{m[0]}" for k in TARGETS for m in METRICS]
        + [f"{k}_rss" for k in TARGETS] + [f"{k}_pids" for k in TARGETS]
        + [f"{k}_slot_mb" for k in SLOTS] + [f"{k}_fresh_s" for k in SCHEMAS])


def sh(*args, check=True):
    r = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and r.returncode != 0:
        sys.exit(f"실패: {' '.join(args)}\n{r.stderr}")
    return r.stdout.strip()


def sql(q):
    return sh("podman", "exec", "hotdb-pg", "psql", "-U", "postgres", "-d", "hotdb", "-AtF", "|", "-c", q)


def http(url, method="GET", timeout=3):
    with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=timeout) as r:
        return r.read().decode("utf-8")


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ── 발행기 — 이 스크립트가 정한 상태를 기억하고, 표본마다 실제와 맞는지 본다 ─────────

class Sim:
    def __init__(self):
        self.want_running, self.want_speed = True, 1.0

    def set(self, running, speed=1.0):
        self.want_running, self.want_speed = running, speed
        http(f"{SIM_URL}/sim/speed?value={speed}", "POST")
        http(f"{SIM_URL}/sim/{'resume' if running else 'pause'}", "POST")

    def check(self):
        try:
            s = json.loads(http(f"{SIM_URL}/sim"))
        except Exception:
            return 0, None
        ok = s["running"] == self.want_running and (not s["running"] or abs(s["speed"] - self.want_speed) < 1e-6)
        return int(ok), s["speed"] if s["running"] else 0


SIM = Sim()


# ── 표본 ──────────────────────────────────────────────────────────────────

def scrape(url):
    try:
        text = http(f"{url}/actuator/prometheus", timeout=2)
    except Exception:
        return {}
    out = {}
    for name, metric, label, scale in METRICS:
        total, hit = 0.0, False
        for m in re.finditer(rf"^{metric}(\{{[^}}]*\}})? (\S+)$", text, re.M):
            if label and label not in (m.group(1) or ""):
                continue
            v = float(m.group(2))
            if v == v:
                total, hit = total + v, True
        if hit:
            out[name] = total * scale
    return out


def mb(s):
    m = re.match(r"([\d.]+)\s*([kMG]?)B", s)
    return float(m.group(1)) * {"": 1e-6, "k": 1e-3, "M": 1, "G": 1e3}[m.group(2)] if m else None


class Sampler:
    def __init__(self, out_dir):
        self.f = open(out_dir / "samples.csv", "w", newline="", encoding="utf-8")
        self.w = csv.DictWriter(self.f, COLS)
        self.w.writeheader()
        self.phase = "-"
        self.rows = []
        self.slow = {}
        self.stop = threading.Event()
        self.lock = threading.Lock()

    def slow_loop(self):
        names = {v[0]: k for k, v in TARGETS.items()}
        while not self.stop.is_set():
            vals = {}
            try:
                for s in json.loads(sh("podman", "stats", "--no-stream", "--format", "json",
                                       *names.keys(), check=False) or "[]"):
                    k = names.get(s["name"])
                    if k:
                        vals[f"{k}_rss"] = mb(s["mem_usage"].split("/")[0].strip())
                        vals[f"{k}_pids"] = int(s["pids"])
            except Exception:
                pass
            try:
                q = " UNION ALL ".join(
                    [f"SELECT '{k}_slot_mb', pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) / 1e6 "
                     f"FROM pg_replication_slots WHERE slot_name = '{s}'" for k, s in SLOTS.items()]
                    + [f"SELECT '{k}_fresh_s', extract(epoch FROM clock_timestamp() - max(src_received_at)) "
                       f"FROM {sc}.device_status_current WHERE module = 'asm'" for k, sc in SCHEMAS.items()])
                for line in sql(q).splitlines():
                    k, v = line.split("|")
                    vals[k] = float(v) if v else None
            except Exception:
                pass
            with self.lock:
                self.slow = vals
            self.stop.wait(2)

    def fast_loop(self):
        while not self.stop.is_set():
            t = time.time()
            ok, speed = SIM.check()
            row = {"t": round(t, 2), "phase": self.phase, "sim_ok": ok, "sim_speed": speed}
            for k, (_, url) in TARGETS.items():
                for name, v in scrape(url).items():
                    row[f"{k}_{name}"] = v
            with self.lock:
                row.update(self.slow)
            self.rows.append(row)
            self.w.writerow(row)
            self.f.flush()
            self.stop.wait(max(0.0, 1 - (time.time() - t)))

    def start(self):
        for fn in (self.slow_loop, self.fast_loop):
            threading.Thread(target=fn, daemon=True).start()

    def close(self):
        self.stop.set()
        time.sleep(1.5)
        self.f.close()

    def last(self, key):
        for r in reversed(self.rows[-30:]):
            if r.get(key) is not None:
                return r[key]
        return None


EVENTS = []


def mark(kind, **kw):
    EVENTS.append({"t": round(time.time(), 2), "kind": kind, **kw})


def record(s, phase, seconds):
    s.phase = phase
    log(f"{phase} — {seconds}초")
    time.sleep(seconds)
    s.phase = "-"


def wait_until(cond, limit, step=0.5):
    t0 = time.time()
    while time.time() - t0 < limit:
        if cond():
            return time.time()
        time.sleep(step)
    return None


def healthy(k):
    try:
        return '"state":"RUNNING"' in http(f"{TARGETS[k][1]}/actuator/health", timeout=2)
    except Exception:
        return False


# ── 시나리오 ───────────────────────────────────────────────────────────────

def load(s):
    SIM.set(False)
    time.sleep(30)
    record(s, "유휴", 120)
    SIM.set(True, 1)
    time.sleep(30)
    record(s, "평시 x1", 300)
    batch(s)
    backlog(s)


BATCH_SQL = """
WITH ins AS (
  INSERT INTO tsdb.status_history (event_time, tag_key, status, error_code, last_heartbeat_at, scan_rate_pts_per_sec,
                                   temperature_c, connectivity_rssi, fov_mode, source_time_text, source_ingested_at, received_at)
  SELECT now() + g.n * interval '1 millisecond', t.tag_key,
         CASE WHEN g.n % 7 = 0 THEN 'CALIBRATING' ELSE 'ONLINE' END, NULL, now(), 100000 + g.n, 35.5, -60, '{mark}',
         to_char(now() + g.n * interval '1 millisecond', 'YYYY-MM-DD"T"HH24:MI:SS.US') || '0', now(), clock_timestamp()
    FROM (SELECT c.tag_key FROM tsdb.tag_catalog c JOIN tsdb.device d ON d.site = c.site AND d.device_id = c.device_id
           WHERE c.channel = 'status' AND d.zone = '{zone}') t
   CROSS JOIN generate_series(1, 1000) g(n)
   ORDER BY g.n, t.tag_key
   LIMIT {rows}
  RETURNING tag_key)
SELECT count(*), count(DISTINCT tag_key) FROM ins
"""


def batch(s, rounds=3):
    SIM.set(False)
    time.sleep(30)
    for i in range(1, rounds + 1):
        tag = f"BATCH-{datetime.now():%H%M%S}"
        s.phase = f"배치 {i}"
        log(f"배치 {i} — {BATCH_ROWS:,}행 한 트랜잭션 (fov_mode={tag})")
        t0 = time.time()
        rows, devices = (int(x) for x in sql(BATCH_SQL.format(mark=tag, zone=ZONE_CODE, rows=BATCH_ROWS)).split("|"))
        done = {}
        while time.time() - t0 < 300 and len(done) < 2:
            for k, sc in SCHEMAS.items():
                if k not in done and int(sql(f"SELECT count(*) FROM {sc}.device_status_current "
                                             f"WHERE module = 'asm' AND fov_mode = '{tag}'") or 0) >= devices:
                    done[k] = round(time.time() - t0, 2)
            time.sleep(0.3)
        mark("batch", round=i, rows=rows, devices=devices, done_s=done)
        log(f"배치 {i} 반영 끝 — 일체형 {done.get('int')}초 · 분리형 {done.get('app')}초")
        time.sleep(15)
        s.phase = "-"
        time.sleep(30)
    SIM.set(True, 1)


def backlog(s, stop_seconds=180, speed=5):
    SIM.set(True, 1)
    time.sleep(20)
    log(f"적체 — 일체형 · 전달부를 같이 멈춤, 발행 x{speed} {stop_seconds}초 (캡처부는 계속 돈다)")
    sh("podman", "stop", TARGETS["int"][0])
    sh("podman", "stop", TARGETS["app"][0])
    mark("backlog-stop")
    SIM.set(True, speed)
    s.phase = "적체 쌓는 중"
    time.sleep(stop_seconds)
    SIM.set(True, 1)
    mark("backlog-start", int_slot_mb=s.last("int_slot_mb"), cap_slot_mb=s.last("cap_slot_mb"),
         cap_log_mb=s.last("cap_log_mb"))
    log(f"쌓인 것: 슬롯 int_asm {s.last('int_slot_mb') or 0:.0f}MB · 슬롯 capture_asm {s.last('cap_slot_mb') or 0:.1f}MB "
        f"· 캡처 로그 {s.last('cap_log_mb') or 0:.0f}MB — 둘 다 다시 띄움")
    s.phase = "적체 따라잡기"
    t0 = time.time()
    sh("podman", "start", TARGETS["int"][0])
    sh("podman", "start", TARGETS["app"][0])
    caught = {}
    while time.time() - t0 < 900 and len(caught) < 2:
        time.sleep(1)
        if "int" not in caught and (s.last("int_slot_mb") or 99) < 2 and (s.last("int_fresh_s") or 99) < 5:
            caught["int"] = round(time.time() - t0, 1)
        if "app" not in caught and s.last("app_behind") == 0 and (s.last("app_fresh_s") or 99) < 5:
            caught["app"] = round(time.time() - t0, 1)
    mark("backlog-caught", caught_s=caught)
    log(f"따라잡음 — 일체형 {caught.get('int')}초 · 분리형 {caught.get('app')}초")
    s.phase = "적체 뒤 평시"
    time.sleep(60)
    s.phase = "-"


def faults(s):
    SIM.set(True, 1)
    time.sleep(30)
    for kind, victim, watch in [("crash", "int", "int"), ("crash", "app", "app"), ("crash", "cap", "app"),
                                ("redeploy", "int", "int"), ("redeploy", "app", "app")]:
        name = TARGETS[victim][0]
        s.phase = f"장애 {kind} {victim}"
        log(f"{s.phase} — {name}")
        t0 = time.time()
        fresh_before = s.last(f"{watch}_fresh_s")
        if kind == "crash":
            sh("podman", "kill", name)           # 프로세스가 죽은 것 — 정리 단계 없이
            sh("podman", "start", name, check=False)   # 감독자(restart 정책)가 바로 다시 띄운 것
        else:
            sh("podman", "restart", "-t", "30", name)   # 정상 종료 후 기동 = 새 이미지 배포
        up = wait_until(lambda: healthy(victim), 180)
        # 회복 = 지켜보는 쪽 결과가 다시 3초 안쪽으로 신선해질 때
        time.sleep(2)
        fresh = wait_until(lambda: (s.last(f"{watch}_fresh_s") or 99) < 3, 300, 1)
        peak = max((r.get(f"{watch}_fresh_s") or 0) for r in s.rows if r["t"] >= t0)
        other = "app" if watch == "int" else "int"
        other_peak = max((r.get(f"{other}_fresh_s") or 0) for r in s.rows if r["t"] >= t0)
        cap_slot_peak = max((r.get("cap_slot_mb") or 0) for r in s.rows if r["t"] >= t0)
        mark("fault", fault=kind, victim=victim, watch=watch, fresh_before=fresh_before,
             up_s=round(up - t0, 1) if up else None, recover_s=round(fresh - t0, 1) if fresh else None,
             fresh_peak_s=round(peak, 1), other_fresh_peak_s=round(other_peak, 1),
             cap_slot_peak_mb=round(cap_slot_peak, 2))
        log(f"  기동 {round(up - t0, 1) if up else '-'}초 · 결과 회복 {round(fresh - t0, 1) if fresh else '-'}초 "
            f"· 최대 묵음 {peak:.1f}초 (상대편 {other_peak:.1f}초)")
        s.phase = "-"
        time.sleep(45)


# ── 정합성 ─────────────────────────────────────────────────────────────────

COMPARE = {
    "device_status_current": ("device_id, status, error_code, src_event_time, fov_mode, scan_rate_pts_per_sec", "true"),
    "device_status_change": ("device_id, changed_at, status, error_code", "changed_at >= {t0}"),
    "scan": ("scan_id, last_event_at, last_event_type, block_progress_rate, completed_at", "src_received_at >= {t0}"),
    "actual_result": ("hull_no, block_id, scan_id, completed_at, judged_status", "src_received_at >= {t0}"),
    "artifact": ("scan_id, artifact_type, segment_key, storage_uri, checksum", "src_received_at >= {t0}"),
}


def verify(s, since):
    SIM.set(False)
    log("정합성 — 발행 멈추고 두 쪽이 다 비울 때까지")
    wait_until(lambda: (s.last("int_slot_mb") or 99) < 1 and s.last("app_behind") == 0
               and (s.last("cap_slot_mb") or 99) < 1, 300, 1)
    time.sleep(10)   # 판별 단계가 PENDING 을 다 집도록
    t0 = f"to_timestamp({since})"
    out = {}
    for table, (cols, where) in COMPARE.items():
        w = where.format(t0=t0)
        q = (f"WITH a AS (SELECT {cols} FROM svc_int.{table} WHERE module = 'asm' AND {w}), "
             f"b AS (SELECT {cols} FROM svc_split.{table} WHERE module = 'asm' AND {w}) "
             f"SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b), "
             f"(SELECT count(*) FROM (SELECT * FROM a EXCEPT SELECT * FROM b) x), "
             f"(SELECT count(*) FROM (SELECT * FROM b EXCEPT SELECT * FROM a) y)")
        n_int, n_split, only_int, only_split = (int(x) for x in sql(q).split("|"))
        out[table] = {"int_rows": n_int, "split_rows": n_split, "only_int": only_int, "only_split": only_split}
        log(f"  {table:22s} 일체형 {n_int:>8,} · 분리형 {n_split:>8,} · 한쪽에만 {only_int} / {only_split}")
    mark("verify", result=out)
    SIM.set(True, 1)
    return out


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

        def series(k):
            return [(r["t"], r[k]) for r in rs if r.get(k) is not None]

        def rate(k):
            v = series(k)
            # 재기동으로 누적값이 0 부터 다시 시작한 구간은 이어 붙인다. 엔진 CPU(그룹 - 콜백)는 두 값을 다른 순간에 읽어
            # 조금 줄어 보일 수 있다 — 반 넘게 줄었을 때만 재기동으로 본다
            total = sum(b[1] - a[1] if b[1] >= a[1] else (b[1] if b[1] < a[1] * 0.5 else 0.0)
                        for a, b in zip(v, v[1:]))
            span = v[-1][0] - v[0][0] if len(v) > 1 else 0
            return total / span if span > 0 else None

        def peak_rate(k):
            v = series(k)
            return max(((b[1] - a[1]) / (b[0] - a[0]) for a, b in zip(v, v[1:]) if b[0] > a[0] and b[1] >= a[1]),
                       default=None)

        def stat(k):
            v = [x for _, x in series(k)]
            return {"avg": sum(v) / len(v), "max": max(v)} if v else None

        per = {}
        for k in TARGETS:
            per[k] = {
                "cpu_pct": (rate(f"{k}_cpu") or 0) * 100 if series(f"{k}_cpu") else None,
                "cpu_pct_peak": (peak_rate(f"{k}_cpu") or 0) * 100 if series(f"{k}_cpu") else None,
                "cpu_engine_pct": (rate(f"{k}_cpu_engine") or 0) * 100 if series(f"{k}_cpu_engine") else None,
                "cpu_handler_pct": (rate(f"{k}_cpu_handler") or 0) * 100 if series(f"{k}_cpu_handler") else None,
                "alloc_mb_s": (rate(f"{k}_alloc") or 0) / 1e6 if series(f"{k}_alloc") else None,
                "events_s": rate(f"{k}_events"),
                "threads": stat(f"{k}_threads"),
                "threads_engine": stat(f"{k}_threads_engine"),
                "heap_mb": stat(f"{k}_heap"),
                "heap_live_mb": stat(f"{k}_heap_live"),
                "nonheap_mb": stat(f"{k}_nonheap"),
                "rss_mb": stat(f"{k}_rss"),
                "pids": stat(f"{k}_pids"),
                "gc_ms_s": (rate(f"{k}_gc_s") or 0) * 1000 if series(f"{k}_gc_s") else None,
                "queue": stat(f"{k}_queue"),
            }
        out[p] = {
            "seconds": round(dur, 1),
            "sim_ok_pct": round(100 * sum(r.get("sim_ok") or 0 for r in rs) / len(rs), 1),
            "per": per,
            "cap_log_mb": stat("cap_log_mb"),
            "cap_log_blocked": stat("cap_log_blocked"),
            "app_behind": stat("app_behind"),
            "int_slot_mb": stat("int_slot_mb"),
            "cap_slot_mb": stat("cap_slot_mb"),
            "int_fresh_s": stat("int_fresh_s"),
            "app_fresh_s": stat("app_fresh_s"),
        }
    return out


# ── 준비 · 정리 ─────────────────────────────────────────────────────────────

SETUP_SQL = """
DO $$
DECLARE s text; t text;
BEGIN
  FOREACH s IN ARRAY ARRAY['svc_int','svc_split'] LOOP
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', s);
    FOREACH t IN ARRAY ARRAY['device_status_current','device_status_change','scan','actual_result','artifact'] LOOP
      EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (LIKE svc.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)', s, t, t);
    END LOOP;
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO svc_asm', s);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO svc_asm', s);
  END LOOP;
END $$
"""


def teardown():
    sh("podman", "compose", "-f", str(ROOT / "compose.split.yml"), "down", "-v", check=False)
    for slot in ("int_asm", "capture_asm", "int_base"):
        sql(f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots WHERE slot_name = '{slot}' AND NOT active")
    sql("DELETE FROM ops.cdc_checkpoint WHERE pipeline IN ('int-asm','capture-asm','int-base')")
    sql("DELETE FROM ops.cdc_heartbeat WHERE pipeline IN ('int-asm','capture-asm','int-base')")
    sql("DROP SCHEMA IF EXISTS svc_int CASCADE; DROP SCHEMA IF EXISTS svc_split CASCADE")
    log("정리 끝 — 슬롯 int_asm · capture_asm, 스키마 svc_int · svc_split, 컨테이너 · 볼륨")


def main():
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    if what == "setup":
        sql(SETUP_SQL)
        return log("svc_int · svc_split 준비")
    if what == "teardown":
        return teardown()
    if what == "summarize":   # 표본만 다시 요약 — python scripts/split-compare.py summarize <결과 디렉터리>
        d = Path(sys.argv[2])
        rows = []
        for r in csv.DictReader(open(d / "samples.csv", encoding="utf-8")):
            rows.append({k: (r[k] if k == "phase" else float(r[k])) for k in r if r[k] != ""})
        old = json.loads((d / "summary.json").read_text(encoding="utf-8"))
        old["phases"] = summarize(rows)
        (d / "summary.json").write_text(json.dumps(old, ensure_ascii=False, indent=1), encoding="utf-8")
        return log(f"다시 요약: {d / 'summary.json'}")
    steps = {"load": [load], "faults": [faults], "verify": [], "all": [load, faults]}.get(what)
    if steps is None:
        sys.exit(__doc__)
    out_dir = ROOT / "app" / "build" / "split-compare" / datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True)
    since = time.time()
    s = Sampler(out_dir)
    s.start()
    time.sleep(5)
    verdict = None
    try:
        for step in steps:
            step(s)
        if what in ("all", "verify"):
            verdict = verify(s, since)
    finally:
        s.close()
        try:
            SIM.set(True, 1)
        except Exception:
            pass
    (out_dir / "events.json").write_text(json.dumps(EVENTS, ensure_ascii=False, indent=1), encoding="utf-8")
    summary = {"when": datetime.now().isoformat(timespec="seconds"), "phases": summarize(s.rows), "verify": verdict}
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    log(f"결과: {out_dir}")
    for p, v in summary["phases"].items():
        c = {k: v["per"][k]["cpu_pct"] for k in TARGETS}
        print(f"  {p:14s} {v['seconds']:5.0f}s 발행기정상 {v['sim_ok_pct']:5.1f}%  CPU% 일체 {c['int'] or 0:5.1f} · "
              f"캡처 {c['cap'] or 0:5.1f} + 전달 {c['app'] or 0:5.1f}  RSS 일체 {(v['per']['int']['rss_mb'] or {}).get('max', 0):.0f}"
              f" · 캡처 {(v['per']['cap']['rss_mb'] or {}).get('max', 0):.0f} + 전달 {(v['per']['app']['rss_mb'] or {}).get('max', 0):.0f}")


if __name__ == "__main__":
    main()
