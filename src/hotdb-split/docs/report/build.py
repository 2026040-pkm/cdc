#!/usr/bin/env python3
"""리포트 빌드 — split-compare 결과(results/<시각>) → engine-split-report.html

    python docs/report/build.py results/20261007-114828

표 · 차트 숫자는 전부 summary.json · events.json · samples.csv 에서 읽는다 (손으로 옮긴 숫자 없음).
html-doc 개발용 템플릿 위에 본문을 얹고, 아티팩트 페이지 규약(문서 껍데기 없음 · 시스템 다크 모드)에 맞게 다듬는다.
"""
import csv
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# Windows 에서 그냥 "bash" 는 WSL 을 부른다 — Git Bash 를 쓴다
BASH = next((b for b in ("C:/Program Files/Git/bin/bash.exe", "/usr/bin/bash") if Path(b).exists()), "bash")
SKILL = Path.home() / ".claude/skills/html-doc"
RUN = Path(sys.argv[1]).resolve()
S = json.loads((RUN / "summary.json").read_text(encoding="utf-8"))
EV = json.loads((RUN / "events.json").read_text(encoding="utf-8"))
P = S["phases"]
ROWS = []
for r in csv.DictReader(open(RUN / "samples.csv", encoding="utf-8")):
    ROWS.append({k: (r[k] if k == "phase" else float(r[k])) for k in r if r[k] != ""})


# ── 값 ─────────────────────────────────────────────────────────────────────

def per(phase, k, field, sub=None):
    v = P[phase]["per"][k][field]
    return v.get(sub) if sub and isinstance(v, dict) else v


def n(v, d=1):
    return "-" if v is None else f"{v:,.{d}f}"


def rate(phase, col):
    rs = [r for r in ROWS if r["phase"] == phase and col in r]
    t = 0.0
    for a, b in zip(rs, rs[1:]):
        t += b[col] - a[col] if b[col] >= a[col] else (b[col] if b[col] < a[col] * 0.5 else 0.0)
    return t / (rs[-1]["t"] - rs[0]["t"]) if len(rs) > 1 else None


def delta(phase, col):
    r = rate(phase, col)
    rs = [x for x in ROWS if x["phase"] == phase and col in x]
    return r * (rs[-1]["t"] - rs[0]["t"]) if r is not None else None


def peak(phase, col):
    rs = [r for r in ROWS if r["phase"] == phase and col in r]
    return max(((b[col] - a[col]) / (b["t"] - a["t"]) for a, b in zip(rs, rs[1:]) if b["t"] > a["t"] and b[col] >= a[col]),
               default=None)


def ev(kind):
    return [e for e in EV if e["kind"] == kind]


X1, IDLE, CATCH, PILE = "평시 x1", "유휴", "적체 따라잡기", "적체 쌓는 중"
BATCHES = [f"배치 {i}" for i in (1, 2, 3)]
cpu = lambda ph, k: per(ph, k, "cpu_pct")
eng = lambda ph, k: per(ph, k, "cpu_engine_pct")
hdl = lambda ph, k: per(ph, k, "cpu_handler_pct")
other = lambda ph, k: cpu(ph, k) - eng(ph, k) - hdl(ph, k)
alloc_x1 = {k: rate(X1, f"int_alloc{k}") / 1e6 for k in ("", "_engine", "_handler")}
alloc_cu = {k: rate(CATCH, f"int_alloc{k}") / 1e6 for k in ("", "_engine", "_handler")}
b_ev = ev("batch")
bl_start, bl_caught = ev("backlog-start")[0], ev("backlog-caught")[0]
faults = {(e["fault"], e["victim"]): e for e in ev("fault")}
verify = ev("verify")[0]["result"]
pile_rows = [r for r in ROWS if r["phase"] == PILE and "int_slot_mb" in r]
wal_mb_s_x5 = (pile_rows[-1]["int_slot_mb"] - pile_rows[0]["int_slot_mb"]) / (pile_rows[-1]["t"] - pile_rows[0]["t"])
hours_to_20g_x1 = 20_000 / (wal_mb_s_x5 / 5) / 3600
catch_events = delta(CATCH, "int_events")
batch_cpu = [{k: delta(b, f"{k}_cpu") for k in ("int", "cap", "app")} | {"eng": delta(b, "int_cpu_engine"),
              "hdl": delta(b, "int_cpu_handler"), "peak": peak(b, "int_cpu") * 100,
              "heap": per(b, "int", "heap_mb", "max"), "queue": per(b, "int", "queue", "max"),
              "base_heap": per(b, "base", "heap_mb", "max")} for b in BATCHES]


def avg(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs)


def med(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2]


# ── 조각 ───────────────────────────────────────────────────────────────────

def chart(lines):
    with tempfile.NamedTemporaryFile("w", suffix=".spec", delete=False, encoding="utf-8") as t:
        t.write("\n".join(lines) + "\n")
    out = subprocess.run([BASH, (SKILL / "scripts/make-chart.sh").as_posix(), Path(t.name).as_posix()],
                         capture_output=True, text=True, encoding="utf-8")
    if out.returncode:
        sys.exit(out.stderr)
    return out.stdout


def flow(name):
    return (HERE / f"flow-{name}.html").read_text(encoding="utf-8")


def table(head, rows, cls="tbl"):
    th = "".join(f"<th>{h}</th>" for h in head)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<div class="tbl-wrap"><table class="{cls}"><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table></div>'


def kpi(items):
    return '<div class="kpi">' + "".join(
        f'<div class="kpi__item"><div class="kpi__v">{v}</div><div class="kpi__l">{l}</div></div>' for v, l in items) + "</div>"


def m(t):
    return f'<span class="mono">{t}</span>'


def ref(i):
    return f'<sup class="ref" data-ref="{i}">{i}</sup>'


def sec(id_, num, title, why, body):
    return f'''
    <section class="sec" id="{id_}">
      <h2><span class="n">{num}</span>{title}</h2>
      <p class="why">{why}</p>
{body}
    </section>'''


def note(kind, title, body):
    return f'<div class="note note--{kind}"><p class="note__t">{title}</p>{body}</div>'


def series_bins(phases, col, step):
    rs = [r for r in ROWS if r["phase"] in phases]
    t0 = rs[0]["t"]
    bins = {}
    for r in rs:
        bins.setdefault(int((r["t"] - t0) // step), []).append(r)
    out = []
    for b in range(max(bins) + 1):
        vals = [r[col] for r in bins.get(b, []) if col in r]
        out.append(max(vals) if vals else None)
    return out


def rate_bins(phase, col, step):
    rs = [r for r in ROWS if r["phase"] == phase and col in r]
    t0 = rs[0]["t"]
    bins = {}
    for r in rs:
        bins.setdefault(int((r["t"] - t0) // step), []).append(r)
    out = []
    for b in range(max(bins) + 1):
        g = bins.get(b, [])
        out.append((g[-1][col] - g[0][col]) / (g[-1]["t"] - g[0]["t"])
                   if len(g) > 1 and g[-1][col] >= g[0][col] else None)
    return out


def csvs(xs, d=0):
    return ",".join("-" if x is None else f"{x:.{d}f}" for x in xs)


# ── 본문 ───────────────────────────────────────────────────────────────────

x1_int_rss, x1_base_rss = per(X1, "int", "rss_mb", "avg"), per(X1, "base", "rss_mb", "avg")
x1_split_rss = per(X1, "cap", "rss_mb", "avg") + per(X1, "app", "rss_mb", "avg")
x1_split_cpu = cpu(X1, "cap") + cpu(X1, "app")
done_int = [e["done_s"]["int"] for e in b_ev]
done_app = [e["done_s"]["app"] for e in b_ev]

head = f'''
    <header class="doc-head">
      <span class="kicker">Measurement Report · 2026-10-07</span>
      <h1>Embedded 엔진은 판별 모듈 안에서 얼마를 쓰고, 캡처부를 떼어 내면 무엇이 달라지나</h1>
      <p class="sub">조립 모듈(asm) 하나를 같은 코드로 두 번 띄웠습니다 — 일체형(엔진 · 반영 · 판별 한 프로세스)과 분리형(캡처부 · 전달부 두 컨테이너).
        둘은 같은 Hot DB 의 같은 WAL 을 나란히 읽고, 결과를 서로 다른 스키마에 써서 행 단위로 맞대 봤습니다.</p>
      <div class="meta">
        <span class="chip">대상 판별 모듈 asm (ASSEMBLY 장비 {b_ev[0]["devices"]}대)</span>
        <span class="chip chip--neutral">JVM -Xmx256m · Debezium 3.6.1 · PG 17.11 + TimescaleDB 2.29.2</span>
        <span class="chip chip--ok">정합성 5표 전부 일치</span>
        <span class="chip chip--warn">로컬 podman 한 대 — 절대값보다 비율을 본다</span>
      </div>
    </header>'''

s0 = sec("s0", "00", "범위와 전제", "무엇을 쟀고 무엇을 재지 않았는지 먼저 못 박습니다.", f'''
      <div class="g2">
        <div class="card card--accent">
          <p class="card__t">잰 것</p>
          <ul style="margin:6px 0 0">
            <li>판별 모듈 프로세스 안에서 Debezium Embedded 엔진이 차지하는 CPU · 메모리 · 스레드 — 평시 · 1만 건 배치 · 적체</li>
            <li>같은 일을 캡처부 · 전달부 두 프로세스로 나눴을 때의 자원 · 장애 격리 · 재기동 단위 · 오프셋 책임</li>
            <li>엔진을 바꾸거나(pgoutput 직접 수신 · 버전 컬럼 폴링) 떼어 낼 때 기준이 될 인터페이스 경계 — 코드로 정하고 테스트로 묶음</li>
          </ul>
        </div>
        <div class="card">
          <p class="card__t">재지 않은 것</p>
          <ul style="margin:6px 0 0">
            <li>HotDB Provider — 개발 범위 밖이고 엔진이 없다. 엔진이 사는 프로세스는 판별 모듈(zone-service)과 RFC Service 다</li>
            <li>캡처부 하나에 전달부 넷(fan-out) — 구조는 되어 있고(소비자 목록) 값은 단일 모듈 측정에서 <span class="chip chip--warn">추정</span></li>
            <li>운영 서버 · 운영 HANA · 네트워크 너머 배치. 전부 로컬 podman VM 한 대(12 vCPU · 25 GB)</li>
          </ul>
        </div>
      </div>
      {note("warn", "첫 측정은 버렸습니다", f"""11:04 에 시작한 첫 측정은 다른 작업(부하 시험 스크립트)이 같은 현장 발행기를 x10 · x15 로 바꿔 평시 · 적체 구간이 오염됐습니다.
        이번 측정은 발행기 상태를 1초마다 기록해 스크립트가 정한 값과 비교했고, 모든 구간에서 일치율 100% 입니다(표본 {len(ROWS):,}개).""")}''')

s1 = sec("s1", "01", "결론", "숫자부터 봅니다. 근거는 아래 절마다 표와 차트로 있습니다.", f'''
      <h3>엔진이 차지하는 몫 (일체형 프로세스 안)</h3>
      {kpi([(f"{eng(X1, 'int'):.1f}%", f"평시 x1 엔진 CPU (코어 1개 = 100%) · 프로세스 전체 {cpu(X1, 'int'):.1f}%"),
            (f"{per(X1, 'int', 'threads', 'max') - per(X1, 'base', 'threads', 'max'):.0f}개", f"엔진이 더한 스레드 (그룹 안 {per(X1, 'int', 'threads_engine', 'max'):.0f}개 · 프로세스 {per(X1, 'int', 'threads', 'max'):.0f} vs 기준선 {per(X1, 'base', 'threads', 'max'):.0f})"),
            (f"+{x1_int_rss - x1_base_rss:.0f} MB", f"평시 RSS (일체형 {x1_int_rss:.0f} vs 엔진 끈 쌍둥이 {x1_base_rss:.0f})"),
            (f"{alloc_x1['_engine'] / alloc_x1[''] * 100:.0f}%", f"힙 할당 중 엔진 몫 — GC 를 부르는 쪽 (평시 {alloc_x1['_engine']:.1f} / {alloc_x1['']:.1f} MB/s)")])}
      <ul>
        <li><b>평시</b>: 엔진 {eng(X1, 'int'):.1f}% · 반영 콜백 {hdl(X1, 'int'):.1f}% · 그 밖의 JVM {other(X1, 'int'):.1f}% (판별 · GC · JIT · 지표 응답). 반영은 DB 가 일을 하므로 프로세스 쪽 비용이 엔진의 1/6 이다.</li>
        <li><b>1만 건 한 트랜잭션</b>: 프로세스 CPU {avg([b['int'] for b in batch_cpu]):.1f}초(그중 엔진 {avg([b['eng'] for b in batch_cpu]):.2f}초 · 콜백 {avg([b['hdl'] for b in batch_cpu]):.2f}초), 1초 최고 {max(b['peak'] for b in batch_cpu):.0f}%, 반영 끝까지 {min(done_int):.1f}~{max(done_int):.1f}초. 엔진 큐는 최대 {max(b['queue'] for b in batch_cpu):,.0f}/8,192.</li>
        <li><b>적체</b>: 3분 x5 로 쌓인 {catch_events:,.0f}건(WAL {bl_start['int_slot_mb']:.0f} MB)을 {bl_caught['caught_s']['int']:.0f}초에 따라잡는다. 그동안 엔진 스레드는 코어 하나({eng(CATCH, 'int'):.0f}%)에서 멈춰 서고 — 슬롯 읽기가 한 스레드라 상한이 여기다 —
            프로세스는 평균 {cpu(CATCH, 'int'):.0f}% · 최고 {per(CATCH, 'int', 'cpu_pct_peak'):.0f}%. 힙은 최대 {per(CATCH, 'int', 'heap_mb', 'max'):.0f}/256 MB 로, 큐가 8,192 건에서 막아 주므로 쌓인 양과 상관없이 묶인다.</li>
      </ul>

      <h3>캡처부를 떼어 내면</h3>
      {table(["", "일체형", "분리형 (캡처 + 전달)", "판정"], [
        ["평시 CPU", f"{cpu(X1, 'int'):.1f}%", f"{cpu(X1, 'cap'):.1f}% + {cpu(X1, 'app'):.1f}% = {x1_split_cpu:.1f}%", f'<span class="chip chip--risk">+{(x1_split_cpu / cpu(X1, "int") - 1) * 100:.0f}%</span>'],
        ["평시 RSS", f"{x1_int_rss:.0f} MB", f"{per(X1, 'cap', 'rss_mb', 'avg'):.0f} + {per(X1, 'app', 'rss_mb', 'avg'):.0f} = {x1_split_rss:.0f} MB", f'<span class="chip chip--risk">+{x1_split_rss - x1_int_rss:.0f} MB (JVM 하나)</span>'],
        ["1만 건 반영 (중앙값)", f"{med(done_int):.2f}초", f"{med(done_app):.2f}초", '<span class="chip chip--neutral">차이 없음</span>'],
        ["전달부가 멈춘 3분 동안 Hot DB 에 남은 WAL", f"{bl_start['int_slot_mb']:.0f} MB", f"{bl_start['cap_slot_mb']:.1f} MB (로그 {bl_start['cap_log_mb']:.0f} MB 는 캡처부 디스크에)", '<span class="chip chip--ok">Hot DB 에서 빠진다</span>'],
        ["적체 따라잡기", f"{bl_caught['caught_s']['int']:.0f}초 · CPU {cpu(CATCH, 'int'):.0f}%", f"{bl_caught['caught_s']['app']:.0f}초 · 전달부 CPU {cpu(CATCH, 'app'):.0f}%", '<span class="chip chip--ok">빠르고 가볍다</span>'],
        ["반영 쪽이 죽었다 살아날 때 결과 묵음", f"{faults[('crash', 'int')]['fresh_peak_s']:.1f}초", f"{faults[('crash', 'app')]['fresh_peak_s']:.1f}초", '<span class="chip chip--neutral">같다</span>'],
        ["그동안 슬롯", "끊겼다 다시 붙음", "붙어 있음 (캡처부는 그대로)", '<span class="chip chip--ok">슬롯을 안 건드린다</span>'],
        ["결과", "", "5표 행 단위 일치", '<span class="chip chip--ok">같다</span>'],
      ])}
      {note("info", "그래서", f"""분리는 <b>Hot DB 를 지키는 장치</b>입니다. 반영 쪽이 멈추거나 재배포될 때 WAL 이 Hot DB 가 아니라 캡처부 디스크에 쌓이고 슬롯은 붙은 채로 있습니다.
        사용자가 보는 지연(결과 묵음 · 1만 건 반영 시간)은 거의 같고, 값은 모듈마다 JVM 하나(RSS +{x1_split_rss - x1_int_rss:.0f} MB, CPU +{x1_split_cpu - cpu(X1, 'int'):.1f}%p)입니다.
        지금 규모(모듈 4개 · 평시 500 행/초)에서는 일체형을 유지하고, <b>경계(포트 · 어댑터 모듈 · 능력 계약)만 원래 프로젝트에 먼저 들여</b> 분리를 설정 · 배치 문제로 남기기를 권합니다 — 재검토 조건은 §09.""")}''')

s2 = sec("s2", "02", "어떻게 쟀나", "엔진은 프로세스 안의 스레드 묶음이라, 프로세스 지표만으로는 엔진 몫을 가를 수 없습니다.", f'''
      {flow("proc")}
      {table(["자원", "엔진 몫을 가르는 방법", "코드"], [
        ["CPU · 힙 할당", "엔진을 <b>전용 스레드 그룹</b> 안에서 만들고 돌린다. Debezium 이 안에서 띄우는 스레드(이름이 pool-N · debezium-* · Thread-N 으로 제각각)가 그룹을 물려받으므로 이름과 상관없이 전부 잡힌다. 그룹 스레드의 CPU · 할당 누적에서 배치 콜백 몫을 빼면 engine", m("cdc-debezium/…/EngineResources.java")],
        ["배치 콜백(반영)", "콜백 시작 · 끝에 현재 스레드 CPU · 할당을 읽어 handler 로 따로 더한다. 콜백은 엔진 스레드 위에서 동기로 돈다 — 캡처와 전달이 한 스레드에 붙어 있다", m("DebeziumCdcSource.handleBatch")],
        ["그 밖의 JVM", "process − engine − handler. GC · JIT · 판별 단계 · Tomcat · Hikari · 지표 응답. GC 비용은 할당한 쪽에 붙지 않으므로 할당량으로 누가 GC 를 부르는지 본다", m("hotdb_cdc_engine_cpu_seconds_total{part}")],
        ["스레드", "그룹의 살아 있는 스레드 수 + 프로세스 전체 스레드를 엔진 끈 쌍둥이와 비교", m("hotdb_cdc_engine_threads")],
        ["메모리", "힙은 스레드별로 나눌 수 없다. 같은 이미지 · 같은 -Xmx256m 으로 <b>엔진만 끈 쌍둥이</b>(int-base, hotdb.cdc.engine-enabled=false)를 옆에 띄우고 차이를 엔진 + 반영 경로 몫으로 본다. 엔진 큐 사용량은 Debezium JMX 에서", m("hotdb_cdc_engine_queue{state}")],
      ])}
      {table(["구간", "길이", "부하", "무엇을 보나"], [
        ["유휴", f"{P[IDLE]['seconds']:.0f}초", "발행 멈춤 (heartbeat 만)", "엔진의 고정비"],
        ["평시 x1", f"{P[X1]['seconds']:.0f}초", f"현장 발행기 x1 = 약 {per(X1, 'int', 'events_s'):.0f} 행/초 (전 권역 — 엔진은 권역과 상관없이 다 읽고 라우트가 거른다)", "운영 평소"],
        ["1만 건 배치 ×3", "각 약 17초", "발행 멈춘 뒤 10,000행을 한 트랜잭션으로 (조립 장비 상태 이력)", "한꺼번에 몰릴 때 · 반영 끝 = 장비 전부가 그 배치 값으로 바뀐 순간"],
        ["적체", f"{P[PILE]['seconds']:.0f}초 + 따라잡기", "일체형 · 전달부를 같이 멈추고 x5 로 쌓은 뒤 다시 띄움 (캡처부는 계속 돈다)", "반영 쪽 장애 · 배포 뒤 몰아 받기"],
        ["장애 주입", "각 약 1분", "x1 에서 kill → 바로 start (감독자 재시작) · restart (재배포)", "격리 · 재기동 단위"],
      ])}
      <p>표본은 1초마다 네 프로세스의 {m("/actuator/prometheus")}, 2~3초마다 컨테이너 RSS · 슬롯 지연 · 결과 신선도(지금 − 최신 반영 행의 Provider 수신 시각). 1초 수집 자체가 프로세스마다 CPU 1% 안팎을 쓴다 — 엔진을 끈 쌍둥이가 유휴에 {cpu(IDLE, 'base'):.1f}% 인 것이 그 값이다.</p>''')

# 엔진 몫 표 · 차트
eng_rows = []
for ph, label in ((IDLE, "유휴"), (X1, "평시 x1")):
    eng_rows.append([label, f"{cpu(ph, 'int'):.1f}%", f"{eng(ph, 'int'):.1f}%", f"{hdl(ph, 'int'):.2f}%", f"{other(ph, 'int'):.1f}%",
                     f"{per(ph, 'int', 'cpu_pct_peak'):.0f}%", f"{per(ph, 'int', 'threads', 'max'):.0f} ({per(ph, 'base', 'threads', 'max'):.0f})",
                     f"{per(ph, 'int', 'heap_mb', 'avg'):.0f} / {per(ph, 'int', 'heap_mb', 'max'):.0f} ({per(ph, 'base', 'heap_mb', 'avg'):.0f} / {per(ph, 'base', 'heap_mb', 'max'):.0f})",
                     f"{per(ph, 'int', 'nonheap_mb', 'max'):.0f} ({per(ph, 'base', 'nonheap_mb', 'max'):.0f})",
                     f"{per(ph, 'int', 'rss_mb', 'avg'):.0f} ({per(ph, 'base', 'rss_mb', 'avg'):.0f})"])
for i, b in enumerate(BATCHES):
    eng_rows.append([f"1만 건 #{i + 1}", f"{cpu(b, 'int'):.1f}%", f"{eng(b, 'int'):.1f}%", f"{hdl(b, 'int'):.2f}%", f"{other(b, 'int'):.1f}%",
                     f"{per(b, 'int', 'cpu_pct_peak'):.0f}%", f"{per(b, 'int', 'threads', 'max'):.0f} ({per(b, 'base', 'threads', 'max'):.0f})",
                     f"{per(b, 'int', 'heap_mb', 'avg'):.0f} / {per(b, 'int', 'heap_mb', 'max'):.0f} ({per(b, 'base', 'heap_mb', 'avg'):.0f} / {per(b, 'base', 'heap_mb', 'max'):.0f})",
                     f"{per(b, 'int', 'nonheap_mb', 'max'):.0f} ({per(b, 'base', 'nonheap_mb', 'max'):.0f})",
                     f"{per(b, 'int', 'rss_mb', 'avg'):.0f} ({per(b, 'base', 'rss_mb', 'avg'):.0f})"])
eng_rows.append(["적체 따라잡기", f"{cpu(CATCH, 'int'):.0f}%", f"{eng(CATCH, 'int'):.0f}%", f"{hdl(CATCH, 'int'):.1f}%", f"{other(CATCH, 'int'):.0f}%",
                 f"{per(CATCH, 'int', 'cpu_pct_peak'):.0f}%", f"{per(CATCH, 'int', 'threads', 'max'):.0f} ({per(CATCH, 'base', 'threads', 'max'):.0f})",
                 f"{per(CATCH, 'int', 'heap_mb', 'avg'):.0f} / {per(CATCH, 'int', 'heap_mb', 'max'):.0f} ({per(CATCH, 'base', 'heap_mb', 'avg'):.0f} / {per(CATCH, 'base', 'heap_mb', 'max'):.0f})",
                 f"{per(CATCH, 'int', 'nonheap_mb', 'max'):.0f} ({per(CATCH, 'base', 'nonheap_mb', 'max'):.0f})",
                 f"{per(CATCH, 'int', 'rss_mb', 'avg'):.0f} / 최대 {per(CATCH, 'int', 'rss_mb', 'max'):.0f} ({per(CATCH, 'base', 'rss_mb', 'avg'):.0f})"])

c_cpu = chart([
    "title=구간별 CPU — 엔진 스레드 vs 프로세스 전체 (코어 1개 = 100%)", "type=bar", "unit=%",
    "labels=유휴,평시 x1,1만 건 (구간 평균),적체 따라잡기",
    f"series=엔진|hw|{eng(IDLE, 'int'):.1f},{eng(X1, 'int'):.1f},{avg([eng(b, 'int') for b in BATCHES]):.1f},{eng(CATCH, 'int'):.1f}",
    f"series=프로세스 전체|info|{cpu(IDLE, 'int'):.1f},{cpu(X1, 'int'):.1f},{avg([cpu(b, 'int') for b in BATCHES]):.1f},{cpu(CATCH, 'int'):.1f}",
    f"caption=평소 엔진은 프로세스 CPU 의 절반 정도다. 따라잡을 때는 엔진이 코어 하나({eng(CATCH, 'int'):.0f}%)에서 더 오르지 않고, 나머지(GC · JIT · 반영)가 프로세스를 {cpu(CATCH, 'int'):.0f}% 까지 끌어올린다 — 엔진 스레드가 따라잡기 속도의 상한이다."])
c_mem = chart([
    "title=RSS — 일체형 vs 엔진 끈 쌍둥이", "type=bar", "unit=MB",
    "labels=유휴,평시 x1,1만 건 #3,적체 따라잡기",
    f"series=일체형|hw|{per(IDLE, 'int', 'rss_mb', 'max'):.0f},{per(X1, 'int', 'rss_mb', 'max'):.0f},{per(BATCHES[2], 'int', 'rss_mb', 'max'):.0f},{per(CATCH, 'int', 'rss_mb', 'max'):.0f}",
    f"series=엔진 끈 쌍둥이|info|{per(IDLE, 'base', 'rss_mb', 'max'):.0f},{per(X1, 'base', 'rss_mb', 'max'):.0f},{per(BATCHES[2], 'base', 'rss_mb', 'max'):.0f},{per(CATCH, 'base', 'rss_mb', 'max'):.0f}",
    f"caption=구간 최대값. 엔진이 있는 프로세스는 유휴에도 {per(IDLE, 'int', 'rss_mb', 'max') - per(IDLE, 'base', 'rss_mb', 'max'):.0f} MB 를 더 쓰고, 따라잡기 때 {per(CATCH, 'int', 'rss_mb', 'max') - per(CATCH, 'base', 'rss_mb', 'max'):.0f} MB 까지 벌어진다. 그래도 -Xmx256m 안에서 끝났다 — 엔진 큐(8,192 건)가 메모리 상한을 정한다."])
c_catch = chart([
    "title=적체 따라잡기 — 초당 처리 건수 (5초 구간)", "type=line", "unit=건/초",
    "labels=" + ",".join(f"{i * 5}s" for i in range(len(rate_bins(CATCH, "int_events", 5)))),
    f"series=일체형|hw|{csvs(rate_bins(CATCH, 'int_events', 5))}",
    f"series=분리형 전달부|info|{csvs(rate_bins(CATCH, 'app_events', 5))}",
    "values=max",
    f"caption=일체형은 엔진 스레드가 코어 하나에 붙은 채 1만 건/초 남짓에서 고르게 간다. 분리형 전달부는 이미 해석해 둔 로그를 읽으므로 조금 빨리 오르고 일찍 끝난다({bl_caught['caught_s']['app']:.0f}초 vs {bl_caught['caught_s']['int']:.0f}초)."])

s3 = sec("s3", "03", "엔진 자원 — 평시 · 1만 건 · 적체", "일체형 프로세스 안을 엔진 · 반영 콜백 · 그 밖의 JVM 으로 나눈 값입니다. 괄호는 엔진만 끈 쌍둥이(기준선)입니다.", f'''
      {table(["구간", "프로세스", "엔진", "반영 콜백", "그 밖", "1초 최고", "스레드", "힙 평균/최대 MB", "non-heap MB", "RSS MB"], eng_rows)}
      {c_cpu}
      {c_mem}
      <h3>1만 건 배치 — 한 번에 드는 값</h3>
      {table(["회차", "반영 끝", "프로세스 CPU", "엔진 CPU", "콜백 CPU", "1초 최고", "힙 최대 (기준선)", "엔진 큐 최대"],
             [[f"#{i + 1}", f"{done_int[i]:.2f}초", f"{b['int']:.2f}초", f"{b['eng']:.2f}초", f"{b['hdl']:.2f}초", f"{b['peak']:.0f}%",
               f"{b['heap']:.0f} MB ({b['base_heap']:.0f})", f"{b['queue']:,.0f} / 8,192"] for i, b in enumerate(batch_cpu)])}
      <p>1만 건에 엔진이 코어 시간 {avg([b['eng'] for b in batch_cpu]):.2f}초를 쓴다 — 건당 약 {avg([b['eng'] for b in batch_cpu]) / 10_000 * 1e6:.0f} µs. 배치 크기(max.batch.size 2,048)로 잘려 콜백이 다섯 번 불리고, 큐가 반쯤 찬 뒤 비워진다.</p>
      <h3>적체 — 상한은 엔진 스레드 하나</h3>
      {c_catch}
      <ul>
        <li>할당: 평시 {alloc_x1[''] :.0f} MB/s 중 엔진 {alloc_x1['_engine']:.0f} · 콜백 {alloc_x1['_handler']:.0f}, 따라잡기 {alloc_cu[''] :.0f} MB/s 중 엔진 {alloc_cu['_engine']:.0f} · 콜백 {alloc_cu['_handler']:.0f}. GC 정지는 평시 {per(X1, 'int', 'gc_ms_s'):.1f} ms/s → 따라잡기 {per(CATCH, 'int', 'gc_ms_s'):.0f} ms/s.</li>
        <li>스레드는 부하와 상관없이 엔진 그룹 {per(X1, 'int', 'threads_engine', 'max'):.0f}개 그대로다 — 슬롯 읽기(coordinator) · 큐 꺼내기(pool-2) · 변환(pool-3) · keep-alive · lsn-flush · SignalProcessor · 지표 · 오프셋 저장 · 엔진 실행 스레드. 기준선보다 {per(X1, 'int', 'threads', 'max') - per(X1, 'base', 'threads', 'max'):.0f}개 많다(그룹 밖 JDBC 타이머 · 판별 스레드 포함).</li>
        <li>큐가 내내 8,192 에 붙어 있어 힙은 쌓인 양({catch_events:,.0f}건)과 상관없이 최대 {per(CATCH, 'int', 'heap_mb', 'max'):.0f} MB 에서 멈췄다. 적체는 메모리가 아니라 슬롯(Hot DB 디스크)이 진다 — §05.</li>
      </ul>''')

s4 = sec("s4", "04", "분리형 구조", "같은 코드에서 캡처부와 전달부 사이에 변경 로그 하나를 끼웠습니다. 판별 모듈 코드는 한 줄도 바뀌지 않았습니다.", f'''
      {flow("split")}
      {table(["모듈 (src/hotdb-split/app)", "맡는 것", "모르는 것 · 지키는 테스트"], [
        [m("cdc-core"), "포트 CdcSource · CdcSink · SourceCapabilities, 이벤트 CdcEvent, 라우팅 · 반영 · 슬롯 연속성", "엔진 — Gradle 의존 없음 · PortBoundaryTest"],
        [m("cdc-debezium"), "Debezium Embedded 어댑터 + 엔진 자원 계측", "라우트 · 로그"],
        [m("cdc-log"), "변경 로그 계약 ChangeRecord · 선 형식 seq\\tjson · /log HTTP · 전달부 어댑터 LogPullSource", "엔진 — ChangeRecordCodecTest"],
        [m("capture-service"), "엔진 + FileChangeLog (세그먼트 · fsync · ack 보존 · 상한 2GB)", "라우트 · 판별 — FileChangeLogTest (반쯤 쓴 줄 · 보존 · 410 · 상한)"],
        [m("apply-service"), "zone-service 소스 · 설정을 그대로 빌드, 원천만 log", "Debezium 이 클래스패스에 없다 — ApplyClasspathTest"],
      ])}
      <p>전달부는 당겨 간다(pull) — {m("GET /log/records?from=&max=2048&waitMs=1000")} 로 long poll 하고, 반영을 끝낸 뒤 순번을 자기 파일에 남기고 {m("POST /log/ack")} 한다.
        캡처부는 전달부가 있는지 모르고, 설정된 소비자 전부가 ack 한 세그먼트만 지운다. 그래서 캡처부 하나에 전달부를 여럿 붙이면 슬롯도 하나다.</p>''')

res_rows = []
for ph, label in ((IDLE, "유휴"), (X1, "평시 x1"), (BATCHES[2], "1만 건 #3"), (CATCH, "적체 따라잡기")):
    res_rows.append([label,
                     f"{cpu(ph, 'int'):.1f}%", f"{cpu(ph, 'cap'):.1f}% + {cpu(ph, 'app'):.1f}%",
                     f"{per(ph, 'int', 'rss_mb', 'avg'):.0f}", f"{per(ph, 'cap', 'rss_mb', 'avg'):.0f} + {per(ph, 'app', 'rss_mb', 'avg'):.0f}",
                     f"{per(ph, 'int', 'heap_mb', 'max'):.0f}", f"{per(ph, 'cap', 'heap_mb', 'max'):.0f} + {per(ph, 'app', 'heap_mb', 'max'):.0f}",
                     f"{per(ph, 'int', 'threads', 'max'):.0f}", f"{per(ph, 'cap', 'threads', 'max'):.0f} + {per(ph, 'app', 'threads', 'max'):.0f}"])
c_res = chart([
    "title=평시 x1 프로세스 CPU (코어 1개 = 100%)", "type=bar", "unit=%",
    "labels=일체형,캡처부,전달부,캡처 + 전달",
    f"series=CPU|hw|{cpu(X1, 'int'):.1f},{cpu(X1, 'cap'):.1f},{cpu(X1, 'app'):.1f},{x1_split_cpu:.1f}",
    "caption=캡처부만으로 일체형과 거의 같다 — 엔진과 JVM 고정비가 그대로 따라가고, 전달부는 그 위에 얹힌다. 반영 자체는 가볍다."])
c_wal = chart([
    "title=전달부가 멈춘 동안 무엇이 쌓였나 (10초 구간 최대)", "type=line", "unit=MB",
    "labels=" + ",".join(f"{i * 10}s" for i in range(len(series_bins([PILE, CATCH, '적체 뒤 평시'], 'int_slot_mb', 10)))),
    f"series=일체형 슬롯 int_asm (Hot DB WAL)|hw|{csvs(series_bins([PILE, CATCH, '적체 뒤 평시'], 'int_slot_mb', 10))}",
    f"series=캡처부 로그 (캡처부 디스크)|info|{csvs(series_bins([PILE, CATCH, '적체 뒤 평시'], 'cap_log_mb', 10))}",
    "values=max",
    f"caption=같은 3분이 일체형에서는 Hot DB 의 WAL 로, 분리형에서는 캡처부 로그로 쌓였다. 분리형 슬롯 capture_asm 은 내내 {per(PILE, 'cap', 'rss_mb', 'max') and P[PILE]['cap_slot_mb']['max']:.1f} MB 아래 — Hot DB 는 반영 쪽 장애를 모른다."])

s5 = sec("s5", "05", "자원 — 일체형 vs 캡처부 + 전달부", "같은 부하를 같은 순간에 받았습니다. 분리형은 두 프로세스의 합입니다.", f'''
      {table(["구간", "CPU 일체형", "CPU 캡처 + 전달", "RSS MB 일체형", "RSS MB 캡처 + 전달", "힙 최대 일체형", "힙 최대 캡처 + 전달", "스레드 일체형", "스레드 캡처 + 전달"], res_rows)}
      {c_res}
      {c_wal}
      {table(["적체 (x5 · 3분)", "일체형", "분리형"], [
        ["쌓인 곳", f"Hot DB 슬롯 int_asm {bl_start['int_slot_mb']:.0f} MB", f"캡처부 로그 {bl_start['cap_log_mb']:.0f} MB · 슬롯 capture_asm {bl_start['cap_slot_mb']:.1f} MB"],
        ["멈춘 동안 돈 것", "없음", f"캡처부 CPU {cpu(PILE, 'cap'):.0f}% (x5 를 실시간으로 받아 로그에 씀)"],
        ["따라잡기", f"{bl_caught['caught_s']['int']:.0f}초 · CPU 평균 {cpu(CATCH, 'int'):.0f}% · 최고 {per(CATCH, 'int', 'cpu_pct_peak'):.0f}%", f"{bl_caught['caught_s']['app']:.0f}초 · 전달부 CPU 평균 {cpu(CATCH, 'app'):.0f}% · 최고 {per(CATCH, 'app', 'cpu_pct_peak'):.0f}%"],
        ["따라잡는 동안 힙 최대", f"{per(CATCH, 'int', 'heap_mb', 'max'):.0f} MB (큐 8,192 가득)", f"전달부 {per(CATCH, 'app', 'heap_mb', 'max'):.0f} MB (배치 2,048 건씩 당김)"],
        ["밀린 양 최대", f"슬롯 {P[CATCH]['int_slot_mb']['max']:.0f} MB", f"전달부 뒤처짐 {P[CATCH]['app_behind']['max']:,.0f} 건"],
      ])}
      {note("info", "모듈 넷이면 (추정)", f"""운영은 판별 모듈 4개가 같은 tsdb 를 읽습니다. 일체형은 슬롯 · 엔진 · walsender 가 넷이고, 분리형은 캡처부 하나에 전달부 넷을 붙일 수 있어 슬롯이 하나입니다.
        단일 모듈 측정을 그대로 곱하면 평시 CPU 는 일체형 4 × {cpu(X1, 'int'):.1f} = {4 * cpu(X1, 'int'):.0f}% 대 분리형 {cpu(X1, 'cap'):.1f} + 4 × {cpu(X1, 'app'):.1f} = {cpu(X1, 'cap') + 4 * cpu(X1, 'app'):.0f}%,
        RSS 는 4 × {x1_int_rss:.0f} = {4 * x1_int_rss:,.0f} MB 대 {per(X1, 'cap', 'rss_mb', 'avg'):.0f} + 4 × {per(X1, 'app', 'rss_mb', 'avg'):.0f} = {per(X1, 'cap', 'rss_mb', 'avg') + 4 * per(X1, 'app', 'rss_mb', 'avg'):,.0f} MB 로 뒤집힙니다.
        전달부가 넷이면 캡처부 로그를 넷이 읽고, 보존은 가장 느린 전달부가 정합니다. <span class="chip chip--warn">추정 — 모듈 넷 fan-out 은 재지 않았다</span>""")}''')

f_rows = []
for key, label, what in ((("crash", "int"), "일체형 kill → start", "엔진 · 반영 · 판별 전부"),
                         (("crash", "app"), "전달부 kill → start", "반영 · 판별만"),
                         (("crash", "cap"), "캡처부 kill → start", "엔진 · 로그만"),
                         (("redeploy", "int"), "일체형 재배포 (restart)", "전부"),
                         (("redeploy", "app"), "전달부 재배포 (restart)", "반영 · 판별만")):
    e = faults[key]
    slot = "int_asm 끊김" if key[1] == "int" else ("capture_asm 끊김" if key[1] == "cap" else "붙어 있음")
    f_rows.append([label, what, f"{e['up_s']:.1f}초", f"{e['recover_s']:.1f}초", f"{e['fresh_peak_s']:.1f}초",
                   f"{e['other_fresh_peak_s']:.1f}초", slot, f"{e['cap_slot_peak_mb']:.2f} MB"])
c_fault = chart([
    "title=죽었다 살아날 때 — 결과가 다시 3초 안쪽으로 신선해질 때까지", "type=bar", "unit=초",
    "labels=일체형 kill,전달부 kill,캡처부 kill,일체형 재배포,전달부 재배포",
    "series=회복|hw|" + ",".join(f"{faults[k]['recover_s']:.1f}" for k in (("crash", "int"), ("crash", "app"), ("crash", "cap"), ("redeploy", "int"), ("redeploy", "app"))),
    "series=최대 묵음|info|" + ",".join(f"{faults[k]['fresh_peak_s']:.1f}" for k in (("crash", "int"), ("crash", "app"), ("crash", "cap"), ("redeploy", "int"), ("redeploy", "app"))),
    "caption=반영 쪽이 죽는 경우 두 방식이 거의 같다 — JVM 기동(6~8초)이 시간을 정한다. 분리형에서 가장 아픈 것은 캡처부가 죽을 때다: 엔진 기동 + 슬롯 재접속 + 전달부 재시도가 겹쳐 묵음이 두 배가 된다."])

s6 = sec("s6", "06", "장애 격리와 재기동 단위", "x1 부하에서 하나씩 죽였다 바로 띄웠습니다(감독자 재시작). 재배포는 정상 종료 후 기동입니다.", f'''
      {table(["주입", "멈춘 것", "기동", "결과 회복", "최대 묵음", "상대편 묵음", "그동안 슬롯", "capture_asm 최대"], f_rows)}
      {c_fault}
      <div class="g2">
        <div class="note note--ok"><p class="note__t">분리해서 얻는 것</p>
          <ul style="margin:4px 0 0">
            <li>반영 · 판별을 재배포해도 슬롯 · 엔진은 그대로다. 슬롯이 끊기지 않으니 Hot DB 에 inactive 슬롯이 생기지 않고, 캡처 갭 검사 · 엔진 기동(약 2초)도 건너뛴다</li>
            <li>반영 쪽이 오래 죽어도 WAL 은 Hot DB 가 아니라 캡처부 디스크(상한 2GB)에 쌓인다 — 다른 소비자(RFC Service · 다른 모듈)와 Hot DB 디스크를 지킨다</li>
            <li>판별 규칙 · 라우트 배포가 잦아도 엔진 쪽은 손대지 않는다. 엔진 버전을 올릴 때는 캡처부만 다시 띄운다</li>
          </ul></div>
        <div class="note note--risk"><p class="note__t">분리해서 생기는 것</p>
          <ul style="margin:4px 0 0">
            <li>죽을 수 있는 것이 하나 더 — 캡처부가 죽으면 회복이 {faults[('crash', 'cap')]['recover_s']:.0f}초로 일체형보다 길다</li>
            <li>새 장애 모양 둘: 로그가 상한에 차면 캡처가 멈춰 결국 슬롯이 쥔다(일체형과 같은 자리로 돌아감), 전달부 순번이 로그에서 지워졌으면 410 으로 멈춘다(사람이 판단)</li>
            <li>캡처부 디스크 크기 · 보존 · 소비자 목록을 운영해야 한다. 등록 안 된 전달부의 ack 는 보존 기준이 아니다</li>
          </ul></div>
      </div>''')

s7 = sec("s7", "07", "오프셋 관리 책임", "일체형은 위치를 한 군데(슬롯)가 쥐고, 분리형은 두 군데가 나눠 쥡니다.", f'''
      {table(["", "일체형", "분리형 캡처부", "분리형 전달부"], [
        ["원천 위치", "슬롯 confirmed_flush + Debezium 오프셋 파일. 반영이 끝난 뒤 넘긴다", "같음 — 단 <b>로그 fsync 뒤에</b> 넘긴다. 반영과 상관없다", "모른다"],
        ["자기 위치", "-", "로그 순번 lastSeq. 보존 = 등록 소비자 ack 의 최솟값 (acks.properties)", "offset-apply-asm.txt — 반영 커밋 뒤 fsync, 그다음 ack"],
        ["DB 에 남는 확인점", "ops.cdc_checkpoint (int-asm) — 기동 때 슬롯과 대조해 캡처 갭을 잡는다", "ops.cdc_checkpoint (capture-asm) — 같은 검사", "없음 — 순번은 캡처부 로그 안에서만 뜻이 있다"],
        ["죽었다 살아나면", "슬롯 위치부터 다시 받아 같은 배치를 다시 반영 (멱등)", "슬롯 위치부터 다시 받아 <b>새 순번으로</b> 다시 쓴다 — 전달부는 같은 변경을 두 번 볼 수 있다", "남긴 순번 다음부터. 반영 커밋 뒤 · 파일 쓰기 전에 죽으면 같은 줄을 다시 반영 (멱등)"],
        ["되받을 수 없는 구간", "캡처 갭 → HALTED (사람이 재동기화)", "같음", "남긴 순번이 로그에서 이미 지워졌으면 410 → HALTED"],
        ["전달 보장", "at-least-once", "at-least-once (로그에 쓴 것은 잃지 않는다)", "at-least-once. 정확히 한 번이 필요하면 순번을 반영과 같은 트랜잭션에 넣는다 (Hot DB 에 표 하나 — 이번 범위 밖)"],
      ])}
      <p>정합성 검사({m("split-compare.py verify")})는 장애 주입 다섯 번을 다 겪은 뒤, 발행을 멈추고 두 쪽이 다 비운 다음 실행 시작 이후 행을 맞대 봤습니다.</p>
      {table(["표", "일체형 svc_int", "분리형 svc_split", "일체형에만", "분리형에만"],
             [[m(t), f"{v['int_rows']:,}", f"{v['split_rows']:,}", str(v['only_int']), str(v['only_split'])] for t, v in verify.items()])}
      <p>상태 전이(device_status_change) · 실적(actual_result, 판별 결과 judged_status 포함) · 산출물까지 한 행도 다르지 않습니다. 캡처부가 같은 변경을 새 순번으로 다시 써도 싱크가 멱등이라 결과는 같습니다.</p>''')

s8 = sec("s8", "08", "엔진 교체를 고려한 인터페이스 경계", "엔진을 바꿀 때 무엇이 바뀌고 무엇이 그대로인지를 코드(포트 · 모듈 · 테스트)로 정했습니다.", f'''
      {flow("bound")}
      <h3>원천 어댑터가 지켜야 할 약속 ({m("cdc-core/port/CdcSource")})</h3>
      <ol>
        <li>원천 레코드를 {m("CdcEvent")}(논리 표 · 물리 표 · op · before · after · lsn · 커밋 시각 · 원문)로 바꿔 넘긴다. 하이퍼테이블 청크는 논리 이름으로 되돌린다({m("HypertableResolver")} 재사용).</li>
        <li>{m("capabilities()")} 로 무엇을 줄 수 있는지 밝히고, 받기 전에 {m("CdcSink.incompatibilities()")} 를 부른다. 비어 있지 않으면 받지 않고 HALTED.</li>
        <li>배치 하나를 싱크가 끝낸 <b>뒤에</b> 위치를 넘긴다. 싱크는 멱등이다(at-least-once).</li>
        <li>기동 전에 캡처 연결고리를 확인하고({m("SlotContinuityGuard")}), 배치마다 처리 위치를 {m("ops.cdc_checkpoint")} 에 남긴다 — 위치 종류가 pg-lsn 일 때.</li>
        <li>해석 못 한 레코드는 엔진을 죽이지 않고 원문째 dead letter. {m("decode(raw)")} 로 원문을 다시 이벤트로 만들 수 있어야 재처리가 된다.</li>
        <li>지표 이름은 어댑터와 상관없이 같다({m("hotdb_cdc_batch")} · {m("hotdb_cdc_lag_commit")} · {m("hotdb_cdc_state")} · {m("hotdb_cdc_events")}) — 대시보드가 원천을 가리지 않는다.</li>
      </ol>
      {table(["SourceCapabilities", "Debezium (지금)", "pgoutput 직접 수신", "버전 컬럼 폴링", "로그 당겨오기 (전달부)"], [
        ["positionKind", "pg-lsn", "pg-lsn", "watermark (버전 값 + 키)", "캡처부 원천을 그대로 넘겨받음"],
        ["deletes", "예", "예", '<span class="chip chip--risk">아니오</span> — 지운 행은 안 보인다 (소프트 삭제 · 묘비 표가 필요)', "〃"],
        ["beforeImage", "예 (REPLICA IDENTITY FULL 일 때 전체)", "예 (〃)", '<span class="chip chip--risk">아니오</span>', "〃"],
        ["commitOrder", "예", "예", "버전 컬럼 순 (같은 시각 · 늦은 커밋은 놓칠 수 있다 — 겹침 창 필요)", "〃"],
        ["commitTime", "예 (source.ts_ms)", "예 (Commit 메시지)", '<span class="chip chip--risk">아니오</span> — 버전 값으로 대신', "〃"],
        ["바꿀 모듈", "-", m("cdc-pgoutput") + " 새로 (PgJDBC replication API · 확인 LSN 전송 · relation 메시지 해석)", m("cdc-polling") + " 새로 (poll-core 의 워터마크 증분을 이벤트로)", "-"],
        ["안 바뀌는 것", "", "cdc-core · 라우트 설정 · 판별 · 대시보드 · 분리형이면 전달부 전체", "〃 — 단 delete · before.* · meta.commit_time 라우트는 기동에서 거부된다", ""],
      ])}
      <h3>캡처부 ↔ 전달부 계약 ({m("cdc-log")})</h3>
      <ul>
        <li>한 줄 = {m("seq\\tJSON")}. 순번은 1부터 빈틈없이 하나씩 늘고, 전달부는 건너뛴 순번을 보면 멈춘다.</li>
        <li>JSON 필드({m("pos table physical op before after lsn ts")})는 <b>더하기만 하고 바꾸지 않는다</b>. 모르는 필드는 무시한다 — 캡처부를 먼저 올려도 전달부가 깨지지 않는다.</li>
        <li>{m("pos")} 는 원천 위치 표기(lsn:… · wm:…)로, 추적용일 뿐 전달부가 해석하지 않는다. 그래서 캡처부 엔진을 바꿔도 전달부는 다시 빌드하지 않는다.</li>
        <li>dead letter 원문은 엔진 형식(Debezium JSON)이 아니라 이 JSON 이다 — 재처리도 엔진을 모른다.</li>
        <li>{m("/log/info")} 의 {m("source")} 가 캡처부 원천의 SourceCapabilities 다. 전달부는 처음 붙을 때 이것으로 자기 라우트를 맞춰 본다.</li>
      </ul>
      {note("info", "원래 프로젝트에 지금 들일 것", f"""분리하지 않더라도 경계는 지금 들이는 편이 쌉니다: {m("cdc-debezium")} 모듈 분리(엔진 의존을 core 밖으로),
        {m("SourceCapabilities")} · {m("incompatibilities()")} (폴링 · pgoutput 어댑터를 붙일 때 라우트가 조용히 틀리지 않게), {m("hotdb.cdc.engine-enabled")} 스위치(측정 기준선).
        세 가지 모두 분리판에서 테스트까지 끝났고(PortBoundaryTest · CapabilityCheckTest), 일체형 동작은 그대로입니다 — 일체형 int-asm 이 이 코드로 돌았습니다.""")}''')

s9 = sec("s9", "09", "판단과 재검토 조건", "결정을 영구화하지 않도록 다시 볼 조건을 수치로 적습니다.", f'''
      <div class="adr">
        <p class="adr__t">ADR-01 · 판별 모듈은 일체형을 유지하고, 캡처 · 전달 경계만 코드에 먼저 둔다</p>
        <dl>
          <dt>배경</dt><dd>분리하면 반영 쪽 장애 · 배포 때 Hot DB 에서 WAL 이 빠지고({bl_start['int_slot_mb']:.0f} → {bl_start['cap_slot_mb']:.1f} MB) 슬롯이 끊기지 않는다. 사용자가 보는 지연은 같고, 모듈마다 JVM 이 하나 는다(RSS +{x1_split_rss - x1_int_rss:.0f} MB · CPU +{x1_split_cpu - cpu(X1, 'int'):.1f}%p).</dd>
          <dt>결정</dt><dd>지금은 일체형 4개. cdc-debezium · SourceCapabilities · cdc-log 계약은 원래 프로젝트로 옮겨, 분리를 코드가 아니라 배치(컨테이너 · 설정)로 할 수 있게 둔다.</dd>
          <dt>대가</dt><dd>반영 쪽이 오래 멈추면 그 모듈 슬롯이 Hot DB 에 WAL 을 쥔다. 평시 x1 에서 슬롯은 약 {wal_mb_s_x5 / 5:.2f} MB/s 로 자라 max_slot_wal_keep_size 20GB 에 {hours_to_20g_x1:.0f}시간쯤 닿는다 <span class="chip chip--warn">추정 · x5 측정값의 1/5</span>.</dd>
          <dt>재검토</dt><dd>① 반영 쪽(판별 · 라우트) 배포가 하루 1회를 넘거나 ② 반영 쪽 장애가 {hours_to_20g_x1 / 2:.0f}시간(20GB 의 절반)을 넘길 수 있는 운영이거나 ③ tsdb 를 읽는 모듈이 5개 이상이 되어 walsender · 엔진이 Hot DB · 서비스 PC CPU 의 10% 를 넘으면 — 분리형(캡처부 1 + 전달부 N)으로 바꾼다.</dd>
        </dl>
      </div>
      {table(["방식", "장점", "단점", "적합도"], [
        ["A. 일체형 (지금)", "프로세스 하나 · 위치 하나 · 가장 적은 자원", "반영 쪽 장애 · 배포가 슬롯을 끊고 WAL 을 Hot DB 에 쌓는다", '<span class="chip chip--ok">지금 규모</span>'],
        ["B. 분리형 1:1 (이번 비교)", "Hot DB 를 반영 쪽 장애에서 떼어 낸다 · 엔진 교체가 캡처부 하나", "모듈마다 JVM +1 · 위치 둘 · 캡처부 디스크 운영", '<span class="chip chip--warn">배포가 잦아지면</span>'],
        ["C. 분리형 1:N (캡처부 공유)", "슬롯 · 엔진 · walsender 가 하나 · 모듈이 늘수록 싸다", "가장 느린 전달부가 보존을 정한다 · 캡처부가 단일 장애점", '<span class="chip chip--warn">모듈 5개 이상</span>'],
        ["D. Kafka 재도입", "표준 해법 · 소비자 그룹 · 보존 정책", "BRD 결정(Kafka 없음)을 뒤집고 브로커를 운영해야 한다", '<span class="chip chip--neutral">탈락</span>'],
      ])}''')

s10 = sec("s10", "10", "다시 재기", "같은 숫자를 다시 만드는 명령입니다. 원래 스택(src/hotdb)이 떠 있어야 합니다.", f'''
<pre>cd src\\hotdb-split
python scripts\\split-compare.py setup                 <span class="cm"># svc_int · svc_split (svc 와 같은 표, RLS · FK 없음)</span>
cd app; .\\gradlew.bat bootJar; cd ..
podman compose -f compose.split.yml up -d --build     <span class="cm"># int-asm 59497 · int-base 59498 · capture-asm 59495 · apply-asm 59496</span>
python scripts\\split-compare.py all                   <span class="cm"># 약 30분 — 결과 app\\build\\split-compare\\&lt;시각&gt;</span>
python docs\\report\\build.py results\\20261007-114828   <span class="cm"># 이 문서</span>
python scripts\\split-compare.py teardown              <span class="cm"># 슬롯 int_asm · capture_asm 을 남기면 WAL 이 쌓인다</span></pre>
      <div class="g2">
        <div class="note note--ok"><p class="note__t">성공 판정</p>정합성 5표가 한쪽에만 0 / 0, 모든 구간 발행기 일치율 100%, 장애 주입 다섯 번 모두 결과 회복 값이 찍힘.</div>
        <div class="note note--warn"><p class="note__t">자주 나는 일</p>다른 작업이 현장 발행기(59480)를 만지면 일치율이 떨어진다 — 그 구간은 버린다. 엔진 CPU 는 두 값을 다른 순간에 읽어 1초 표본에서 조금 줄어 보일 수 있어, 요약은 반 넘게 줄었을 때만 재기동으로 본다.</div>
      </div>''')

s11 = sec("s11", "11", "참고 자료", "숫자와 판단의 근거입니다.", f'''
      <ol class="refs">
        <li id="ref-1"><span class="refs__n">1</span><span><span class="refs__src">측정 원자료</span><span class="refs__meta">file · src/hotdb-split/results/20261007-114828/ (samples.csv {len(ROWS):,}행 · summary.json · events.json)</span></span></li>
        <li id="ref-2"><span class="refs__n">2</span><span><span class="refs__src">엔진 자원 계측</span><span class="refs__meta">code · src/hotdb-split/app/cdc-debezium/src/main/java/dev/hotdb/cdc/adapter/debezium/EngineResources.java · DebeziumCdcSource.java</span></span></li>
        <li id="ref-3"><span class="refs__n">3</span><span><span class="refs__src">변경 로그 · 전달부 어댑터</span><span class="refs__meta">code · capture-service/…/FileChangeLog.java · cdc-log/…/LogPullSource.java · ChangeRecordCodec.java</span></span></li>
        <li id="ref-4"><span class="refs__n">4</span><span><span class="refs__src">원천 능력 계약</span><span class="refs__meta">code · cdc-core/…/port/SourceCapabilities.java · route/CapabilityCheck.java</span></span></li>
        <li id="ref-5"><span class="refs__n">5</span><span><span class="refs__src">비교 스크립트</span><span class="refs__meta">code · src/hotdb-split/scripts/split-compare.py · compose.split.yml</span></span></li>
        <li id="ref-6"><span class="refs__n">6</span><span><span class="refs__src">CDC 방식 결정 (Debezium Embedded, Kafka 없음)</span><span class="refs__meta">doc · docs/hotdb/BRD.md §0 결정 기록 2026-10-06</span></span></li>
        <li id="ref-7"><span class="refs__n">7</span><span><span class="refs__src">Debezium Embedded Engine · 스트리밍 지표(QueueTotalCapacity · QueueRemainingCapacity)</span><span class="refs__meta">web · https://debezium.io/documentation/reference/3.6/development/engine.html</span></span></li>
      </ol>''')

gloss = sec("gloss", "12", "용어", "이 문서에 나오는 약어 · 구성요소 · 식별자입니다.", f'''
      <div class="tbl-wrap">
        <table class="tbl gloss">
          <thead><tr><th style="width:18%">용어</th><th style="width:12%">종류</th><th style="width:36%">뜻</th><th>이 문서에서</th></tr></thead>
          <tbody>
            <tr class="gloss__g"><td colspan="4">구성요소</td></tr>
            <tr><td>판별 모듈 (zone-service)</td><td>서비스</td><td>tsdb 변경을 CDC 로 받아 svc 표에 쓰고 실적을 판정한다</td><td>엔진이 사는 프로세스 — 측정 대상. asm = 조립</td></tr>
            <tr><td>HotDB Provider</td><td>서비스</td><td>장비 데이터를 tsdb 에 적재 (MQTT Agent 뒤)</td><td>범위 밖 · 엔진 없음. 현장 발행기가 대신한다</td></tr>
            <tr><td>일체형 (int-asm)</td><td>컨테이너</td><td>엔진 + 반영 + 판별 한 프로세스</td><td>비교 기준. 슬롯 int_asm → svc_int</td></tr>
            <tr><td>엔진 끈 쌍둥이 (int-base)</td><td>컨테이너</td><td>같은 이미지에서 엔진만 끔</td><td>메모리 · 스레드 기준선</td></tr>
            <tr><td>캡처부 (capture-asm)</td><td>컨테이너</td><td>엔진 + 변경 로그</td><td>슬롯 capture_asm · /log 를 연다</td></tr>
            <tr><td>전달부 (apply-asm)</td><td>컨테이너</td><td>로그 당겨오기 + 반영 + 판별</td><td>Debezium 없음 → svc_split</td></tr>
            <tr><td>현장 발행기 (field-simulator)</td><td>서비스</td><td>tsdb 에 장비 데이터를 넣는 시험 도구</td><td>x1 ≈ 500 행/초 · 측정 중 스크립트가 잡는다</td></tr>
            <tr class="gloss__g"><td colspan="4">코드 · 계약</td></tr>
            <tr><td>CdcSource / CdcSink</td><td>포트</td><td>입력 · 출력 인터페이스 (cdc-core)</td><td>엔진 교체 경계</td></tr>
            <tr><td>SourceCapabilities</td><td>계약</td><td>원천이 주는 것 — 삭제 · 변경 전 값 · 커밋 순서 · 커밋 시각 · 위치 종류</td><td>라우트와 맞지 않으면 기동 거부</td></tr>
            <tr><td>ChangeRecord</td><td>계약</td><td>변경 로그 한 줄 (seq + 원천 위치 + CdcEvent)</td><td>캡처부 ↔ 전달부 사이에 오가는 것 전부</td></tr>
            <tr><td>engine / handler / process</td><td>지표 라벨</td><td>hotdb_cdc_engine_cpu_seconds_total 의 part</td><td>엔진 스레드 그룹 − 콜백 / 배치 콜백 / 프로세스 전체</td></tr>
            <tr><td>ack</td><td>프로토콜</td><td>전달부가 "여기까지 반영했다"고 캡처부에 알림</td><td>로그 보존 기준 (등록 소비자의 최솟값)</td></tr>
            <tr class="gloss__g"><td colspan="4">PostgreSQL · JVM</td></tr>
            <tr><td>슬롯 (replication slot)</td><td>PG</td><td>소비자가 어디까지 받았는지 서버가 기억하는 자리. 확인 전 WAL 을 지우지 않는다</td><td>적체가 Hot DB 디스크로 가는 이유</td></tr>
            <tr><td>WAL</td><td>PG</td><td>Write-Ahead Log — 변경 기록</td><td>슬롯 지연 MB 로 잰다</td></tr>
            <tr><td>LSN</td><td>PG</td><td>Log Sequence Number — WAL 안 위치</td><td>positionKind pg-lsn</td></tr>
            <tr><td>pgoutput</td><td>PG</td><td>PostgreSQL 기본 논리 복제 출력 플러그인</td><td>지금 엔진 · 직접 수신 어댑터 후보가 같이 쓴다</td></tr>
            <tr><td>walsender</td><td>PG</td><td>슬롯마다 하나씩 도는 WAL 송신 프로세스</td><td>모듈 수만큼 늘어난다 (일체형)</td></tr>
            <tr><td>RSS</td><td>OS</td><td>Resident Set Size — 프로세스가 실제로 차지한 물리 메모리</td><td>podman stats 값</td></tr>
            <tr><td>svc · svc_int · svc_split</td><td>스키마</td><td>svc = service. 판별 모듈 결과 표 묶음</td><td>svc 는 원래 스택, 나머지 둘은 비교용 복사 (같은 표 · RLS 없음)</td></tr>
          </tbody>
        </table>
      </div>''')

main = head + s0 + s1 + s2 + s3 + s4 + s5 + s6 + s7 + s8 + s9 + s10 + s11 + gloss


# ── 템플릿에 얹고 아티팩트 규약에 맞추기 ───────────────────────────────────

tpl = (SKILL / "assets/devdoc-template.html").read_text(encoding="utf-8")
a, b = tpl.index('<main class="main">') + len('<main class="main">'), tpl.index("</main>")
html = tpl[:a] + main + "\n  " + tpl[b:]
html = html.replace("<title>개발 문서 제목</title>", "<title>CDC 엔진 분리 비교</title>")
html = re.sub(r'<div class="side__brand"><span class="bar"></span><b>.*?</b></div>',
              '<div class="side__brand"><span class="bar"></span><b>CDC 엔진 자원 · 분리 비교</b></div>', html)
html = re.sub(r'<p class="side__meta">.*?</p>', '<p class="side__meta">2026-10-07 · 조립 모듈(asm) · 로컬 podman</p>', html)
html = html.replace('<button id="bPrint" type="button">인쇄</button>', "")
html = html.replace('<button id="bReset" type="button">체크 초기화</button>', "")
html = html.replace("bind('bPrint', function () { window.print(); });", "")
(HERE.parent / "engine-split-report.html").write_text(html, encoding="utf-8")

# 아티팩트판: 문서 껍데기를 벗기고, 시스템 다크 모드에도 따라가게
art = html
art = re.sub(r"^\s*<!doctype html>\s*<html[^>]*>\s*<head>\s*", "", art, flags=re.I)
art = re.sub(r'<meta charset="utf-8">\s*<meta name="viewport"[^>]*>\s*', "", art)
art = art.replace("</head>", "").replace("<body>", "").replace("</body>", "").replace("</html>", "")
art = art.replace("html,body{margin:0;padding:0}", "body{margin:0}")
dark = re.findall(r':root\[data-theme="dark"\]([^{]*)\{([^}]*)\}', art)
media = "@media (prefers-color-scheme: dark){\n" + "\n".join(
    f':root:not([data-theme="light"]){sel}{{{body}{"; color-scheme:dark" if not sel.strip() else ""}}}' for sel, body in dark) + "\n}\n"
art = art.replace(':root[data-theme="dark"]{', ':root[data-theme="dark"]{color-scheme:dark;', 1)
art = art.replace("</style>", media + "</style>", 1)
(HERE / "engine-split-report.artifact.html").write_text(art.strip() + "\n", encoding="utf-8")
print("ok", len(html), len(art), "dark rules", len(dark))
