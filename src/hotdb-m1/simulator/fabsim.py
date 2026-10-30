#!/usr/bin/env python3
"""가공 레거시 리플레이 시뮬레이터 (M1-2.1, IPCS-304).

inshoplocal 사본(2026-08~09 추출분)을 시계 T 의 원천으로 되돌려 놓고(sim.* 뷰), RFC Service 의 가공 적재 작업 SQL 을
그대로 돌려 Hot DB 의 hot.* 에 쓴다. 쓰는 방식은 poll-core PollJob 과 같다:
  - 원천 SELECT 의 별칭 중 대상에 있는 열만, CAST(? AS 대상형) 로 UPSERT (대상 PK 충돌 시 갱신)
  - 대상에 src_job · polled_at · src_deleted_at 이 있으면 함께 쓴다 (V20 적재 추적)
  - incremental 은 ops.poll_state.watermark 이상만 읽고, 다 쓴 뒤에 워터마크를 저장한다
  - mark-missing 인 full 작업은 이번 실행에 안 온 행에 src_deleted_at 을 찍는다
다른 점은 시각뿐이다 — 실행 시각(polled_at · ops.poll_state)은 벽시계가 아니라 시뮬레이션 시각(shift 반영)이다.

  fabsim.py prepare                       inshoplocal 에 sim 스키마(함수)를 만든다. 원천 뜨기(sim._표 · sim.표 뷰)는 run 이 한다
  fabsim.py run --profile cut             시계를 돌리며 적재 (Ctrl+C 로 멈추고 --resume 으로 이어서)
  fabsim.py samples --profile cut         IF 별 입력 샘플 · 같은 시각의 hot 출력 샘플을 samples/ 에 쓴다
  fabsim.py clean                         이 시뮬레이터가 쓴 hot 행과 ops.poll_state 를 지운다
"""
import argparse
import datetime as dt
import json
import os
import re
import signal
import sys
import time

import psycopg
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
TRACK = ['src_job', 'polled_at', 'src_deleted_at']
VALID_D = r"^(19|20)[0-9]{6}$"

# 리플레이 프로필 — 사본에서 가공 데이터가 몰린 구간 (월별 밀도 실측, 2026-10-28)
PROFILES = {
    # 6065: 2023-09 ~ 12 에 절단 완료 월 2~3천 · 모듬 1~1.7만. 절단 · 전처리 · 모듬 · 사상 · 곡 · 사외 반입 · W/O
    # 샘플 도면 6065DS447CNE06 = 140M 강판 1 · 002M 모듬 24 · 821M 사상 16 · 부재 마스터(060M) 2, 절단 완료 2023-11-09
    'cut': {'hulls': '6065', 'start': '2023-09-01', 'end': '2024-03-31', 'dwg': '6065DS447CNE06'},
    # 2514: ProSSYS 강재 이력(STY01H)이 있는 호선, 2021-07 ~ 12 에 몰림. 입고 · 1·2차 선별 · 불출 (6065 는 STY01H 에 도면이 없다)
    # 샘플 도면 2514DS662CFG03 = 입고 09-13 → 불출 11-03 → 절단 완료 12-01 (순서가 맞는 도면)
    'ssy': {'hulls': '2514', 'start': '2021-07-01', 'end': '2021-12-31', 'dwg': '2514DS662CFG03'},
    # 5486: 가공(cut)과 ProSSYS(ssy)가 한 호선에 다 있다 — STY01H 3,324 · 310M 13,534 · 002M 40,888 · 060M 67,223, 모두 2022-07 ~ 10 에 몰림
    # 샘플 도면 5486DS642CNE06 = 입고 08-10 → 310M 등록 08-30 → 불출 09-05 → 마킹 · 절단 09-22, 111M 1 · 140M 2 · 002M 104 · 821M 102
    'full': {'hulls': '5486', 'start': '2022-06-01', 'end': '2022-11-30', 'dwg': '5486DS642CNE06'},
}


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def env(name, dflt):
    return os.environ.get(name, dflt)


SRC_DSN = env('FABSIM_SRC_DSN', 'host=localhost port=54433 dbname=inshoplocal user=inshoplocal password=inshoplocal')
HOT_DSN = env('FABSIM_HOT_DSN', 'host=localhost port=5432 dbname=hotdb user=rfc_service password=rfc_service')


def load_yaml(name):
    with open(os.path.join(HERE, name), encoding='utf-8') as f:
        return yaml.safe_load(f)


def parse_ts(s):
    return dt.datetime.fromisoformat(s)


def parse_dur(s):
    m = re.fullmatch(r'(\d+)\s*([smhd])', s.strip())
    if not m:
        die(f'기간 형식은 30s · 5m · 1h · 1d — {s}')
    unit = {'s': 'seconds', 'm': 'minutes', 'h': 'hours', 'd': 'days'}[m.group(2)]
    return dt.timedelta(**{unit: int(m.group(1))})


def interval_of(job):
    """interval-ms 의 ${VAR:기본값} 을 풀어 시뮬레이션 시간 간격으로."""
    v = str(job.get('interval-ms', '300000'))
    m = re.fullmatch(r'\$\{([A-Z0-9_]+):(\d+)\}', v)
    ms = int(env(m.group(1), m.group(2))) if m else int(v)
    return dt.timedelta(milliseconds=ms)


# ── prepare: sim 스키마 ───────────────────────────────────────────────────────
FUNCS = r"""
CREATE SCHEMA IF NOT EXISTS sim;
-- 시계 T(원천 시각) · 날짜 이동(일)은 세션 설정으로 받는다
CREATE OR REPLACE FUNCTION sim.clock() RETURNS timestamp LANGUAGE sql STABLE
  AS $$ SELECT current_setting('sim.clock')::timestamp $$;
CREATE OR REPLACE FUNCTION sim.shift() RETURNS int LANGUAGE sql STABLE
  AS $$ SELECT coalesce(nullif(current_setting('sim.shift_days', true), ''), '0')::int $$;
CREATE OR REPLACE FUNCTION sim.today() RETURNS date LANGUAGE sql STABLE
  AS $$ SELECT (sim.clock() + make_interval(days => sim.shift()))::date $$;
CREATE TABLE IF NOT EXISTS sim.staged (hulls text NOT NULL, staged_at timestamptz NOT NULL DEFAULT now());
-- 'YYYYMMDD' + 'HHMM' · 'HHMMSS' · 'HMMSS' → timestamp. 잘못된 날짜(0231 등)는 넘치는 대로 두고 오류를 내지 않는다
CREATE OR REPLACE FUNCTION sim.digits(t text) RETURNS text LANGUAGE sql IMMUTABLE
  AS $$ SELECT regexp_replace(coalesce(t, ''), '[^0-9]', '', 'g') $$;
CREATE OR REPLACE FUNCTION sim.hms(t text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN length(sim.digits(t)) = 4 THEN sim.digits(t) || '00'
              WHEN length(sim.digits(t)) BETWEEN 1 AND 6 THEN lpad(sim.digits(t), 6, '0') ELSE '000000' END $$;
CREATE OR REPLACE FUNCTION sim.ts(d text, t text) RETURNS timestamp LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN d ~ '^(19|20)[0-9]{6}$' THEN
           make_date(substr(d, 1, 4)::int, greatest(1, least(12, substr(d, 5, 2)::int)), 1)::timestamp
           + make_interval(days => greatest(0, substr(d, 7, 2)::int - 1),
                           hours => least(23, substr(sim.hms(t), 1, 2)::int), mins => least(59, substr(sim.hms(t), 3, 2)::int),
                           secs => least(59, substr(sim.hms(t), 5, 2)::int))
         END $$;
CREATE OR REPLACE FUNCTION sim.ts(d date) RETURNS timestamp LANGUAGE sql IMMUTABLE AS $$ SELECT d::timestamp $$;
CREATE OR REPLACE FUNCTION sim.vis(d text, t text) RETURNS boolean LANGUAGE sql STABLE
  AS $$ SELECT coalesce(sim.ts(d, t) <= sim.clock(), false) $$;
CREATE OR REPLACE FUNCTION sim.vis(d date) RETURNS boolean LANGUAGE sql STABLE
  AS $$ SELECT coalesce(d::timestamp <= sim.clock(), false) $$;
-- 계획일: 늘 보이고 날짜만 옮긴다
CREATE OR REPLACE FUNCTION sim.p(d text) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN d ~ '^(19|20)[0-9]{6}$' THEN to_char(sim.ts(d, NULL) + make_interval(days => sim.shift()), 'YYYYMMDD') ELSE d END $$;
CREATE OR REPLACE FUNCTION sim.p(d date) RETURNS date LANGUAGE sql STABLE AS $$ SELECT d + sim.shift() $$;
-- 실적일: T 를 지나야 보인다 (전에는 SAP 처럼 빈 문자열)
CREATE OR REPLACE FUNCTION sim.a(d text, t text) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN d !~ '^(19|20)[0-9]{6}$' THEN d WHEN sim.vis(d, t) THEN sim.p(d) ELSE '' END $$;
CREATE OR REPLACE FUNCTION sim.a(d date) RETURNS date LANGUAGE sql STABLE
  AS $$ SELECT CASE WHEN sim.vis(d) THEN d + sim.shift() END $$;
-- T 까지 보인 가장 늦은 시각 → 원천의 MNT_DATE · MNT_TIME (증분 작업의 워터마크)
CREATE OR REPLACE FUNCTION sim.last(VARIADIC ts timestamp[]) RETURNS timestamp LANGUAGE sql STABLE
  AS $$ SELECT max(x) FROM unnest(ts) x WHERE x <= sim.clock() $$;
CREATE OR REPLACE FUNCTION sim.mnt_date(ts timestamp) RETURNS text LANGUAGE sql STABLE
  AS $$ SELECT coalesce(to_char(ts + make_interval(days => sim.shift()), 'YYYYMMDD'), '') $$;
CREATE OR REPLACE FUNCTION sim.mnt_time(ts timestamp) RETURNS text LANGUAGE sql STABLE
  AS $$ SELECT coalesce(to_char(ts, 'HH24MISS'), '') $$;
"""


def build_view(cur, name, rule):
    schema, table = name.split('.')
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s"
                " ORDER BY ordinal_position", (schema, table))
    cols = cur.fetchall()
    if not cols:
        return None
    types = {c: ('date' if t == 'date' else 'text' if t in ('text', 'character varying', 'character') else 'other') for c, t in cols}
    plan, actual = rule.get('plan') or [], rule.get('actual') or {}
    hide, interp, appear = rule.get('hide') or {}, rule.get('interp') or {}, rule.get('appear')

    def ts(d, t=None):
        return f"sim.ts(t.{d})" if types.get(d) == 'date' else f"sim.ts(t.{d}, {('t.' + t) if t else 'NULL'})"

    def vis(d, t=None):
        return f"sim.vis(t.{d})" if types.get(d) == 'date' else f"sim.vis(t.{d}, {('t.' + t) if t else 'NULL'})"

    def blank(c):
        return "''" if types[c] == 'text' else 'NULL'

    times = [ts(*appear)] if appear else []
    times += [ts(d, t) for d, t in actual.items()]
    if not appear and actual and 'rgsr_date' in types:
        times.append(ts('rgsr_date', 'rgsr_time' if 'rgsr_time' in types else None))
    # 보간 열은 진행 중에 날마다 바뀐다 — 레거시가 일일 실적으로 누적 진척률을 고칠 때처럼 그날 0시를 수정 시각으로
    for sd, fd in interp.values():
        times.append(f"CASE WHEN {vis(sd)} AND NOT {vis(fd)} THEN date_trunc('day', sim.clock()) END")
    out = []
    for c, _ in cols:
        if c in actual:
            e = f"sim.a(t.{c})" if types[c] == 'date' else f"sim.a(t.{c}, {('t.' + actual[c]) if actual[c] else 'NULL'})"
        elif c in hide:
            h = hide[c]
            e = f"CASE WHEN {vis(*h['when'])} THEN t.{c}"
            if h.get('else_when'):
                e += f" WHEN {vis(*h['else_when'])} THEN '{h.get('else_value', '')}'"
                e += f" ELSE {blank(c)} END"
            else:
                e += f" ELSE {repr(h['else_value']) if 'else_value' in h else blank(c)} END"
        elif c in interp:
            sd, fd = interp[c]
            e = (f"CASE WHEN {vis(fd)} THEN t.{c} WHEN {vis(sd)} THEN round((t.{c} * least(1, extract(epoch FROM sim.clock() - {ts(sd)})"
                 f" / nullif(extract(epoch FROM {ts(fd)} + interval '1 day' - {ts(sd)}), 0)))::numeric, 1) ELSE 0 END")
        elif c in plan:
            e = f"sim.p(t.{c})"
        elif c in ('mnt_date', 'mnt_time') and times:
            e = f"sim.{c}(sim.last({', '.join(times)}))"
        else:
            e = f"t.{c}"
        out.append(f"{e} AS {c}")
    where = f" WHERE {vis(*appear)}" if appear else ''
    return f"CREATE VIEW sim.{table} AS SELECT {', '.join(out)} FROM sim._{table} t{where}"


INDEX_COLS = ['dwg_no', 'part_no', 'pick_ser_id', 'work_ord_no', 'prod_ord_no', 'part_plat_no', 'roll_no', 'mat_code',
              'mat_desc1', 'proj_no']


def stage(src, hulls, force=False):
    """대상 호선 행만 sim._<표> 로 뜨고 그 위에 시각 T 뷰 sim.<표> 를 만든다. 같은 호선으로 이미 떠 있으면 그대로 쓴다."""
    rules = load_yaml('sim-tables.yml')['tables']
    with src.cursor() as cur:
        cur.execute("SELECT to_regclass('sim.staged') IS NOT NULL")
        if not cur.fetchone()[0]:
            die('sim 스키마가 없습니다 — 먼저 fabsim.py prepare')
        cur.execute("SELECT hulls FROM sim.staged")
        r = cur.fetchone()
        if r and r[0] == hulls and not force:
            return
        t0 = time.monotonic()
        cur.execute("SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                    " WHERE n.nspname = 'sim' AND c.relkind IN ('r', 'v') AND c.relname <> 'staged'")
        for rel, kind in cur.fetchall():
            cur.execute(f"DROP {'VIEW' if kind == 'v' else 'TABLE'} IF EXISTS sim.{q(rel)} CASCADE")
        arr = [h for h in hulls.split(',') if h] if hulls != '*' else None
        order = [n for n, r in rules.items() if not (r or {}).get('follow')] + [n for n, r in rules.items() if (r or {}).get('follow')]
        made = []
        for name in order:
            rule = rules[name] or {}
            table = name.split('.')[1]
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
            if not cur.fetchone()[0]:
                print(f'  건너뜀 {name} — 사본에 표가 없다')
                continue
            if rule.get('follow'):
                cond, params = f"t.{rule['follow']}", ()
            elif rule.get('hull') and arr:
                cond, params = f"left({rule['hull']}, 4) = ANY (%s)", (arr,)
            else:
                cond, params = 'true', ()
            cur.execute(f"CREATE UNLOGGED TABLE sim._{table} AS SELECT * FROM {name} t WHERE {cond}", params)
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'sim' AND table_name = %s",
                        ('_' + table,))
            for (c,) in cur.fetchall():
                if c in INDEX_COLS:
                    cur.execute(f"CREATE INDEX ON sim._{table} ({c})")
            cur.execute(f"ANALYZE sim._{table}")
            cur.execute(f"SELECT count(*) FROM sim._{table}")
            made.append(f"{table} {cur.fetchone()[0]:,}")
            cur.execute(build_view(cur, name, rule))
        cur.execute("DELETE FROM sim.staged")
        cur.execute("INSERT INTO sim.staged (hulls) VALUES (%s)", (hulls,))
    src.commit()
    print(f'호선 {hulls} 원천을 떴다 ({time.monotonic() - t0:.0f}s): ' + ' · '.join(made))


def cmd_prepare(a):
    with psycopg.connect(SRC_DSN) as c, c.cursor() as cur:
        cur.execute('DROP SCHEMA IF EXISTS sim CASCADE')
        cur.execute(FUNCS)
        c.commit()
    print('sim 스키마(함수)를 만들었다 — 원천 뜨기는 run · samples 가 호선에 맞춰 한다')


# ── run: 시계 + PollJob 과 같은 적재 ──────────────────────────────────────────
class Target:
    def __init__(self, conn):
        self.c = conn
        self.meta = {}

    def table(self, name):
        if name not in self.meta:
            with self.c.cursor() as cur:
                cur.execute("SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a"
                            " WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", (name,))
                cols = dict(cur.fetchall())
                cur.execute("SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)"
                            " WHERE i.indrelid = to_regclass(%s) AND i.indisprimary ORDER BY array_position(i.indkey::int[], a.attnum::int)",
                            (name,))
                pk = [r[0] for r in cur.fetchall()]
            if not cols:
                die(f'대상 표 {name} 가 Hot DB 에 없습니다')
            self.meta[name] = (cols, pk)
        return self.meta[name]


def q(n):
    return '"' + n.replace('"', '""') + '"'


def rewrite(sql):
    sql = re.sub(r'\b(gp|am)\.', 'sim.', sql)
    return re.sub(r'\bcurrent_date\b', 'sim.today()', sql)


class Job:
    def __init__(self, spec, agent):
        self.spec = spec
        self.name = spec['name']
        self.agent = agent
        self.mode = spec.get('mode', 'incremental')
        self.scope = spec.get('scope') or self.name
        self.mark_missing = bool(spec.get('mark-missing'))
        self.interval = interval_of(spec)
        self.frm = rewrite(spec['from'])
        self.cache = None      # full 작업: 지난 실행의 키 → 행 (바뀐 행만 쓴다)
        self.last_run = None

    def run(self, src, tgt, now):
        cols_t, pk = tgt.table(self.spec['target'])
        with src.cursor() as cur:
            cur.execute(f"SELECT * FROM {self.frm} WHERE 1 = 0")
            names = [d.name for d in cur.description]
        cols = [n for n in names if n.lower() in cols_t and n.lower() not in TRACK]
        track = all(t in cols_t for t in TRACK)
        for k in pk:
            if k not in [x.lower() for x in cols]:
                die(f'[{self.name}] 키 {k} 가 원천에 없습니다')
        wm_expr = self.spec.get('watermark') if self.mode == 'incremental' else None
        with tgt.c.cursor() as cur:
            cur.execute("INSERT INTO ops.poll_state (agent, job, last_run_at) VALUES (%s, %s, %s)"
                        " ON CONFLICT (agent, job) DO UPDATE SET last_run_at = EXCLUDED.last_run_at", (self.agent, self.name, now))
            wm = None
            if wm_expr:
                cur.execute("SELECT watermark FROM ops.poll_state WHERE agent = %s AND job = %s", (self.agent, self.name))
                r = cur.fetchone()
                wm = r[0] if r else None
        tgt.c.commit()

        sel = 'SELECT ' + ', '.join(q(x) for x in cols)
        if wm_expr:
            sel += f', ({wm_expr}) AS hotdb_wm'
        sel += f' FROM {self.frm}'
        params = ()
        if wm is not None:
            sel += f' WHERE ({wm_expr}) >= %s'
            params = (wm,)
        with src.cursor() as cur:
            cur.execute(sel, params)
            rows = cur.fetchall()
        src.commit()

        n = len(cols)
        wcols = [x.lower() for x in cols] + (TRACK if track else [])
        upsert = (f"INSERT INTO {self.spec['target']} AS t ({', '.join(q(x) for x in wcols)}) VALUES ("
                  + ', '.join(f"CAST(%s AS {cols_t[x]})" for x in wcols) + f") ON CONFLICT ({', '.join(q(k) for k in pk)})")
        sets = [f"{q(x)} = EXCLUDED.{q(x)}" for x in wcols if x not in pk]
        upsert += (' DO UPDATE SET ' + ', '.join(sets)) if sets else ' DO NOTHING'
        pki = [[x.lower() for x in cols].index(k) for k in pk]

        max_wm = wm
        batch, seen = [], {}
        for r in rows:
            data = r[:n]
            if wm_expr and r[n] is not None and (max_wm is None or r[n] > max_wm):
                max_wm = r[n]
            key = tuple(data[i] for i in pki)
            seen[key] = data
            if self.mode == 'full' and self.cache is not None and self.cache.get(key) == data:
                continue  # 지난 실행과 같은 행 — PollJob 은 다시 쓰지만 값이 같으니 polled_at 만 아래에서 한 번에 고친다
            batch.append(list(data) + ([self.scope, now, None] if track else []))
        with tgt.c.cursor() as cur:
            if batch:
                cur.executemany(upsert, batch)
            if wm_expr and max_wm is not None:
                cur.execute("UPDATE ops.poll_state SET watermark = %s WHERE agent = %s AND job = %s", (max_wm, self.agent, self.name))
            deleted = 0
            if self.mark_missing:
                if self.cache is None:   # 첫 실행은 PollJob 과 같은 식 (이번 실행에 안 쓴 행)
                    cur.execute(f"UPDATE {self.spec['target']} SET src_deleted_at = %s"
                                " WHERE src_job = %s AND polled_at < %s AND src_deleted_at IS NULL", (now, self.scope, now))
                    deleted = cur.rowcount
                else:
                    gone = [k for k in self.cache if k not in seen]
                    cond = ' AND '.join(f'{q(k)} = %s' for k in pk)
                    for k in gone:
                        cur.execute(f"UPDATE {self.spec['target']} SET src_deleted_at = %s WHERE {cond} AND src_deleted_at IS NULL",
                                    (now, *k))
                        deleted += cur.rowcount
            if self.mode == 'full' and track:
                # 이번 실행에 온 행 = 같은 scope 에서 삭제 표시가 없는 행 → PollJob 처럼 polled_at 을 이번 실행 시각으로
                cur.execute(f"UPDATE {self.spec['target']} SET polled_at = %s"
                            " WHERE src_job = %s AND src_deleted_at IS NULL AND polled_at < %s", (now, self.scope, now))
            cur.execute("UPDATE ops.poll_state SET last_ok_at = %s, last_rows = %s, last_error = NULL WHERE agent = %s AND job = %s",
                        (now, len(rows), self.agent, self.name))
        tgt.c.commit()
        if self.mode == 'full':
            self.cache = seen
        self.last_run = now
        return len(batch), deleted


def profile_args(a):
    p = PROFILES[a.profile]
    hulls = a.hulls or p['hulls']
    start = parse_ts(a.start or p['start'])
    end = parse_ts(a.end or p['end'])
    if a.shift == 'none':
        shift = 0
    elif a.shift == 'today':   # 구간 끝이 오늘 근처가 되게, 요일을 지키려고 7일 단위로
        shift = (dt.date.today() - end.date()).days // 7 * 7
    else:
        shift = int(a.shift)
        if shift % 7:
            print(f'주의: shift {shift} 일은 7의 배수가 아니라 요일이 바뀐다', file=sys.stderr)
    return hulls, start, end, shift


def set_clock(src, clock, shift, hulls):
    with src.cursor() as cur:
        cur.execute("SELECT set_config('sim.clock', %s, false), set_config('sim.shift_days', %s, false), set_config('sim.hulls', %s, false)",
                    (clock.isoformat(sep=' '), str(shift), hulls))
    src.commit()


def state_path(profile):
    return os.path.join(HERE, f'.fabsim-{profile}.json')


def leftover_watermark(hot, agent):
    """워터마크는 agent · 작업마다 하나다 (RFC Service 와 같다). 남은 값이 있으면 이번 구간의 증분을 못 읽는다."""
    with hot.cursor() as cur:
        cur.execute("SELECT count(*) FROM ops.poll_state WHERE agent = %s AND watermark IS NOT NULL", (agent,))
        n = cur.fetchone()[0]
    hot.commit()
    return n


def tick(src, hot, tgt, jobs, clock, shift, hulls, full_every, agent):
    """폴링 한 주기: 시계를 T 로 두고 때가 된 작업을 돈다. → (Hot DB 시각, [(작업, 쓴 행, 삭제)], [(작업, 오류)])"""
    set_clock(src, clock, shift, hulls)
    now = clock + dt.timedelta(days=shift)  # Hot DB 에 남는 실행 시각 (shift 반영)
    stats, errors = [], []
    for j in jobs:
        every = max(j.interval, full_every) if (full_every and j.mode == 'full') else j.interval
        if j.last_run is not None and now - j.last_run < every:
            continue
        try:
            w, d = j.run(src, tgt, now)
        except psycopg.Error as e:
            src.rollback()
            hot.rollback()
            with hot.cursor() as cur:
                cur.execute("UPDATE ops.poll_state SET last_error = %s WHERE agent = %s AND job = %s", (str(e)[:500], agent, j.name))
            hot.commit()
            errors.append((j.name, str(e).splitlines()[0]))
            continue
        stats.append((j.name, w, d))
    return now, stats, errors


def load_jobs(agent, only=None):
    return [Job(j, agent) for j in load_yaml('jobs-fab.yml')['jobs'] if not only or j['name'] in only.split(',')]


def save_state(profile, clock, shift, hulls):
    json.dump({'clock': clock.isoformat(), 'shift': shift, 'hulls': hulls}, open(state_path(profile), 'w', encoding='utf-8'))


def load_state(profile):
    sp = state_path(profile)
    return json.load(open(sp, encoding='utf-8')) if os.path.exists(sp) else None


def cmd_run(a):
    hulls, start, end, shift = profile_args(a)
    step = parse_dur(a.step)
    full_every = parse_dur(a.full_every) if a.full_every else None
    jobs = load_jobs(a.agent, a.only)
    clock = start
    st = load_state(a.profile)
    if a.resume and st:
        clock = parse_ts(st['clock'])
        print(f'이어서: 시계 {clock}')
    stop = {'v': False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(v=True))
    print(f'리플레이 {a.profile}: 호선 {hulls} · 원천 {start:%Y-%m-%d} ~ {end:%Y-%m-%d} · 한 걸음 {a.step} · 날짜 +{shift}일 · agent {a.agent}')
    with psycopg.connect(SRC_DSN) as src, psycopg.connect(HOT_DSN) as hot:
        if not a.resume and leftover_watermark(hot, a.agent):
            die(f'agent {a.agent} 의 워터마크가 남아 있다 — 이어서 돌리려면 --resume, 새로 돌리려면 fabsim.py clean 먼저')
        stage(src, hulls)
        tgt = Target(hot)
        while clock <= end and not stop['v']:
            t0 = time.monotonic()
            now, stats, errors = tick(src, hot, tgt, jobs, clock, shift, hulls, full_every, a.agent)
            for name, e in errors:
                print(f'  [{name}] 실패: {e}', file=sys.stderr)
            parts = [f'{n.replace("hot.", "")} {w}' + (f' (삭제 {d})' if d else '') for n, w, d in stats if w or d]
            if parts or a.verbose:
                print(f'{now:%Y-%m-%d %H:%M}  ' + (' · '.join(parts) if parts else '변경 없음') + f'  [{time.monotonic() - t0:.1f}s]',
                      flush=True)
            clock += step   # 다음에 돌 시각을 남긴다 — --resume 은 여기서 이어간다
            save_state(a.profile, clock, shift, hulls)
            if a.speed:
                rest = step.total_seconds() / a.speed - (time.monotonic() - t0)
                if rest > 0:
                    time.sleep(rest)
    print('멈춤' if stop['v'] else '구간 끝')


# ── samples: IF 별 입력 샘플 (= RFC/DB Agent 가 넘기는 원천 행 모양) + hot 출력 ───────
def jsonable(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()
    if hasattr(v, 'is_integer') or type(v).__name__ == 'Decimal':
        return float(v)
    return v


def cmd_samples(a):
    hulls, start, end, shift = profile_args(a)
    at = parse_ts(a.at) if a.at else end
    dwg = a.dwg or PROFILES[a.profile]['dwg']
    rules = load_yaml('sim-tables.yml')['tables']
    os.makedirs(os.path.join(HERE, 'samples'), exist_ok=True)
    out = {}
    with psycopg.connect(SRC_DSN) as src:
        stage(src, hulls)
        set_clock(src, at, shift, hulls)
        with src.cursor() as cur:
            if not dwg:
                cur.execute("SELECT dwg_no FROM sim.sty01h WHERE length(dwg_no) = 14 GROUP BY dwg_no ORDER BY count(*) DESC LIMIT 1")
                dwg = cur.fetchone()[0]
            for name, rule in rules.items():
                rule = rule or {}
                f = rule.get('sample')
                if not f:
                    continue
                cur.execute(f"SELECT * FROM sim.{name.split('.')[1]} WHERE {f} LIMIT %(n)s", {'dwg': dwg, 'n': a.limit})
                cols = [d.name for d in cur.description]
                rows = [{c: jsonable(v) for c, v in zip(cols, r) if v not in (None, '')} for r in cur.fetchall()]
                label = rule.get('if', '-')
                out.setdefault(label if label.startswith('FAB-IF') else 'REF', []).append(
                    {'table': name.upper(), 'note': None if label.startswith('FAB-IF') else label, 'rows': rows})
    meta = {'profile': a.profile, 'source_clock': at.isoformat(), 'shift_days': shift, 'dwg_no': dwg, 'hulls': hulls}
    for i, tables in out.items():
        fn = os.path.join(HERE, 'samples', f'{a.profile}_{re.sub(r"[^A-Za-z0-9-]", "", i) or "etc"}.json')
        json.dump({**meta, 'if': i, 'tables': tables}, open(fn, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
        print(f'  {fn} — ' + ', '.join(f"{t['table']} {len(t['rows'])}" for t in tables))
    if a.hot:
        hot_out = {}
        with psycopg.connect(HOT_DSN) as hot, hot.cursor() as cur:
            for t, w in [('hot.l_work_unit', "unit_id = %(d)s OR parent_id = %(d)s"), ('hot.l_event', 'unit_id = %(d)s'),
                         ('hot.l_cut_part', 'dwg_no = %(d)s'), ('ops.poll_state', "agent = %(ag)s")]:
                cur.execute(f"SELECT * FROM {t} WHERE {w} ORDER BY 1, 2 LIMIT %(n)s", {'d': dwg, 'ag': a.agent, 'n': a.limit * 4})
                cols = [d.name for d in cur.description]
                hot_out[t] = [{c: jsonable(v) for c, v in zip(cols, r) if v is not None} for r in cur.fetchall()]
        fn = os.path.join(HERE, 'samples', f'{a.profile}_hot.json')
        json.dump({**meta, 'tables': hot_out}, open(fn, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
        print(f'  {fn} — ' + ', '.join(f'{k} {len(v)}' for k, v in hot_out.items()))


def cmd_clean(a):
    jobs = load_yaml('jobs-fab.yml')['jobs']
    with psycopg.connect(HOT_DSN) as hot, hot.cursor() as cur:
        for t in sorted({j['target'] for j in jobs}, key=lambda x: ['hot.l_event', 'hot.l_cut_part', 'hot.l_work_unit',
                                                                     'hot.m_location', 'hot.m_block', 'hot.m_hull'].index(x)):
            cols, _ = Target(hot).table(t)
            if 'src_job' in cols:
                cur.execute(f"DELETE FROM {t} WHERE src_job = ANY (%s)", ([j.get('scope') or j['name'] for j in jobs],))
                print(f'  {t} {cur.rowcount}행 삭제')
        # rfc_service 는 ops.poll_state 를 지울 수 없다 — 워터마크만 비워 처음부터 다시 읽게 한다
        cur.execute("UPDATE ops.poll_state SET watermark = NULL, last_rows = NULL, last_error = NULL WHERE agent = %s", (a.agent,))
        hot.commit()
    for p in PROFILES:
        if os.path.exists(state_path(p)):
            os.remove(state_path(p))
    print('정리 끝 (m_hull · m_block · m_location 처럼 적재 추적 열이 없는 표는 남는다 — 지우려면 hotdb.sh clean)')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    sub.add_parser('prepare')
    for n in ('run', 'samples', 'clean'):
        p = sub.add_parser(n)
        p.add_argument('--agent', default='rfc-sim', help='ops.poll_state.agent (RFC Service 는 rfc-service)')
        if n == 'clean':
            continue
        p.add_argument('--profile', default='cut', choices=PROFILES)
        p.add_argument('--hulls')
        p.add_argument('--start')
        p.add_argument('--end')
        p.add_argument('--shift', default='none', help='none · today · 일 수(7의 배수)')
    r = sub.choices['run']
    r.add_argument('--step', default='1h', help='한 걸음 = 폴링 한 주기의 시뮬레이션 시간')
    r.add_argument('--full-every', help='full 작업 최소 간격 (예: 6h). 없으면 interval-ms 그대로')
    r.add_argument('--speed', type=float, help='시뮬레이션 초 / 실제 초. 없으면 최대 속도')
    r.add_argument('--only', help='작업 이름 쉼표 목록')
    r.add_argument('--resume', action='store_true')
    r.add_argument('--verbose', action='store_true')
    s = sub.choices['samples']
    s.add_argument('--at', help='원천 시각 (기본: 구간 끝)')
    s.add_argument('--dwg')
    s.add_argument('--limit', type=int, default=5)
    s.add_argument('--hot', action='store_true', help='같은 도면의 hot 출력도 함께')
    a = ap.parse_args()
    {'prepare': cmd_prepare, 'run': cmd_run, 'samples': cmd_samples, 'clean': cmd_clean}[a.cmd](a)


if __name__ == '__main__':
    main()
