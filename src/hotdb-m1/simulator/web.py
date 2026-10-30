#!/usr/bin/env python3
"""fabsim 웹 제어 — 브라우저에서 리플레이를 시작 · 일시정지 · 한 걸음 · 멈춤 · 정리 · 샘플.

  python web.py                  http://127.0.0.1:8765  (이 PC 에서만)
  python web.py --port 8800      포트 바꾸기 (Windows 예약 구간 59248~59347 은 피한다)

내릴 때는 화면 오른쪽 위 "서버 종료" 또는 Ctrl+C. 도는 리플레이는 이번 주기를 마치고 위치를 저장한 뒤 내린다.

실행 스레드 하나가 원천 · Hot DB 연결을 쥐고 fabsim.tick 을 돈다. 화면은 /api/state 를 1초마다 읽는다.
"""
import argparse
import collections
import contextlib
import datetime as dt
import io
import json
import os
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

import fabsim

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT = 'rfc-sim'
STEPS = ['15m', '1h', '6h', '1d']


class Runner:
    """리플레이 한 판. 상태는 lock 아래에서만 바꾼다."""

    def __init__(self):
        self.lock = threading.Lock()
        self.wake = threading.Condition(self.lock)
        self.thread = None
        self.state = 'idle'          # idle · running · paused · done · error
        self.cfg = None              # profile · hulls · start · end · step · speed · shift · full_every
        self.clock = None            # 다음에 돌 원천 시각
        self.last_now = None         # 마지막 주기의 Hot DB 시각
        self.pending_steps = 0       # 일시정지 중 "한 걸음" 요청 수
        self.stop_req = False
        self.log = collections.deque(maxlen=200)
        self.ticks = collections.deque(maxlen=120)   # (원천 시각, 쓴 행 합, 삭제 합, 걸린 초)
        self.busy = None             # prepare · clean · samples 같은 단발 작업 이름
        self.message = ''

    # ── 단발 작업 (실행 중에는 못 한다) ──
    def oneshot(self, name, fn):
        with self.lock:
            if self.state in ('running', 'paused'):
                raise RuntimeError('리플레이가 도는 중이다 — 먼저 멈춤')
            if self.busy:
                raise RuntimeError(f'{self.busy} 하는 중이다')
            self.busy = name
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                fn()
            out = buf.getvalue().strip()
            self.add_log(f'[{name}] ' + (out.replace('\n', ' / ') if out else '끝'))
            return out
        except SystemExit:
            out = buf.getvalue().strip()
            self.add_log(f'[{name}] 실패: {out}')
            raise RuntimeError(out or f'{name} 실패')
        finally:
            with self.lock:
                self.busy = None

    def add_log(self, s):
        self.log.appendleft(f'{dt.datetime.now():%H:%M:%S}  {s}')

    # ── 리플레이 ──
    def start(self, cfg, resume):
        with self.lock:
            if self.state in ('running', 'paused'):
                raise RuntimeError('이미 도는 중이다')
            if self.busy:
                raise RuntimeError(f'{self.busy} 하는 중이다')
            self.cfg = cfg
            self.stop_req = False
            self.pending_steps = 0
            self.ticks.clear()
            self.state = 'running' if not cfg.get('paused') else 'paused'
            self.message = ''
        self.thread = threading.Thread(target=self._loop, args=(cfg, resume), daemon=True)
        self.thread.start()

    def _loop(self, cfg, resume):
        try:
            a = argparse.Namespace(profile=cfg['profile'], hulls=cfg.get('hulls') or None, start=cfg.get('start') or None,
                                   end=cfg.get('end') or None, shift=cfg.get('shift') or 'none')
            hulls, start, end, shift = fabsim.profile_args(a)
            step = fabsim.parse_dur(cfg['step'])
            full_every = fabsim.parse_dur(cfg['full_every']) if cfg.get('full_every') else None
            clock = start
            st = fabsim.load_state(cfg['profile'])
            if resume and st:
                clock = fabsim.parse_ts(st['clock'])
            with self.lock:
                self.cfg.update(hulls=hulls, start=start.isoformat(), end=end.isoformat(), shift_days=shift)
                self.clock = clock
            with psycopg.connect(fabsim.SRC_DSN) as src, psycopg.connect(fabsim.HOT_DSN) as hot:
                if not resume and fabsim.leftover_watermark(hot, AGENT):
                    raise RuntimeError('이전 리플레이의 워터마크가 남아 있다 — "이어서"로 시작하거나 먼저 "정리"')
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    fabsim.stage(src, hulls)
                if buf.getvalue().strip():
                    self.add_log(buf.getvalue().strip())
                tgt = fabsim.Target(hot)
                jobs = fabsim.load_jobs(AGENT)
                self.add_log(f'시작 {cfg["profile"]} · 호선 {hulls} · {clock:%Y-%m-%d %H:%M} ~ {end:%Y-%m-%d} · 한 걸음 {cfg["step"]}'
                             + (f' · 날짜 +{shift}일' if shift else ''))
                while True:
                    with self.lock:
                        while self.state == 'paused' and not self.pending_steps and not self.stop_req:
                            self.wake.wait()
                        if self.stop_req:
                            break
                        if self.pending_steps:
                            self.pending_steps -= 1
                        speed = self.cfg.get('speed')
                    if clock > end:
                        break
                    t0 = time.monotonic()
                    now, stats, errors = fabsim.tick(src, hot, tgt, jobs, clock, shift, hulls, full_every, AGENT)
                    took = time.monotonic() - t0
                    w = sum(s[1] for s in stats)
                    d = sum(s[2] for s in stats)
                    parts = [f'{n.replace("hot.", "")} {x}' + (f' (삭제 {y})' if y else '') for n, x, y in stats if x or y]
                    self.add_log(f'{now:%Y-%m-%d %H:%M}  ' + (' · '.join(parts) if parts else '변경 없음') + f'  [{took:.1f}s]')
                    for n, e in errors:
                        self.add_log(f'  [{n}] 실패: {e}')
                    clock += step
                    fabsim.save_state(cfg['profile'], clock, shift, hulls)
                    with self.lock:
                        self.clock = clock
                        self.last_now = now
                        self.ticks.append((now.isoformat(), w, d, round(took, 2), len(errors)))
                    if speed:
                        rest = step.total_seconds() / speed - took
                        with self.lock:
                            if rest > 0 and self.state == 'running':
                                self.wake.wait(timeout=rest)
            with self.lock:
                self.state = 'idle' if self.stop_req else 'done'
            self.add_log('멈춤' if self.stop_req else '구간 끝')
        except Exception as e:
            with self.lock:
                self.state = 'error'
                self.message = str(e)
            self.add_log(f'오류: {e}')
            traceback.print_exc()

    def pause(self):
        with self.lock:
            if self.state == 'running':
                self.state = 'paused'
            self.wake.notify_all()

    def resume(self):
        with self.lock:
            if self.state == 'paused':
                self.state = 'running'
            self.wake.notify_all()

    def step_once(self):
        with self.lock:
            if self.state != 'paused':
                raise RuntimeError('일시정지 중에만 한 걸음씩 갈 수 있다')
            self.pending_steps += 1
            self.wake.notify_all()

    def stop(self):
        with self.lock:
            self.stop_req = True
            self.wake.notify_all()
        if self.thread:
            self.thread.join(timeout=60)

    def set_speed(self, speed):
        with self.lock:
            if self.cfg is not None:
                self.cfg['speed'] = speed
            self.wake.notify_all()


R = Runner()


def hot_snapshot():
    """Hot DB 의 지금 모습 — 작업 상태와 적재 건수."""
    out = {'jobs': [], 'counts': []}
    try:
        with psycopg.connect(fabsim.HOT_DSN, connect_timeout=3) as hot, hot.cursor() as cur:
            cur.execute("SELECT job, last_run_at, last_ok_at, last_rows, watermark, last_error FROM ops.poll_state"
                        " WHERE agent = %s ORDER BY job", (AGENT,))
            out['jobs'] = [dict(job=r[0], last_run_at=iso(r[1]), last_ok_at=iso(r[2]), last_rows=r[3], watermark=r[4], last_error=r[5])
                           for r in cur.fetchall()]
            cur.execute("""
              SELECT '도면 앵커 (l_work_unit DWG)', count(*) FILTER (WHERE src_deleted_at IS NULL), count(*) FILTER (WHERE src_deleted_at IS NOT NULL),
                     count(*) FILTER (WHERE actl_fd IS NOT NULL AND src_deleted_at IS NULL)
                FROM hot.l_work_unit WHERE level = 'DWG' AND src_job LIKE 'hot.l_work_unit.dwg%%'
              UNION ALL
              SELECT 'W/O (l_work_unit WO)', count(*) FILTER (WHERE src_deleted_at IS NULL), count(*) FILTER (WHERE src_deleted_at IS NOT NULL),
                     count(*) FILTER (WHERE actl_fd IS NOT NULL AND src_deleted_at IS NULL)
                FROM hot.l_work_unit WHERE level = 'WO' AND src_job LIKE 'hot.l_work_unit.wo%%'
              UNION ALL
              SELECT 'l_event ' || event_code || ' (' || source || ')', count(*) FILTER (WHERE src_deleted_at IS NULL),
                     count(*) FILTER (WHERE src_deleted_at IS NOT NULL), NULL
                FROM hot.l_event WHERE src_job LIKE 'hot.l_event.%%' GROUP BY event_code, source
              UNION ALL
              SELECT '부재 (l_cut_part)', count(*) FILTER (WHERE src_deleted_at IS NULL), count(*) FILTER (WHERE src_deleted_at IS NOT NULL),
                     count(*) FILTER (WHERE group_ts IS NOT NULL AND src_deleted_at IS NULL)
                FROM hot.l_cut_part WHERE src_job = 'hot.l_cut_part'
            """)
            out['counts'] = [dict(name=r[0], live=r[1], deleted=r[2], done=r[3]) for r in cur.fetchall()]
    except Exception as e:
        out['error'] = f'Hot DB 연결 실패: {e}'
    return out


def iso(v):
    return v.isoformat() if v else None


def state_json():
    with R.lock:
        cfg = dict(R.cfg) if R.cfg else None
        s = dict(state=R.state, busy=R.busy, message=R.message, cfg=cfg,
                 clock=R.clock.isoformat() if R.clock else None, last_now=iso(R.last_now),
                 log=list(R.log)[:80], ticks=list(R.ticks))
    s['hot'] = hot_snapshot()
    s['profiles'] = {k: {**v} for k, v in fabsim.PROFILES.items()}
    s['saved'] = {k: fabsim.load_state(k) for k in fabsim.PROFILES}
    s['steps'] = STEPS
    s['dsn'] = {'src': mask(fabsim.SRC_DSN), 'hot': mask(fabsim.HOT_DSN)}
    s['samples'] = sorted(f for f in os.listdir(os.path.join(HERE, 'samples'))) if os.path.isdir(os.path.join(HERE, 'samples')) else []
    return s


def mask(dsn):
    return ' '.join(p for p in dsn.split() if not p.startswith('password='))


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype='application/json; charset=utf-8'):
        b = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(b)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path in ('/', '/index.html'):
            with open(os.path.join(HERE, 'web', 'index.html'), 'rb') as f:
                return self.send(200, f.read(), 'text/html; charset=utf-8')
        if self.path == '/api/state':
            return self.send(200, state_json())
        if self.path.startswith('/samples/'):
            name = os.path.basename(self.path)
            p = os.path.join(HERE, 'samples', name)
            if name.endswith('.json') and os.path.isfile(p):
                with open(p, 'rb') as f:
                    return self.send(200, f.read())
        self.send(404, {'error': 'not found'})

    def do_POST(self):
        n = int(self.headers.get('Content-Length') or 0)
        body = json.loads(self.rfile.read(n) or b'{}')
        try:
            out = self.route(self.path, body)
            self.send(200, {'ok': True, 'out': out})
        except RuntimeError as e:
            self.send(409, {'ok': False, 'error': str(e)})
        except Exception as e:
            traceback.print_exc()
            self.send(500, {'ok': False, 'error': str(e)})

    def route(self, path, b):
        if path == '/api/start':
            cfg = {k: b.get(k) for k in ('profile', 'hulls', 'start', 'end', 'step', 'shift', 'full_every')}
            cfg['speed'] = float(b['speed']) if b.get('speed') else None
            cfg['paused'] = bool(b.get('paused'))
            if cfg['profile'] not in fabsim.PROFILES:
                raise RuntimeError('프로필을 고른다')
            if cfg['step'] not in STEPS:
                raise RuntimeError('한 걸음은 ' + ' · '.join(STEPS))
            R.start(cfg, bool(b.get('resume')))
        elif path == '/api/pause':
            R.pause()
        elif path == '/api/resume':
            R.resume()
        elif path == '/api/step':
            R.step_once()
        elif path == '/api/stop':
            R.stop()
        elif path == '/api/shutdown':
            R.stop()
            threading.Thread(target=self.server.shutdown, daemon=True).start()
            return '서버를 내린다'
        elif path == '/api/speed':
            R.set_speed(float(b['speed']) if b.get('speed') else None)
        elif path == '/api/prepare':
            return R.oneshot('준비', lambda: fabsim.cmd_prepare(argparse.Namespace()))
        elif path == '/api/clean':
            return R.oneshot('정리', lambda: fabsim.cmd_clean(argparse.Namespace(agent=AGENT)))
        elif path == '/api/samples':
            prof = b.get('profile') or 'cut'
            ns = argparse.Namespace(profile=prof, hulls=None, start=None, end=None, shift=b.get('shift') or 'none',
                                    at=b.get('at') or None, dwg=b.get('dwg') or None, limit=5, hot=True, agent=AGENT)
            return R.oneshot('샘플', lambda: fabsim.cmd_samples(ns))
        else:
            raise RuntimeError('없는 명령')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=8765)
    a = ap.parse_args()
    if 59248 <= a.port <= 59347:
        raise SystemExit('59248~59347 은 Windows 예약 구간이라 브라우저가 못 연다 — 다른 포트')
    srv = ThreadingHTTPServer((a.host, a.port), H)
    print(f'fabsim 웹 제어: http://{a.host}:{a.port}  (원천 {mask(fabsim.SRC_DSN)} · Hot DB {mask(fabsim.HOT_DSN)})', flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        R.stop()
    srv.server_close()
    print('fabsim 웹 제어: 서버 내림', flush=True)


if __name__ == '__main__':
    main()
