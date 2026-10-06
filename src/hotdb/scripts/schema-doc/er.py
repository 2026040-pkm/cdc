#!/usr/bin/env python3
"""컬럼 단위 ER 그림 (인라인 SVG) — TSDB field data · RDB field data · SAP 쪽 새 표.

    python er.py tsdb > out/er-tsdb.html
    python er.py rdb  > out/er-rdb.html
    python er.py sap  > out/er-sap.html
    python er.py --css > out/er.css

컬럼 · 타입 · PK 는 make.sh 가 기동 중인 DB 에서 뽑은 out/cols.txt · pk.txt · sapz.txt 에서 온다.
표 배치와 선(FK · CDC 라우트 · 같은 키)은 아래 LAYOUT 에 있다 — 표가 늘면 여기를 고친다.
"""
import html
import sys
from collections import defaultdict
from pathlib import Path

D = Path(__file__).parent
O = D / "out"
E = html.escape

W, HEAD, ROW = 300, 30, 22          # 표 폭 · 머리 높이 · 행 높이


def load():
    cols, pk = defaultdict(list), defaultdict(set)
    for f in ("cols.txt", "sapz.txt"):
        p = O / f
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                parts = line.split("|")
                if len(parts) >= 6:
                    cols[f"{parts[0]}.{parts[1]}"].append((parts[3], parts[4], parts[5] == "YES"))
    for f in ("pk.txt", "sapz_pk.txt"):
        p = O / f
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                s, t, c = line.split("|")
                pk[f"{s}.{t}"].add(c)
    return cols, pk


COLS, PK = load()
SHORT = {"timestamp with time zone": "timestamptz", "timestamp without time zone": "timestamp",
         "character varying(100)": "varchar(100)", "character varying(36)": "varchar(36)",
         "double precision": "float8", "boolean": "bool"}

# 표 머리 색 — 칸(그림) 기준
TONE = {"tsdb": "field", "svc": "rdb", "erpsrc": "sap", "ops": "ops"}


class Er:
    def __init__(self):
        self.tables = {}      # key → (x, y, title, sub, cols[(name, type, pk, fk, note)], tone)
        self.edges = []       # (from_tbl, from_col, to_tbl, to_col, kind, label, sides)

    def table(self, key, x, y, title=None, sub="", only=None, notes=None, fks=None, tone=None, src=None):
        src = src or key
        notes, fks = notes or {}, fks or {}
        rows = []
        for name, typ, _nul in COLS[src]:
            if only and name not in only:
                continue
            rows.append((name, SHORT.get(typ, typ), name in PK[src], fks.get(name, ""), notes.get(name, "")))
        if only:
            rows.sort(key=lambda r: only.index(r[0]))
        self.tables[key] = (x, y, title or key, sub, rows, tone or TONE.get(src.split(".")[0], "ops"))

    def ext(self, key, x, y, title, sub, rows, tone):
        """DB 메타데이터가 아니라 손으로 적는 작은 표 (다른 칸의 참조 대상 등)"""
        self.tables[key] = (x, y, title, sub, rows, tone)

    def edge(self, a, ac, b, bc, kind="fk", label="", sides="lr"):
        self.edges.append((a, ac, b, bc, kind, label, sides))

    def _anchor(self, key, col, side):
        x, y, _, _, rows, _ = self.tables[key]
        h = HEAD + ROW * len(rows)
        if col is None:            # 표 머리
            cy = y + HEAD / 2
        else:
            i = [r[0] for r in rows].index(col)
            cy = y + HEAD + ROW * i + ROW / 2
        return {"l": (x, cy), "r": (x + W, cy), "t": (x + W / 2, y), "b": (x + W / 2, y + h)}[side]

    def render(self, caption=""):
        w = max(x + W for x, *_ in self.tables.values()) + 20
        h = max(y + HEAD + ROW * len(r) for x, y, _, _, r, _ in self.tables.values()) + 20
        out = [f'<figure class="er"><svg viewBox="0 0 {w} {h}" role="img" aria-label="{E(caption)}">',
               '<defs><marker id="er-a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
               '<path d="M0,0 L10,5 L0,10 z" class="er-ah"/></marker>'
               '<marker id="er-o" viewBox="0 0 10 10" refX="5" refY="5" markerWidth="6" markerHeight="6">'
               '<circle cx="5" cy="5" r="3.2" class="er-dot"/></marker></defs>']
        # 선 먼저 (표 아래로)
        for a, ac, b, bc, kind, label, sides in self.edges:
            (x1, y1), (x2, y2) = self._anchor(a, ac, sides[0]), self._anchor(b, bc, sides[1])
            if sides in ("lr", "rl", "ll", "rr"):
                dx = 60 if sides[0] == "r" else -60
                ex = 60 if sides[1] == "r" else -60
                if sides in ("ll", "rr"):
                    dx = ex = (70 if sides[0] == "r" else -70)
                path = f"M{x1},{y1} C{x1 + dx},{y1} {x2 + ex},{y2} {x2},{y2}"
            else:
                dy = 50 if sides[0] == "b" else -50
                ey = 50 if sides[1] == "b" else -50
                path = f"M{x1},{y1} C{x1},{y1 + dy} {x2},{y2 + ey} {x2},{y2}"
            out.append(f'<path d="{path}" class="er-e er-e--{kind}" marker-start="url(#er-o)" marker-end="url(#er-a)"/>')
            if label:
                mx, my = (x1 + x2) / 2, (y1 + y2) / 2
                lw = 8 + 7.2 * len(label)
                out.append(f'<g class="er-lb er-lb--{kind}"><rect x="{mx - lw / 2}" y="{my - 10}" width="{lw}" height="20" rx="10"/>'
                           f'<text x="{mx}" y="{my + 4}" text-anchor="middle">{E(label)}</text></g>')
        # 표
        for key, (x, y, title, sub, rows, tone) in self.tables.items():
            hgt = HEAD + ROW * len(rows)
            out.append(f'<g class="er-t er-t--{tone}"><rect x="{x}" y="{y}" width="{W}" height="{hgt}" rx="8" class="er-box"/>'
                       f'<path d="M{x},{y + 8} a8,8 0 0 1 8,-8 h{W - 16} a8,8 0 0 1 8,8 v{HEAD - 8} h-{W} z" class="er-hd"/>'
                       f'<text x="{x + 10}" y="{y + 20}" class="er-tt">{E(title)}</text>'
                       f'<text x="{x + W - 10}" y="{y + 20}" text-anchor="end" class="er-ts">{E(sub)}</text>')
            for i, (name, typ, pk, fk, note) in enumerate(rows):
                ry = y + HEAD + ROW * i
                if i % 2:
                    out.append(f'<rect x="{x + 1}" y="{ry}" width="{W - 2}" height="{ROW}" class="er-zebra"/>')
                badge = ""
                bx = x + 8
                if pk:
                    badge += f'<rect x="{bx}" y="{ry + 5}" width="20" height="12" rx="3" class="er-pk"/><text x="{bx + 10}" y="{ry + 14.5}" text-anchor="middle" class="er-bt">PK</text>'
                    bx += 23
                if fk:
                    badge += f'<rect x="{bx}" y="{ry + 5}" width="20" height="12" rx="3" class="er-fk"/><text x="{bx + 10}" y="{ry + 14.5}" text-anchor="middle" class="er-bt er-bt--fk">FK</text>'
                    bx += 23
                tx = x + 56 if (pk or fk) else x + 12
                tx = max(tx, bx + 4)
                out.append(badge + f'<text x="{tx}" y="{ry + 15}" class="er-c{" er-c--pk" if pk else ""}">{E(name)}</text>'
                           f'<text x="{x + W - 10}" y="{ry + 15}" text-anchor="end" class="er-ty">{E(note or typ)}</text>')
            out.append("</g>")
        out.append("</svg>")
        if caption:
            out.append(f'<figcaption>{caption}</figcaption>')
        out.append("</figure>")
        return "\n".join(out)


CSS = """
.er{margin:12px 0 18px;padding:10px;border:1px solid var(--line);border-radius:var(--radius);background:var(--bg2);overflow-x:auto}
.er svg{display:block;width:100%;min-width:760px;height:auto;font-family:var(--font)}
.er figcaption{font-size:12.5px;color:var(--ink3);margin-top:6px}
.er-box{fill:var(--bg);stroke:var(--line2);stroke-width:1.2}
.er-zebra{fill:var(--bg2)}
.er-hd{stroke:none}
.er-t--field .er-hd{fill:var(--hw-tint)} .er-t--field .er-box{stroke:var(--hw)}
.er-t--rdb .er-hd{fill:var(--ok-bg)} .er-t--rdb .er-box{stroke:var(--ok)}
.er-t--sap .er-hd{fill:var(--info-bg)} .er-t--sap .er-box{stroke:var(--info)}
.er-t--ops .er-hd{fill:var(--bg3)}
.er-t--ext .er-hd{fill:var(--bg3)} .er-t--ext .er-box{stroke-dasharray:5 3}
.er-tt{font-family:var(--mono);font-weight:700;font-size:13.5px;fill:var(--ink)}
.er-ts{font-size:11px;fill:var(--ink3)}
.er-c{font-family:var(--mono);font-size:12px;fill:var(--ink)}
.er-c--pk{font-weight:700}
.er-ty{font-family:var(--mono);font-size:10.5px;fill:var(--ink3)}
.er-pk{fill:var(--hw)} .er-fk{fill:none;stroke:var(--hw-dk);stroke-width:1.2}
.er-bt{font-size:8.5px;font-weight:700;fill:#fff;font-family:var(--font)} .er-bt--fk{fill:var(--hw-dk)}
.er-e{fill:none;stroke-width:1.6}
.er-e--fk{stroke:var(--ink3)}
.er-e--cdc{stroke:var(--hw);stroke-width:2}
.er-e--send{stroke:var(--info);stroke-width:2}
.er-e--same{stroke:var(--ink4);stroke-dasharray:5 4}
.er-ah{fill:var(--ink3)} .er-dot{fill:var(--bg);stroke:var(--ink3);stroke-width:1.4}
.er-lb rect{fill:var(--bg);stroke:var(--line2)} .er-lb text{font-size:11px;fill:var(--ink2);font-family:var(--font)}
.er-lb--cdc rect{stroke:var(--hw)} .er-lb--send rect{stroke:var(--info)}
.er-legend{display:flex;flex-wrap:wrap;gap:14px;font-size:12px;color:var(--ink3);margin:-8px 0 14px}
.er-legend i{display:inline-block;width:22px;height:0;border-top:2px solid;vertical-align:middle;margin-right:5px}
"""

LEGEND = ('<div class="er-legend"><span><i style="border-color:var(--ink3)"></i>FK (자식 → 부모)</span>'
          '<span><i style="border-color:var(--hw)"></i>CDC 로 옮김 (라우트)</span>'
          '<span><i style="border-color:var(--info)"></i>SAP 로 보냄</span>'
          '<span><i style="border-top-style:dashed;border-color:var(--ink4)"></i>같은 키 (FK 없음)</span>'
          '<span><b style="color:var(--hw)">PK</b> 기본키 · <b style="color:var(--hw-dk)">FK</b> 외래키 · 오른쪽 글자 = 타입 또는 값 출처</span></div>')


def tsdb():
    e = Er()
    e.table("tsdb.device", 20, 20, sub="장비 마스터 · 일반 표")
    e.table("tsdb.tag_catalog", 420, 20, sub="태그 등록부 · 일반 표", fks={"site": 1, "device_id": 1})
    e.table("tsdb.status_history", 20, 330, sub="하이퍼 · 6h · 1초", fks={"tag_key": 1})
    e.table("tsdb.actual_history", 360, 330, sub="하이퍼 · 1d · 1분", fks={"tag_key": 1})
    e.table("tsdb.artifact_history", 700, 330, sub="하이퍼 · 1d", fks={"tag_key": 1})
    e.edge("tsdb.tag_catalog", "device_id", "tsdb.device", "device_id", "fk", "(site, device_id)", "lr")
    e.edge("tsdb.status_history", None, "tsdb.tag_catalog", "active", "fk", "tag_key", "tb")
    e.edge("tsdb.actual_history", None, "tsdb.tag_catalog", "active", "fk", "", "tb")
    e.edge("tsdb.artifact_history", None, "tsdb.tag_catalog", "active", "fk", "", "tb")
    e.edge("tsdb.actual_history", "scan_id", "tsdb.artifact_history", "scan_id", "same", "scan_id", "rl")
    return LEGEND + e.render("TSDB field data (스키마 tsdb) — 이력 세 표는 tag_key 하나로 태그 등록부에 매달리고, 태그 등록부가 장비 마스터에 매달린다. "
                             "실적과 산출물은 FK 없이 scan_id 로 같은 스캔을 가리킨다.")


def rdb():
    e = Er()
    e.ext("tsdb.device", 20, 120, "tsdb.device", "다른 칸 · 장비 마스터",
          [("site", "text", True, "", ""), ("device_id", "text", True, "", ""), ("zone", "text", False, "", "모듈을 가르는 값")], "ext")
    e.table("svc.device_status_current", 380, 20, "device_status_current", "장비당 1행", src="svc.device_status_current",
            fks={"site": 1, "device_id": 1})
    e.table("svc.device_status_change", 760, 20, "device_status_change", "바뀔 때만", src="svc.device_status_change",
            fks={"device_id": 1})
    e.table("svc.scan", 380, 330, "scan", "스캔 1건 · 실적 · 산출물의 부모", src="svc.scan", fks={"site": 1, "device_id": 1})
    e.table("svc.actual_result", 760, 330, "actual_result", "COMPLETE · 판정", src="svc.actual_result", fks={"scan_id": 1})
    e.table("svc.artifact", 760, 640, "artifact", "산출물 위치", src="svc.artifact", fks={"scan_id": 1})
    e.edge("svc.device_status_current", "device_id", "tsdb.device", "device_id", "fk", "", "lr")
    e.edge("svc.scan", "device_id", "tsdb.device", "device_id", "fk", "", "lr")
    e.edge("svc.device_status_change", "device_id", "svc.device_status_current", "device_id", "fk", "", "lr")
    e.edge("svc.actual_result", "scan_id", "svc.scan", "scan_id", "fk", "", "lr")
    e.edge("svc.artifact", "scan_id", "svc.scan", "scan_id", "fk", "scan_id", "lr")
    return LEGEND + e.render("RDB field data (스키마 svc 하나, 모든 표에 module 컬럼 · 행 수준 보안으로 모듈별 분리). 모듈 안 FK 는 커밋 때 검사(DEFERRABLE) — "
                             "한 배치 안 순서가 섞여도 된다. 장비 마스터 FK 는 다른 칸(tsdb.device)의 (site, device_id) 를 가리킨다.")


def sap():
    e = Er()
    e.table("tsdb.status_history", 20, 20, sub="TSDB", only=["event_time", "tag_key", "status", "error_code", "received_at"])
    e.table("tsdb.actual_history", 20, 200, sub="TSDB", only=["event_time", "tag_key", "scan_id", "event_type", "hull_no",
                                                               "block_id", "block_progress_rate", "match_confidence", "received_at"])
    e.table("tsdb.artifact_history", 20, 460, sub="TSDB", only=["event_time", "tag_key", "scan_id", "artifact_type",
                                                                 "segment_key", "storage_uri", "received_at"])
    e.table("svc.device_status_current", 400, 20, "device_status_current", "RDB", src="svc.device_status_current",
            only=["device_id", "status", "src_event_time", "applied_at"])
    e.table("svc.scan", 400, 160, "scan", "RDB", src="svc.scan", only=["scan_id", "device_id", "last_event_type", "completed_at"])
    e.table("svc.actual_result", 400, 300, "actual_result", "RDB · SAP 로 가는 표", src="svc.actual_result",
            notes={"judged_status": "PENDING → 판별 로직"})
    e.table("svc.artifact", 400, 610, "artifact", "RDB", src="svc.artifact", only=["scan_id", "artifact_type", "segment_key", "storage_uri"])
    e.table("ops.rfc_sent", 780, 20, "ops.rfc_sent", "보낸 기록 · 한 번만", notes={"sent_at": "보낸 시각"})
    e.table("erpsrc.zhotdb_actual_result", 780, 300, "zhotdb_actual_result", "SAP 새 표 (Z)",
            notes={"zone": "svc.actual_result.module", "sent_at": "SAP 기록 시각"})
    # CDC 라우트 (tsdb → RDB)
    e.edge("tsdb.status_history", "status", "svc.device_status_current", "status", "cdc", "status-current", "rl")
    e.edge("tsdb.actual_history", "scan_id", "svc.scan", "scan_id", "cdc", "scan", "rl")
    e.edge("tsdb.actual_history", "hull_no", "svc.actual_result", "hull_no", "cdc", "COMPLETE 만", "rl")
    e.edge("tsdb.artifact_history", "scan_id", "svc.artifact", "scan_id", "cdc", "artifact", "rl")
    # SAP 송신 (RDB → SAP Z) — 컬럼마다
    for c in ["hull_no", "block_id", "scan_id", "judged_status", "device_id", "completed_at", "block_progress_rate",
              "match_confidence"]:
        e.edge("svc.actual_result", c, "erpsrc.zhotdb_actual_result", c, "send", "", "rl")
    e.edge("svc.actual_result", "hull_no", "ops.rfc_sent", "hull_no", "same", "", "rl")
    return LEGEND + e.render("tsdb → RDB → SAP 로 컬럼이 옮겨 가는 길. 주황 = 판별 모듈의 CDC 라우트, 파랑 = RFC Service 가 SAP Z 표에 쓰는 컬럼. "
                             "zone 은 원천 행의 module 컬럼에서, sent_at 은 SAP 쪽 시각. "
                             "점선 = RFC Service 가 SAP 에 쓰기 전에 ops.rfc_sent 에 먼저 기록하고, 이미 있으면 보내지 않는다. Z 표와 ops.rfc_sent 의 PK(zone · hull_no · block_id · scan_id · judged_status)가 같은 실적을 두 번 쓰지 않게 막는다.")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg == "--css":
        print(CSS)
    else:
        print({"tsdb": tsdb, "rdb": rdb, "sap": sap}[arg]())
