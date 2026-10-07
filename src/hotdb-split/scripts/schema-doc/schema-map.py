#!/usr/bin/env python3
"""Hot DB 전체 스키마 지도 (HTML 조각 + CSS) — 브리핑 덱과 스키마 문서가 같이 쓴다.

    python schema-map.py > out/schema-map.html

표 목록 · 컬럼 수 · 레거시 표 수는 make.sh 가 기동 중인 DB 에서 뽑은 out/*.txt 에서 읽는다.
배치(그림의 네 칸 · 쓰는 쪽 · publication)는 아래 상수에 있다 — 스키마가 늘면 여기를 고친다.
"""
import html
from collections import Counter, defaultdict
from pathlib import Path

D = Path(__file__).parent
O = D / "out"
E = html.escape

ncols = Counter()
for line in (O / "cols.txt").read_text(encoding="utf-8").splitlines():
    s, t = line.split("|")[:2]
    ncols[f"{s}.{t}"] += 1
legacy = defaultdict(list)
for line in (O / "legacy.txt").read_text(encoding="utf-8").splitlines():
    s, t, n, p = line.split("|")
    legacy[s].append(t)

CSS = """
.smap{border:2px solid var(--info);border-radius:14px;background:var(--info-bg);padding:12px 14px 10px;font-size:12.5px;color:var(--ink)}
.smap__hd{display:flex;align-items:baseline;gap:10px;margin:0 0 10px}
.smap__hd b{font-size:15px}
.smap__hd .sp{flex:1}
.smap__hd span{color:var(--ink3);font-size:12px}
.smap__row{display:grid;gap:10px;margin-bottom:10px}
.smap__row--4{grid-template-columns:1fr 1fr 1.15fr 1.25fr}
.smap__row--3{grid-template-columns:1.6fr 1fr .8fr}
.sbox{background:var(--bg);border:1px solid var(--line2);border-radius:10px;padding:8px 10px;box-shadow:var(--shadow);position:relative;min-width:0}
.sbox--cdc{border-top:4px solid var(--hw)}
.sbox--multi{box-shadow:4px 4px 0 -1px var(--bg),4px 4px 0 0 var(--line2),8px 8px 0 -1px var(--bg),8px 8px 0 0 var(--line2),var(--shadow)}
.sbox--sys{background:var(--bg2);border-style:dashed}
.sbox__k{font-size:10.5px;letter-spacing:.06em;text-transform:uppercase;color:var(--ink3);font-weight:700}
.sbox__n{font-family:var(--mono);font-weight:700;font-size:14px;margin:1px 0 4px;display:flex;gap:6px;align-items:baseline;flex-wrap:wrap}
.sbox__n small{font-family:var(--font);font-weight:600;font-size:11px;color:var(--ink3)}
.sbox ul{list-style:none;margin:4px 0 6px;padding:0}
.sbox li{display:flex;gap:6px;align-items:baseline;padding:2px 0;border-bottom:1px dotted var(--line)}
.sbox li:last-child{border-bottom:0}
.sbox li .t{font-family:var(--mono);font-size:11.5px;white-space:nowrap}
.sbox li .d{color:var(--ink3);font-size:10.5px;margin-left:auto;text-align:right}
.sbox li .fk{color:var(--hw-dk);font-family:var(--mono);font-size:10.5px;white-space:nowrap}
.sbox li.ind{padding-left:12px}
.sbox__w{display:flex;flex-wrap:wrap;gap:4px;margin-top:4px}
.sbox__w span{font-size:10.5px;border:1px solid var(--line2);border-radius:999px;padding:1px 7px;background:var(--bg2);white-space:nowrap}
.sbox__w span.w{border-color:var(--hw-tint);background:var(--hw-pale);color:var(--hw-dp)}
.sbox__w span.c{border-color:var(--info);color:var(--info);background:var(--info-bg)}
.smap__pub{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.smap__pub div{background:var(--bg);border:1px dashed var(--hw);border-radius:8px;padding:6px 10px;font-size:11.5px}
.smap__pub b{font-family:var(--mono)}
.smap__pub .ar{color:var(--hw-dk);font-weight:700;margin:0 4px}
"""


def li(name, desc="", fk="", ind=False):
    cols = ncols.get(name)
    full = f"{desc} · {cols}열" if cols and desc else (f"{cols}열" if cols else desc)
    short = name.split(".", 1)[1]
    return (f'<li class="{"ind" if ind else ""}"><span class="t">{E(short)}</span>'
            + (f'<span class="fk">→ {E(fk)}</span>' if fk else "")
            + f'<span class="d">{E(full)}</span></li>')


def box(kicker, schema, extra, items, writers, cls=""):
    w = "".join(f'<span class="{c}">{E(t)}</span>' for c, t in writers)
    return (f'<div class="sbox {cls}"><div class="sbox__k">{E(kicker)}</div>'
            f'<div class="sbox__n">{schema}<small>{E(extra)}</small></div><ul>{"".join(items)}</ul>'
            f'<div class="sbox__w">{w}</div></div>')


def legacy_items(schema, picks, more_label):
    names = legacy[schema]
    out = [f'<li><span class="t">{E(t)}</span><span class="d">{E(d)}</span></li>' for t, d in picks if t in names]
    rest = len(names) - len([p for p, _ in picks if p in names])
    if rest > 0:
        out.append(f'<li><span class="t">…</span><span class="d">{E(more_label.format(rest))}</span></li>')
    return out


def render():
    n_legacy = sum(len(v) for v in legacy.values())
    erp = box("Legacy RDB sap", "erp", f"{len(legacy['erp'])}표",
             legacy_items("erp", [("item", "품목 · 폴링"), ("part", "부품 · 폴링"), ("order_line", "주문 행 · 폴링 10초")], "{}표 더 · 시험용"),
             [("w", "RFC Service ① 폴링"), ("", "계정 rfc_agent"), ("", "원천 SAP")])
    ora = box("Legacy RDB oracle", "mes · lgs · geo",
              f"{len(legacy['mes'])} · {len(legacy['lgs'])} · {len(legacy['geo'])}표",
              legacy_items("mes", [("work_log", "작업 기록 · 폴링"), ("alarm", "설비 경보 · 폴링")], "mes {}표 더 · 시험용")
              + legacy_items("lgs", [("shipment", "출하 · 폴링"), ("tracking", "위치 추적 · 폴링 10초")], "lgs {}표 더")
              + legacy_items("geo", [("zone_area", "구역 지도")], ""),
              [("w", "DB Agent 폴링"), ("", "계정 db_agent"), ("", "원천 Oracle")])
    tsdb = box("TSDB field data", "tsdb", "필드 데이터",
               [li("tsdb.device", "장비 마스터 · zone"), li("tsdb.tag_catalog", "tag_key", "device"),
                li("tsdb.status_history", "하이퍼 6h · 1초", "tag"), li("tsdb.actual_history", "하이퍼 1d · 1분", "tag"),
                li("tsdb.artifact_history", "하이퍼 1d · 스캔당 12", "tag")],
               [("w", "HotDB Provider (대역: 발행기)"), ("", "계정 hotdb_provider"), ("c", "tsdb_cdc_pub")], "sbox--cdc")
    svc = box("RDB field data", "svc_&lt;모듈&gt;", "× 4 — mch · asm · oft · pnt",
              [li("svc.device_status_current", "장비당 1행", "tsdb.device"),
               li("svc.device_status_change", "바뀔 때만", "current", ind=True),
               li("svc.scan", "스캔 1건", "tsdb.device"),
               li("svc.actual_result", "COMPLETE · 판정", "scan", ind=True),
               li("svc.artifact", "산출물 위치", "scan", ind=True)],
              [("w", "실적 판별 모듈 (CDC)"), ("", "계정 svc_<모듈>"), ("c", "svc_cdc_pub = actual_result")],
              "sbox--cdc sbox--multi")
    ops = box("운영", "ops", "CDC · 폴링 · 모듈 상태",
              [li("ops.module", "모듈 등록부 · provision_module"), li("ops.poll_state", "폴링 워터마크 · 오류"),
               li("ops.rfc_sent", "SAP 로 보낸 실적"), li("ops.cdc_heartbeat", "슬롯 전진"),
               li("ops.cdc_dead_letter", "반영 못 한 이벤트"), li("ops.flyway_schema_history", "마이그레이션 V1~V8")],
              [("", "판별 모듈 · RFC Service · 에이전트 · 관리자")])
    internal = box("TimescaleDB 내부", "_timescaledb_internal", "직접 쓰지 않음",
                   ['<li><span class="t">_hyper_N_M_chunk</span><span class="d">이력의 실제 행 · CDC 로 나감</span></li>',
                    '<li><span class="t">*_compressed</span><span class="d">7일 뒤 압축 · CDC 가 버림</span></li>',
                    '<li><span class="t">bgw_*</span><span class="d">잡 통계</span></li>'],
                   [("c", "tsdb_cdc_pub (스키마째)")], "sbox--sys")
    pub = box("확장", "public", "",
              ['<li><span class="t">timescaledb</span><span class="d">함수 · CREATE 금지</span></li>'], [], "sbox--sys")
    return f"""<div class="smap">
  <div class="smap__hd"><b>Hot DB</b><span>PostgreSQL 17 + TimescaleDB 2.29 · DB hotdb · 스키마 10 · 표 {5 + 5 * 4 + 6 + n_legacy}</span><span class="sp"></span><span>주황 띠 = CDC 원천 · 겹친 카드 = 모듈 수만큼 · → = FK (부모)</span></div>
  <div class="smap__row smap__row--4">{erp}{ora}{tsdb}{svc}</div>
  <div class="smap__row smap__row--3">{ops}{internal}{pub}</div>
  <div class="smap__pub">
    <div><b>tsdb_cdc_pub</b> <span class="ar">→</span> 슬롯 <b>zone_mch · zone_asm · zone_oft · zone_pnt</b> <span class="ar">→</span> 실적 판별 모듈 → svc_&lt;모듈&gt;</div>
    <div><b>svc_cdc_pub</b> <span class="ar">→</span> 슬롯 <b>rfc_service</b> <span class="ar">→</span> RFC Service ② → SAP Z 표 (ops.rfc_sent 로 한 번만)</div>
  </div>
</div>"""


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--css":
        print(CSS)
    else:
        print(render())
