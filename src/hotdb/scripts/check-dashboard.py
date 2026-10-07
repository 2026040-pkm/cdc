#!/usr/bin/env python3
"""대시보드(dashboards/*.json 전부) 쿼리를 Prometheus 에 직접 던져 빈 패널을 찾는다.

    python scripts/check-dashboard.py [http://localhost:59490]

지표 이름을 바꾸거나 exporter 쿼리를 고친 뒤에 돌린다. 빈 쿼리가 있으면 종료 코드 1.
(dead letter · 재시도처럼 평소 0 이라 시계열이 없는 것도 빈 것으로 나온다 — 목록을 보고 판단한다)
"""
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

prom = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:59490"
dir_ = Path(__file__).resolve().parent.parent / "monitoring" / "grafana" / "dashboards"
panels_all = []
for dash in sorted(dir_.glob("*.json")):
    for p in json.loads(dash.read_text(encoding="utf-8"))["panels"]:
        p["title"] = f"{dash.stem} · {p.get('title', '')}"
        panels_all.append(p)

empty, total = [], 0
for p in panels_all:
    for t in p.get("targets", []):
        total += 1
        expr = t["expr"].replace("$pipeline", ".*")   # 대시보드 변수 — 전체로 본다
        url = prom + "/api/v1/query?" + urllib.parse.urlencode({"query": expr})
        with urllib.request.urlopen(url, timeout=10) as r:
            res = json.load(r)
        if res.get("status") != "success" or not res["data"]["result"]:
            empty.append((p["title"], t["expr"]))

print(f"쿼리 {total}개 중 빈 것 {len(empty)}개")
for title, expr in empty:
    print(f"  - [{title}] {expr}")
sys.exit(1 if empty else 0)
