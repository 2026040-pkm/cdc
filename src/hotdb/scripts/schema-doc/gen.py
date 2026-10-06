"""docs/hotdb/hotdb-schema-dataflow.html 생성기 — make.sh 가 부른다. 표 정의는 기동 중인 DB 조회 결과(out/*.txt)에서 온다."""
import html
import re
from collections import defaultdict
from pathlib import Path

D = Path(__file__).parent
O = D / "out"
E = html.escape

cols = defaultdict(list)
for line in (O / "cols.txt").read_text(encoding="utf-8").splitlines():
    s, t, _, c, ty, nul, dflt, ident = line.split("|")
    cols[f"{s}.{t}"].append((c, ty, nul == "YES", dflt, ident == "YES"))
pk = defaultdict(set)
for line in (O / "pk.txt").read_text(encoding="utf-8").splitlines():
    s, t, c = line.split("|")
    pk[f"{s}.{t}"].add(c)

# ── 컬럼별 출처 · 뜻 ──────────────────────────────────────────────────────
# tsdb: MQTT raw_payload 필드 → 컬럼 (태그 카탈로그 v1 실제 발행 기준)
SRC = {
    "tsdb.device": {
        "site": ("raw_payload.site", "사업장 (geoje)"),
        "device_id": ("봉투 id", "장비 id. 산출물은 접미사를 뗀 값"),
        "device_role": ("raw_payload.device_role", "LIDAR"),
        "zone": ("raw_payload.zone", "ASSEMBLY · OUTFITTING · PAINTING · MACHINING — 권역 서비스가 거르는 기준"),
        "shop": ("raw_payload.shop", "작업장"),
        "bay": ("raw_payload.bay", "베이"),
        "active": ("—", "등록 해제 표시"),
    },
    "tsdb.tag_catalog": {
        "tag_key": ("자동 (identity)", "이력 표가 참조하는 숫자 키"),
        "edge_group_id": ("태그 등록부", "Agent 그룹. 발행기는 sim, 테스트는 it"),
        "tag_id": ("Mqtt.Agent", "{id}.{토픽 '/'→'_'}.raw_payload — 100자 이하"),
        "mqtt_topic": ("토픽", "TagId 에서 역변환이 안 돼 원문 보관"),
        "source_id": ("봉투 id", "산출물은 -REGISTERED_PCD 등 접미사 포함"),
        "site": ("raw_payload.site", "device FK"),
        "device_id": ("봉투 id (접미사 제거)", "device FK"),
        "channel": ("토픽 마지막 마디", "status · actual · artifact"),
        "artifact_type": ("id 접미사", "artifact 채널만"),
        "active": ("—", ""),
    },
    "tsdb.status_history": {
        "event_time": ("raw_payload.occurred_at", "발생 시각 (마이크로초까지)"),
        "tag_key": ("Provider 가 태그 등록부로 풂", "MQTT Agent 태그(TagId) → tag_key"),
        "status": ("raw_payload.status", "ONLINE · CALIBRATING · ERROR · OFFLINE"),
        "error_code": ("raw_payload.error_code", "ERROR 일 때"),
        "last_heartbeat_at": ("raw_payload.last_heartbeat_at", ""),
        "scan_rate_pts_per_sec": ("raw_payload.scan_rate_pts_per_sec", "pts/s · 예 229,866"),
        "temperature_c": ("raw_payload.temperature_c", "℃ · 예 40.9"),
        "connectivity_rssi": ("raw_payload.connectivity_rssi", "dBm · 예 -52"),
        "fov_mode": ("raw_payload.fov_mode", "wide …"),
        "source_time_text": ("raw_payload.occurred_at 원문", ".NET 7자리 그대로 — PG 가 잘라 먹는 자릿수 보존"),
        "source_ingested_at": ("raw_payload.ingested_at", "발행기 수집 시각"),
        "received_at": ("Provider 수신 시각", "clock_timestamp(). 반영 지연의 기준점"),
    },
    "tsdb.actual_history": {
        "event_time": ("raw_payload.occurred_at", ""),
        "tag_key": ("Provider 가 태그 등록부로 풂", ""),
        "scan_id": ("raw_payload.scan_id", "artifact 와 조인 키"),
        "event_type": ("raw_payload.event_type", "START · PROGRESS · COMPLETE"),
        "hull_no": ("raw_payload.hull_no", "호선 · 예 H1207"),
        "block_id": ("raw_payload.block_id", "블록 · 예 B107P"),
        "scanned_at": ("raw_payload.scanned_at", ""),
        "block_progress_rate": ("raw_payload.block_progress_rate", "0 ~ 100 %"),
        "match_confidence": ("raw_payload.match_confidence", "0 ~ 1"),
        "reference_cad_id": ("raw_payload.reference_cad_id", ""),
        "model_version": ("raw_payload.model_version", "예 seg-v1.4.2"),
        "source_time_text": ("raw_payload.occurred_at 원문", ""),
        "source_ingested_at": ("raw_payload.ingested_at", ""),
        "received_at": ("Provider 수신 시각", ""),
    },
    "tsdb.artifact_history": {
        "event_time": ("raw_payload.occurred_at", ""),
        "tag_key": ("Provider 가 태그 등록부로 풂", "종류별 태그 — 장비당 3개"),
        "scan_id": ("raw_payload.scan_id", ""),
        "artifact_type": ("raw_payload.artifact_type", "REGISTERED_PCD · TRANSFORMATION_MATRIX · SEGMENTED_PCD"),
        "segment_key": ("raw_payload.segment_id", "null → '' (PK 에 null 불가). SEGMENTED_PCD 만 값"),
        "hull_no": ("raw_payload.hull_no", ""),
        "block_id": ("raw_payload.block_id", ""),
        "storage_uri": ("raw_payload.storage_uri", "MATRIX 만 null"),
        "file_size_bytes": ("raw_payload.file_size_bytes", ""),
        "checksum": ("raw_payload.checksum", "sha256:…"),
        "transformation_matrix": ("raw_payload.transformation_matrix", "4×4 = 16원소. MATRIX 만"),
        "produced_by_device_id": ("raw_payload.produced_by_device_id", "추론 WS · 예 INF-GJ-A1-01"),
        "model_version": ("raw_payload.model_version", ""),
        "source_time_text": ("raw_payload.occurred_at 원문", ""),
        "source_ingested_at": ("raw_payload.ingested_at", ""),
        "received_at": ("Provider 수신 시각", ""),
    },
}

# svc_*: 라우트 식 (zone-service application.yml). (라우트, 식, 뜻)
ROUTE = {
    "svc.device_status_current": ("status-current · upsert · keys device_id · newer-than src_event_time", {
        "site": "tag.site", "device_id": "tag.device_id", "status": "row.status", "error_code": "row.error_code",
        "last_heartbeat_at": "row.last_heartbeat_at", "scan_rate_pts_per_sec": "row.scan_rate_pts_per_sec",
        "temperature_c": "row.temperature_c", "connectivity_rssi": "row.connectivity_rssi", "fov_mode": "row.fov_mode",
        "src_event_time": "row.event_time", "src_received_at": "row.received_at", "applied_at": "$now"}),
    "svc.device_status_change": ("status-change · insert-on-change · partition-by device_id · change-of status, error_code", {
        "device_id": "tag.device_id", "changed_at": "row.event_time", "status": "row.status", "error_code": "row.error_code",
        "src_received_at": "row.received_at", "applied_at": "$now"}),
    "svc.scan": ("scan · upsert · keys scan_id · newer-than last_event_at  (+ scan-stub · insert — 산출물이 먼저 오면 부모 자리)", {
        "scan_id": "row.scan_id", "site": "tag.site", "device_id": "tag.device_id", "hull_no": "row.hull_no", "block_id": "row.block_id",
        "first_event_at": "row.event_time | min", "last_event_at": "row.event_time", "last_event_type": "row.event_type",
        "block_progress_rate": "row.block_progress_rate", "match_confidence": "row.match_confidence",
        "reference_cad_id": "row.reference_cad_id", "model_version": "row.model_version",
        "completed_at": "row.event_time if row.event_type=COMPLETE | coalesce",
        "src_received_at": "row.received_at", "applied_at": "$now"}),
    "svc.actual_result": ("actual-result · upsert · when event_type=COMPLETE · keys hull_no, block_id, scan_id", {
        "hull_no": "row.hull_no", "block_id": "row.block_id", "scan_id": "row.scan_id", "device_id": "tag.device_id",
        "completed_at": "row.event_time", "block_progress_rate": "row.block_progress_rate",
        "match_confidence": "row.match_confidence", "reference_cad_id": "row.reference_cad_id",
        "model_version": "row.model_version", "judged_status": "'PENDING' | keep",
        "src_received_at": "row.received_at", "applied_at": "$now"}),
    "svc.artifact": ("artifact · insert (충돌 무시)", {
        "scan_id": "row.scan_id", "artifact_type": "row.artifact_type", "segment_key": "row.segment_key",
        "device_id": "tag.device_id", "hull_no": "row.hull_no", "block_id": "row.block_id",
        "storage_uri": "row.storage_uri", "file_size_bytes": "row.file_size_bytes", "checksum": "row.checksum",
        "transformation_matrix": "row.transformation_matrix", "produced_by_device_id": "row.produced_by_device_id",
        "model_version": "row.model_version", "src_event_time": "row.event_time",
        "src_received_at": "row.received_at", "applied_at": "$now"}),
}
NOTE = {
    "first_event_at": "더 이른 값을 남김 (min) — 산출물이 만든 자리보다 START 가 이르면 START",
    "last_event_type": "START · PROGRESS · COMPLETE · PENDING(산출물이 만든 자리)",
    "site": "장비 마스터 tsdb.device FK 의 한쪽",
    "completed_at": "COMPLETE 일 때만 값, 아니면 기존 값 유지 (coalesce)",
    "judged_status": "판별 로직(2단계)이 바꾼 값을 재전송이 되돌리지 않게 keep",
    "applied_at": "DB clock_timestamp() — 반영 시각",
    "src_received_at": "tsdb 의 received_at",
    "device_id": "lookup tag 로 tag_key 를 장비 id 로 풂",
}
OPS = {
    "ops.module": {"module": "모듈 이름 (mch · asm · oft · pnt …)", "zone_code": "tsdb.device.zone 값 — 이 값의 장비만 이 모듈로",
                   "display_name": "가공 실적 판별 …", "created_at": ""},
    "ops.poll_state": {"agent": "rfc-service · db-agent", "job": "작업 이름 (대상 표)", "watermark": "증분: 마지막으로 읽은 upd_date || upd_time",
                       "last_run_at": "", "last_ok_at": "경보 LegacyPollStale 기준", "last_rows": "", "last_error": "실패 원인 (성공하면 비움)"},
    "ops.rfc_sent": {"zone": "모듈", "hull_no": "", "block_id": "", "scan_id": "", "judged_status": "판정이 바뀌면 새 행 → 다시 보냄",
                     "src_received_at": "tsdb 수신 시각", "src_applied_at": "모듈 RDB 반영 시각", "src_lsn": "",
                     "sent_at": "SAP 송신 시각 — sent_at - src_applied_at = 2단 지연"},
    "ops.cdc_heartbeat": {"pipeline": "zone-asm · zone-oft …", "beat_at": "Debezium heartbeat.action.query 가 10초마다 갱신"},
    "ops.cdc_dead_letter": {"id": "", "pipeline": "어느 서비스", "route": "어느 라우트", "source": "원천 논리 표",
                            "lsn": "원천 커밋 LSN", "payload": "Debezium 원문 JSON", "error": "마지막 오류 메시지",
                            "created_at": ""},
}
TBL_DESC = {
    "ops.module": "판별 모듈 등록부 — provision_module 이 채운다",
    "ops.poll_state": "레거시 폴링 작업 상태 (RFC Service · DB Agent)",
    "ops.rfc_sent": "SAP 로 보낸 실적 기록 — 실적 · 판정당 한 번",
    "tsdb.device": "장비 한 대 한 행. 1초마다 바뀌지 않는 위치 · 역할을 이력 대신 여기 둔다",
    "tsdb.tag_catalog": "태그 등록부. 이력 행의 tag_key 를 장비 · 채널로 푸는 유일한 길",
    "tsdb.status_history": "하이퍼테이블 · 청크 6시간 · 350 태그 × 1초 ≈ 3,024만 행/일",
    "tsdb.actual_history": "하이퍼테이블 · 청크 1일 · 350 태그 × 1분 ≈ 50만 행/일",
    "tsdb.artifact_history": "하이퍼테이블 · 청크 1일 · 스캔당 12행 ≈ 605만 행/일",
    "svc.device_status_current": "장비별 최신 상태 1행 · 부모 tsdb.device",
    "svc.device_status_change": "상태가 바뀐 순간만 · 부모 device_status_current",
    "svc.scan": "스캔 1건의 진행 · 부모 tsdb.device · 실적 · 산출물의 부모",
    "svc.actual_result": "실적 판별 대상 — COMPLETE 된 스캔 · 부모 scan",
    "svc.artifact": "산출물 위치 (파일 본체 아님) · 부모 scan",
    "ops.cdc_heartbeat": "CDC 소비자 생존 신호 · 슬롯 전진용",
    "ops.cdc_dead_letter": "재시도해도 반영 못 한 이벤트",
}


def type_cell(ty, nul, dflt, ident):
    out = f'<span class="mono">{E(ty)}</span>'
    if not nul:
        out += ' <span class="chip chip--neutral">NOT NULL</span>'
    if ident:
        out += ' <span class="chip chip--info">identity</span>'
    if dflt:
        out += f' <span class="small muted">기본 {E(dflt)}</span>'
    return out


def col_name(t, c):
    key = '<span class="chip chip--ok">PK</span> ' if c in pk[t] else ""
    return f'{key}<span class="mono">{E(c)}</span>'


def table_block(t, mode):
    h = [f'<h3 id="t-{t.replace(".", "-")}"><span class="mono">{E(t)}</span> — {E(TBL_DESC.get(t, ""))}</h3>']
    if mode == "svc":
        route, exprs = ROUTE[t]
        h.append(f'<p class="small">라우트 <span class="mono">{E(route)}</span></p>')
        head = '<th style="width:24%">컬럼</th><th style="width:28%">타입</th><th style="width:28%">값 식 (라우트)</th><th>뜻</th>'
    elif mode == "tsdb":
        head = '<th style="width:23%">컬럼</th><th style="width:27%">타입</th><th style="width:25%">출처 (MQTT 필드)</th><th>뜻</th>'
    else:
        head = '<th style="width:24%">컬럼</th><th style="width:32%">타입</th><th>뜻</th>'
    h.append(f'<div class="tbl-wrap"><table class="tbl"><thead><tr>{head}</tr></thead><tbody>')
    for c, ty, nul, dflt, ident in cols[t]:
        if mode == "tsdb":
            src, mean = SRC[t].get(c, ("", ""))
            h.append(f"<tr><td>{col_name(t, c)}</td><td>{type_cell(ty, nul, dflt, ident)}</td>"
                     f'<td class="mono small">{E(src)}</td><td>{E(mean)}</td></tr>')
        elif mode == "svc":
            ex = ROUTE[t][1].get(c, "")
            h.append(f"<tr><td>{col_name(t, c)}</td><td>{type_cell(ty, nul, dflt, ident)}</td>"
                     f'<td class="mono small">{E(ex)}</td><td>{E(NOTE.get(c, ""))}</td></tr>')
        else:
            h.append(f"<tr><td>{col_name(t, c)}</td><td>{type_cell(ty, nul, dflt, ident)}</td>"
                     f"<td>{E(OPS.get(t, {}).get(c, ''))}</td></tr>")
    h.append("</tbody></table></div>")
    return "\n".join(h)


def legacy_block():
    rows = defaultdict(list)
    for line in (O / "legacy.txt").read_text(encoding="utf-8").splitlines():
        s, t, n, p = line.split("|")
        rows[s].append((t, int(n), p))
    src = {"erp": ("SAP", "rfc_agent"), "mes": ("Oracle · 생산", "db_agent"), "lgs": ("Oracle · 물류", "db_agent"),
           "geo": ("Oracle · 구역 지도", "db_agent")}
    out = ['<div class="tbl-wrap"><table class="tbl"><thead><tr><th>스키마</th><th>원천</th><th>쓰는 계정</th>'
           '<th>표</th><th>PK 있는 표</th><th>컬럼 합</th></tr></thead><tbody>']
    for s in ("erp", "mes", "lgs", "geo"):
        r = rows[s]
        out.append(f'<tr><td class="mono">{s}</td><td>{src[s][0]}</td><td class="mono">{src[s][1]}</td>'
                   f'<td class="num">{len(r)}</td><td class="num">{sum(1 for x in r if x[2])}</td>'
                   f'<td class="num">{sum(x[1] for x in r):,}</td></tr>')
    out.append("</tbody></table></div>")
    for s in ("erp", "mes", "lgs", "geo"):
        out.append(f'<details class="leg"><summary><span class="mono">{s}</span> 표 목록 ({len(rows[s])})</summary>'
                   '<div class="tbl-wrap"><table class="tbl"><thead><tr><th style="width:22%">표</th>'
                   '<th style="width:10%">컬럼</th><th>PK</th></tr></thead><tbody>')
        for t, n, p in rows[s]:
            out.append(f'<tr><td class="mono">{E(t)}</td><td class="num">{n}</td><td class="mono small">{E(p) or "—"}</td></tr>')
        out.append("</tbody></table></div></details>")
    return "\n".join(out)


body = (D / "body.tpl.html").read_text(encoding="utf-8")
for t in ["tsdb.device", "tsdb.tag_catalog", "tsdb.status_history", "tsdb.actual_history", "tsdb.artifact_history"]:
    body = body.replace(f"@@T:{t}@@", table_block(t, "tsdb"))
for t in ROUTE:
    body = body.replace(f"@@T:{t}@@", table_block(t, "svc"))
for t in OPS:
    body = body.replace(f"@@T:{t}@@", table_block(t, "ops"))
body = body.replace("@@LEGACY@@", legacy_block())
for k in ("tsdb", "rdb", "sap"):
    body = body.replace(f"@@ER_{k.upper()}@@", (O / f"er-{k}.html").read_text(encoding="utf-8"))
body = body.replace("@@SMAP@@", (O / "schema-map.html").read_text(encoding="utf-8"))
import base64
img = D.parents[3] / "docs/hotdb/img/hotdb-architecture-2026-10-06.png"
body = body.replace("@@IMG@@", "data:image/png;base64," + base64.b64encode(img.read_bytes()).decode())
for k in ("all", "status", "rel"):
    body = body.replace(f"@@FLOW:{k}@@", (O / f"flow-{k}.html").read_text(encoding="utf-8"))
left = re.findall(r"@@[^@]+@@", body)
assert not left, left

tpl = (Path.home() / ".claude/skills/html-doc/assets/devdoc-template.html").read_text(encoding="utf-8")
a, b = tpl.index('<div class="doc">'), tpl.index('<button id="bSideOpen"')
extra = ("<style>details.leg{margin:8px 0}details.leg summary{cursor:pointer;padding:6px 0;font-weight:600}"
         "@media print{details.leg{display:block}}"
         "figure.shot{margin:16px 0;padding:12px;border:1px solid var(--line);border-radius:var(--radius);background:#fff}"
         "figure.shot img{display:block;max-width:100%;height:auto;margin:0 auto}"
         "figure.shot figcaption{margin-top:8px;font-size:12.5px;opacity:.8}"
         + (O / "schema-map.css").read_text(encoding="utf-8") + (O / "er.css").read_text(encoding="utf-8") + "</style>")
out = tpl[:a] + body + "\n" + tpl[b:]
out = out.replace("<title>개발 문서 제목</title>", "<title>HotDB 스키마와 데이터 흐름</title>" + extra, 1)
(D.parents[3] / "docs/hotdb/hotdb-schema-dataflow.html").write_text(out, encoding="utf-8")
print("ok", len(out))
