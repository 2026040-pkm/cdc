import { ArrowLeft } from "lucide-react";
import {
  ARTIFACT_LABEL,
  ERROR_LABEL,
  EVENT_LABEL,
  STAGE_LABEL,
  bayLabel,
  formatAgo,
  formatBytes,
  formatClock,
  type DeviceState,
  type Issue,
} from "../model";

interface Props {
  state: DeviceState | undefined;
  device: string;
  issues: Issue[];
  now: number;
  onClose: () => void;
}

export function DevicePanel({ state, device, issues, now, onClose }: Props) {
  const s = state?.status;
  const a = state?.lastActual;
  const worst = issues[0]?.severity;
  return (
    <section className="panel device" aria-labelledby="dev-h">
      <div className="panel-head">
        <button type="button" className="icon-btn" onClick={onClose} aria-label="실적 피드로 돌아가기">
          <ArrowLeft size={16} />
        </button>
        <h2 id="dev-h" className="mono">
          {device}
        </h2>
        <span className="muted">{bayLabel(state?.bay ?? null)}</span>
      </div>

      <div className="dev-status" data-sev={worst ?? (s ? "ok" : "nodata")}>
        <strong>{s?.status ?? "수신 전"}</strong>
        <span>
          {s?.error_code ? `${s.error_code} · ${ERROR_LABEL[s.error_code] ?? ""}` : issues.map((i) => i.title).join(" · ") || "이상 없음"}
        </span>
        {state?.statusSince && <span className="mono muted">이 상태 {formatAgo(now - state.statusSince)}</span>}
      </div>

      <dl className="metrics">
        <Metric label="온도" value={s ? `${s.temperature_c.toFixed(1)}℃` : "-"} />
        <Metric label="스캔 속도" value={s ? `${(s.scan_rate_pts_per_sec / 1000).toFixed(0)}k pts/s` : "-"} />
        <Metric label="신호" value={s ? `${s.connectivity_rssi} dBm` : "-"} />
        <Metric label="시야" value={s?.fov_mode ?? "-"} />
        <Metric label="마지막 하트비트" value={formatClock(s?.last_heartbeat_at)} />
        <Metric label="마지막 수신" value={state?.lastStatusAt ? `${formatAgo(now - state.lastStatusAt)} 전` : "-"} />
      </dl>

      <h3>마지막 실적</h3>
      {a ? (
        <div className="dev-actual">
          <p>
            <b>{STAGE_LABEL[a.stage] ?? a.stage}</b> {EVENT_LABEL[a.event_type] ?? a.event_type}
            <span className="mono muted"> · {a.hull_no}/{a.block_id}</span>
          </p>
          <div className="bar" role="meter" aria-valuemin={0} aria-valuemax={100} aria-valuenow={a.block_progress_rate} aria-label="블록 진척률">
            <i style={{ width: `${Math.min(100, a.block_progress_rate)}%` }} />
          </div>
          <p className="mono muted">
            진척 {a.block_progress_rate.toFixed(1)}% · 일치도 {(a.match_confidence * 100).toFixed(0)}% · {formatClock(a.occurred_at)}
          </p>
        </div>
      ) : (
        <p className="muted">접속 후 아직 실적이 없습니다.</p>
      )}

      <h3>최근 산출물</h3>
      {state?.artifacts.length ? (
        <ul className="artifacts">
          {state.artifacts.map((x, i) => (
            <li key={i}>
              <span>{ARTIFACT_LABEL[x.type] ?? x.type}</span>
              <span className="mono muted">{x.payload.segment_id ?? ""}</span>
              <span className="mono num">{formatBytes(x.payload.file_size_bytes)}</span>
              <span className="mono muted num">{formatClock(x.at)}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="muted">접속 후 아직 산출물이 없습니다.</p>
      )}

      <details className="raw">
        <summary>원문 보기</summary>
        <pre>{JSON.stringify({ status: s ?? null, actual: a ?? null }, null, 2)}</pre>
      </details>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd className="mono">{value}</dd>
    </div>
  );
}
