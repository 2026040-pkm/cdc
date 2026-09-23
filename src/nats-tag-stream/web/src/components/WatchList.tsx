import { AlertOctagon, AlertTriangle, CheckCircle2, RefreshCw, Signal, Thermometer, WifiOff } from "lucide-react";
import type { ReactNode } from "react";
import { bayLabel, formatAgo, shortId, type Issue } from "../model";

export interface DeviceIssues {
  device: string;
  bay: string | null;
  worst: Issue;
  others: Issue[];
}

interface Props {
  rows: DeviceIssues[];
  calibrating: number;
  waiting: boolean;
  now: number;
  selected: string | null;
  onSelect: (device: string) => void;
}

const ICON: Record<Issue["kind"], ReactNode> = {
  offline: <WifiOff size={16} />,
  error: <AlertOctagon size={16} />,
  stale: <WifiOff size={16} />,
  silent: <WifiOff size={16} />,
  hot: <Thermometer size={16} />,
  warm: <Thermometer size={16} />,
  signal: <Signal size={16} />,
  calibrating: <RefreshCw size={16} />,
};

export function WatchList({ rows, calibrating, waiting, now, selected, onSelect }: Props) {
  return (
    <section className="panel watch" aria-labelledby="watch-h">
      <div className="panel-head">
        <h2 id="watch-h">지금 볼 것</h2>
        <span className="count">{rows.length > 0 ? `${rows.length}대` : ""}</span>
      </div>

      {waiting ? (
        <p className="empty">
          <AlertTriangle size={18} aria-hidden />
          수신을 기다리는 중입니다. 백엔드(:64080)가 떠 있고 NATS 에 붙어 있는지 확인하세요.
        </p>
      ) : rows.length === 0 ? (
        <p className="empty ok">
          <CheckCircle2 size={18} aria-hidden />
          이상 장비가 없습니다. 모든 장비가 최근 5초 안에 정상 상태를 보냈습니다.
        </p>
      ) : (
        <ol className="issues">
          {rows.map((r) => (
            <li key={r.device}>
              <button
                type="button"
                className="issue"
                data-sev={r.worst.severity}
                aria-pressed={selected === r.device}
                onClick={() => onSelect(r.device)}
              >
                <span className="sev-icon" aria-hidden>
                  {ICON[r.worst.kind]}
                </span>
                <span className="issue-id mono">{shortId(r.device)}</span>
                <span className="issue-bay">{bayLabel(r.bay)}</span>
                <span className="issue-title">{r.worst.title}</span>
                <span className="issue-detail">
                  {r.worst.detail}
                  {r.others.map((o) => (
                    <em key={o.kind}>{o.title} {o.detail}</em>
                  ))}
                </span>
                <span className="issue-since mono">{r.worst.since ? formatAgo(now - r.worst.since) : ""}</span>
              </button>
            </li>
          ))}
        </ol>
      )}

      {calibrating > 0 && (
        <p className="foot-note">
          <RefreshCw size={14} aria-hidden /> 보정 중 {calibrating}대 — 이상으로 치지 않습니다. bay 지도에서 파란 칸입니다.
        </p>
      )}
    </section>
  );
}
