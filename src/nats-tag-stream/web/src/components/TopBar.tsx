import { Moon, Radar, Sun } from "lucide-react";
import { FLEET_SIZE, formatAgo } from "../model";
import type { LinkState } from "../stream";

interface Props {
  link: LinkState;
  now: number;
  lastFrameAt: number | null;
  reporting: number;
  issueDevices: number;
  criticalDevices: number;
  actualPerMin: number;
  artifactPerMin: number;
  theme: "light" | "dark";
  onTheme: () => void;
}

export function TopBar(p: Props) {
  return (
    <header className="topbar">
      <div className="brand">
        <Radar size={20} strokeWidth={2} aria-hidden />
        <div>
          <h1>LiDAR 현장 관제</h1>
          <p>거제 · 조립 1공장 · 의장 1공장</p>
        </div>
      </div>

      <dl className="stats">
        <div>
          <dt>상태 수신</dt>
          <dd>
            <b>{p.reporting}</b>
            <span>/ {FLEET_SIZE}대</span>
          </dd>
        </div>
        <div data-tone={p.criticalDevices > 0 ? "critical" : p.issueDevices > 0 ? "warn" : "ok"}>
          <dt>이상 장비</dt>
          <dd>
            <b>{p.issueDevices}</b>
            <span>{p.criticalDevices > 0 ? `중대 ${p.criticalDevices}` : "대"}</span>
          </dd>
        </div>
        <div>
          <dt>실적</dt>
          <dd>
            <b>{p.actualPerMin}</b>
            <span>건/분</span>
          </dd>
        </div>
        <div>
          <dt>산출물</dt>
          <dd>
            <b>{p.artifactPerMin}</b>
            <span>건/분</span>
          </dd>
        </div>
      </dl>

      <div className="topbar-end">
        <LinkBadge link={p.link} now={p.now} lastFrameAt={p.lastFrameAt} />
        <span className="sim-tag" title="lidar-sim 이 만든 값입니다. 실제 현장 수치가 아닙니다.">
          시뮬레이터 데이터
        </span>
        <button
          type="button"
          className="icon-btn"
          onClick={p.onTheme}
          aria-label={p.theme === "dark" ? "밝은 화면으로" : "어두운 화면으로"}
        >
          {p.theme === "dark" ? <Sun size={16} /> : <Moon size={16} />}
        </button>
      </div>
    </header>
  );
}

function LinkBadge({ link, now, lastFrameAt }: { link: LinkState; now: number; lastFrameAt: number | null }) {
  if (link.phase === "open") {
    const age = lastFrameAt ? now - lastFrameAt : null;
    const late = age !== null && age > 3_000;
    return (
      <span className="link" data-state={late ? "late" : "open"} role="status">
        <i aria-hidden />
        {age === null ? "연결됨 · 첫 프레임 대기" : late ? `프레임 멈춤 ${formatAgo(age)}` : "실시간"}
      </span>
    );
  }
  if (link.phase === "connecting") {
    return (
      <span className="link" data-state="connecting" role="status">
        <i aria-hidden />
        연결 중
      </span>
    );
  }
  return (
    <span className="link" data-state="down" role="status" title={link.reason}>
      <i aria-hidden />
      {link.reason} · {formatAgo(Math.max(0, link.retryAt - now))} 후 재시도
    </span>
  );
}
