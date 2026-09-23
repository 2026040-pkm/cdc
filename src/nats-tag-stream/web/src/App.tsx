import { useEffect, useMemo, useState } from "react";
import { ActualFeed } from "./components/ActualFeed";
import { DevicePanel } from "./components/DevicePanel";
import { FloorMap } from "./components/FloorMap";
import { TopBar } from "./components/TopBar";
import { WatchList, type DeviceIssues } from "./components/WatchList";
import { BAYS, SEVERITY_RANK, bayOf, STALE_MS, issuesOf, worstOf, type Issue, type Severity } from "./model";
import { useTagStream } from "./stream";

type Theme = "light" | "dark";

function initialTheme(): Theme {
  try {
    const saved = localStorage.getItem("lidar-monitor-theme");
    if (saved === "light" || saved === "dark") return saved;
  } catch {
    // 저장소를 못 쓰면 시스템 설정을 따른다
  }
  return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export function App() {
  const s = useTagStream();
  const [selected, setSelected] = useState<string | null>(null);
  const [bayFilter, setBayFilter] = useState<string | null>(null);
  const [theme, setTheme] = useState<Theme>(initialTheme);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try {
      localStorage.setItem("lidar-monitor-theme", theme);
    } catch {
      // 무시
    }
  }, [theme]);

  // 프레임(1초)마다 다시 계산한다. 350대라 가볍다.
  const derived = useMemo(() => {
    const perDevice = new Map<string, Issue[]>();
    const worst = new Map<string, Severity>();
    const rows: DeviceIssues[] = [];
    let calibrating = 0;
    let reporting = 0;
    let critical = 0;

    const ids = new Set<string>(BAYS.flatMap((b) => b.devices));
    for (const id of s.devices.keys()) ids.add(id);

    for (const id of ids) {
      const d = s.devices.get(id) ?? { device: id, bay: bayOf(id), artifacts: [] };
      if (d.lastStatusAt && s.now - d.lastStatusAt <= STALE_MS) reporting++;
      const issues = issuesOf(d, s.now, s.connectedAt);
      issues.sort((x, y) => SEVERITY_RANK[x.severity] - SEVERITY_RANK[y.severity]);
      perDevice.set(id, issues);
      const w = worstOf(issues);
      if (w) worst.set(id, w);
      if (w === "info") {
        calibrating++;
        continue;
      }
      const serious = issues.filter((i) => i.severity !== "info");
      if (serious.length) {
        if (serious[0].severity === "critical") critical++;
        rows.push({ device: id, bay: serious[0].bay, worst: serious[0], others: serious.slice(1) });
      }
    }
    rows.sort(
      (x, y) =>
        SEVERITY_RANK[x.worst.severity] - SEVERITY_RANK[y.worst.severity] ||
        (x.worst.since ?? Infinity) - (y.worst.since ?? Infinity) ||
        x.device.localeCompare(y.device),
    );

    const actualPerMin = s.rate.reduce((n, r) => n + r.actual, 0);
    const artifactPerMin = s.rate.reduce((n, r) => n + r.artifact, 0);
    return { perDevice, worst, rows, calibrating, reporting, critical, actualPerMin, artifactPerMin };
  }, [s.now, s.devices, s.connectedAt, s.rate]);

  const waiting = s.link.phase !== "open" || s.lastFrameAt === null;

  return (
    <div className="app">
      <TopBar
        link={s.link}
        now={s.now}
        lastFrameAt={s.lastFrameAt}
        reporting={derived.reporting}
        issueDevices={derived.rows.length}
        criticalDevices={derived.critical}
        actualPerMin={derived.actualPerMin}
        artifactPerMin={derived.artifactPerMin}
        theme={theme}
        onTheme={() => setTheme(theme === "dark" ? "light" : "dark")}
      />
      <main className="layout">
        <div className="main-col">
          <WatchList
            rows={derived.rows}
            calibrating={derived.calibrating}
            waiting={waiting}
            now={s.now}
            selected={selected}
            onSelect={setSelected}
          />
          <FloorMap
            devices={s.devices}
            worst={derived.worst}
            now={s.now}
            selected={selected}
            bayFilter={bayFilter}
            onSelect={setSelected}
            onBay={setBayFilter}
          />
        </div>
        <aside className="side-col">
          {selected ? (
            <DevicePanel
              device={selected}
              state={s.devices.get(selected)}
              issues={derived.perDevice.get(selected) ?? []}
              now={s.now}
              onClose={() => setSelected(null)}
            />
          ) : (
            <ActualFeed
              items={s.feed}
              now={s.now}
              bayFilter={bayFilter}
              selected={selected}
              onClearBay={() => setBayFilter(null)}
              onSelect={setSelected}
            />
          )}
        </aside>
      </main>
    </div>
  );
}
