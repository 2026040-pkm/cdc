import { BAYS, EVENT_LABEL, STAGE_LABEL, ZONES, formatAgo, shortId, type DeviceState, type Severity } from "../model";

interface Props {
  devices: Map<string, DeviceState>;
  worst: Map<string, Severity>;
  now: number;
  selected: string | null;
  bayFilter: string | null;
  onSelect: (device: string) => void;
  onBay: (bay: string | null) => void;
}

/** 실적이 도착하면 그 장비 칸에 한 번 번지는 표시. 이 시간 안이면 칸을 다시 그려 애니메이션을 한 번 돌린다. */
const PULSE_MS = 2_400;

export function FloorMap({ devices, worst, now, selected, bayFilter, onSelect, onBay }: Props) {
  return (
    <section className="panel floor" aria-labelledby="floor-h">
      <div className="panel-head">
        <h2 id="floor-h">bay 상태</h2>
        <ul className="legend" aria-label="칸 색 범례">
          <li data-tile="ok">정상</li>
          <li data-tile="info">보정 중</li>
          <li data-tile="warn">주의</li>
          <li data-tile="high">끊김·과열</li>
          <li data-tile="critical">OFFLINE·오류</li>
          <li data-tile="nodata">수신 전</li>
          <li data-tile="pulse">실적 도착</li>
        </ul>
      </div>

      {ZONES.map((zone) => (
        <div className="zone" key={zone.key}>
          <h3 className="zone-label">{zone.label}</h3>
          <div className="bays" data-zone={zone.key}>
            {BAYS.filter((b) => b.zone === zone.key).map((bay) => {
              let reporting = 0;
              let lastActual: DeviceState | undefined;
              for (const id of bay.devices) {
                const d = devices.get(id);
                if (d?.lastStatusAt && now - d.lastStatusAt <= 5_000) reporting++;
                if (d?.lastActualAt && (!lastActual || d.lastActualAt > (lastActual.lastActualAt ?? 0))) lastActual = d;
              }
              const down = bay.devices.length - reporting;
              const active = bayFilter === bay.key;
              return (
                <div className="bay" key={bay.key} data-active={active || undefined}>
                  <button
                    type="button"
                    className="bay-head"
                    aria-pressed={active}
                    onClick={() => onBay(active ? null : bay.key)}
                    title={active ? "실적 피드 필터 해제" : "이 bay 실적만 보기"}
                  >
                    <b>B{bay.bay}</b>
                    <span className="mono" data-down={down > 0 || undefined}>
                      {reporting}/{bay.devices.length}
                    </span>
                  </button>
                  <div className="tiles" style={{ ["--cols" as string]: zone.perBay === 30 ? 6 : 5 }}>
                    {bay.devices.map((id) => {
                      const d = devices.get(id);
                      const sev = worst.get(id);
                      const tile = sev ?? (d?.lastStatusAt ? "ok" : "nodata");
                      const pulse = d?.lastActualAt && now - d.lastActualAt < PULSE_MS ? d.lastActualAt : null;
                      return (
                        <button
                          type="button"
                          key={id}
                          className="tile"
                          data-tile={tile}
                          aria-pressed={selected === id}
                          aria-label={`${shortId(id)} ${d?.status?.status ?? "수신 전"}`}
                          title={`${shortId(id)} · ${d?.status?.status ?? "수신 전"}`}
                          onClick={() => onSelect(id)}
                        >
                          {pulse && <span className="pulse" key={pulse} aria-hidden />}
                        </button>
                      );
                    })}
                  </div>
                  <p className="bay-last">
                    {lastActual?.lastActual ? (
                      <>
                        {STAGE_LABEL[lastActual.lastActual.stage] ?? lastActual.lastActual.stage}{" "}
                        {EVENT_LABEL[lastActual.lastActual.event_type] ?? lastActual.lastActual.event_type}
                        <span className="mono"> {formatAgo(now - (lastActual.lastActualAt ?? now))}</span>
                      </>
                    ) : (
                      <span className="muted">실적 없음</span>
                    )}
                  </p>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </section>
  );
}
