import { X } from "lucide-react";
import { EVENT_LABEL, STAGE_LABEL, bayLabel, formatClock, shortId } from "../model";
import type { FeedItem } from "../stream";

interface Props {
  items: FeedItem[];
  now: number;
  bayFilter: string | null;
  selected: string | null;
  onClearBay: () => void;
  onSelect: (device: string) => void;
}

const FRESH_MS = 2_000;
const SHOW = 120;

export function ActualFeed({ items, now, bayFilter, selected, onClearBay, onSelect }: Props) {
  const rows = (bayFilter ? items.filter((i) => i.bay === bayFilter) : items).slice(0, SHOW);
  return (
    <section className="panel feed" aria-labelledby="feed-h">
      <div className="panel-head">
        <h2 id="feed-h">실적 피드</h2>
        {bayFilter && (
          <button type="button" className="chip" onClick={onClearBay}>
            {bayLabel(bayFilter)}
            <X size={12} aria-label="필터 해제" />
          </button>
        )}
      </div>

      {rows.length === 0 ? (
        <p className="empty">
          {bayFilter ? `${bayLabel(bayFilter)} 실적이 아직 없습니다.` : "실적은 장비마다 1분에 한 번 옵니다. 첫 실적을 기다리는 중입니다."}
        </p>
      ) : (
        <table className="feed-table">
          <thead>
            <tr>
              <th scope="col">수신</th>
              <th scope="col">장비</th>
              <th scope="col">블록</th>
              <th scope="col">공정</th>
              <th scope="col" className="num">진척</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const a = r.payload;
              return (
                <tr
                  key={r.id + r.at}
                  data-fresh={now - r.at < FRESH_MS || undefined}
                  aria-selected={selected === r.device}
                  onClick={() => onSelect(r.device)}
                >
                  <td className="mono muted">{formatClock(r.at)}</td>
                  <td className="mono">{shortId(r.device)}</td>
                  <td className="mono">
                    {a.hull_no}/{a.block_id}
                  </td>
                  <td>
                    {STAGE_LABEL[a.stage] ?? a.stage}{" "}
                    <span className="evt" data-evt={a.event_type}>
                      {EVENT_LABEL[a.event_type] ?? a.event_type}
                    </span>
                  </td>
                  <td className="num mono">{a.block_progress_rate.toFixed(1)}%</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </section>
  );
}
