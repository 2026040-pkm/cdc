import { useEffect, useRef, useState } from "react";
import {
  bayOf,
  type ActualPayload,
  type ArtifactPayload,
  type DeviceState,
  type StatusPayload,
  type WireFrame,
} from "./model";

export const WS_URL: string =
  import.meta.env.VITE_WS_URL ?? `ws://${location.hostname || "localhost"}:64080/ws/tags`;

export type LinkState =
  | { phase: "connecting"; attempt: number }
  | { phase: "open"; since: number }
  | { phase: "waiting"; attempt: number; retryAt: number; reason: string };

export interface FeedItem {
  id: string;
  at: number; // 수신 시각
  device: string;
  bay: string | null;
  payload: ActualPayload;
}

export interface StreamSnapshot {
  link: LinkState;
  connectedAt: number | null;
  devices: Map<string, DeviceState>;
  feed: FeedItem[];
  lastFrameAt: number | null;
  lastFrameCount: number;
  /** 최근 60초 동안 받은 [수신시각, 채널별 건수] */
  rate: { at: number; status: number; actual: number; artifact: number }[];
  now: number;
}

const FEED_LIMIT = 300;
const ARTIFACT_KEEP = 12;
const RETRY_MS = [1_000, 2_000, 4_000, 8_000, 10_000];

/**
 * /ws/tags 에 붙어 장비별 최신 상태와 실적 피드를 쌓는다.
 * 프레임은 약 1초에 한 번이라 프레임마다 한 번 다시 그린다. 수신이 없어도 '끊김' 판정이 흐르도록 1초 시계를 돈다.
 */
export function useTagStream(): StreamSnapshot {
  const devices = useRef(new Map<string, DeviceState>());
  const feed = useRef<FeedItem[]>([]);
  const rate = useRef<StreamSnapshot["rate"]>([]);
  const lastFrame = useRef<{ at: number | null; count: number }>({ at: null, count: 0 });
  const [link, setLink] = useState<LinkState>({ phase: "connecting", attempt: 0 });
  const [connectedAt, setConnectedAt] = useState<number | null>(null);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retry: number | undefined;
    let attempt = 0;
    let disposed = false;

    const connect = () => {
      setLink({ phase: "connecting", attempt });
      ws = new WebSocket(WS_URL);
      ws.onopen = () => {
        attempt = 0;
        const t = Date.now();
        setConnectedAt(t);
        setLink({ phase: "open", since: t });
      };
      ws.onmessage = (e) => {
        const msg = JSON.parse(e.data as string) as WireFrame | { type: "hello" };
        if (msg.type !== "tags") return;
        ingest(msg as WireFrame);
        setNow(Date.now());
      };
      ws.onclose = (e) => {
        if (disposed) return;
        const wait = RETRY_MS[Math.min(attempt, RETRY_MS.length - 1)];
        attempt += 1;
        setConnectedAt(null);
        setLink({
          phase: "waiting",
          attempt,
          retryAt: Date.now() + wait,
          reason: e.code === 1006 ? "서버에 닿지 않습니다" : `연결 종료 (${e.code})`,
        });
        retry = window.setTimeout(connect, wait);
      };
    };

    const ingest = (frame: WireFrame) => {
      const at = Date.now();
      let nStatus = 0;
      let nActual = 0;
      let nArtifact = 0;
      for (const tag of frame.tags) {
        const d = deviceOf(devices.current, tag.device);
        const value = tag.value as Record<string, unknown>;
        if (tag.channel === "status") {
          nStatus++;
          const s = value as unknown as StatusPayload;
          if (d.status?.status !== s.status) d.statusSince = at;
          d.status = s;
          d.lastStatusAt = at;
        } else if (tag.channel === "actual") {
          nActual++;
          const a = value as unknown as ActualPayload;
          d.lastActual = a;
          d.lastActualAt = at;
          feed.current.unshift({ id: `${tag.device}:${a.scan_id}:${a.stage}:${a.event_type}`, at, device: tag.device, bay: d.bay, payload: a });
        } else if (tag.channel === "artifact") {
          nArtifact++;
          d.artifacts.unshift({ type: tag.artifactType ?? "?", payload: value as unknown as ArtifactPayload, at });
          if (d.artifacts.length > ARTIFACT_KEEP) d.artifacts.length = ARTIFACT_KEEP;
        }
      }
      if (feed.current.length > FEED_LIMIT) feed.current.length = FEED_LIMIT;
      rate.current.push({ at, status: nStatus, actual: nActual, artifact: nArtifact });
      while (rate.current.length && at - rate.current[0].at > 60_000) rate.current.shift();
      lastFrame.current = { at, count: frame.count };
    };

    connect();
    const clock = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => {
      disposed = true;
      window.clearTimeout(retry);
      window.clearInterval(clock);
      ws?.close();
    };
  }, []);

  return {
    link,
    connectedAt,
    devices: devices.current,
    feed: feed.current,
    lastFrameAt: lastFrame.current.at,
    lastFrameCount: lastFrame.current.count,
    rate: rate.current,
    now,
  };
}

function deviceOf(map: Map<string, DeviceState>, device: string): DeviceState {
  let d = map.get(device);
  if (!d) {
    d = { device, bay: bayOf(device), artifacts: [] };
    map.set(device, d);
  }
  return d;
}
