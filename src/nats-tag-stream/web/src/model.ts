// 백엔드(nats-tag-stream) /ws/tags 프레임과 화면이 쓰는 파생 상태.

export type Channel = "status" | "actual" | "artifact";

export interface WireTag {
  tagId: string;
  device: string;
  artifactType?: string;
  topicKey: string;
  channel: Channel | string;
  field: string;
  valueKind: string;
  changedAt: string;
  value: Record<string, unknown> | unknown;
}

export interface WireFrame {
  type: "tags";
  subject: string;
  edgeGroupId: string;
  receivedAt: string;
  count: number;
  tags: WireTag[];
}

export interface StatusPayload {
  status: string;
  error_code: string | null;
  temperature_c: number;
  scan_rate_pts_per_sec: number;
  connectivity_rssi: number;
  fov_mode: string;
  last_heartbeat_at: string;
  occurred_at: string;
  zone: string;
  shop: string;
  bay: string;
}

export interface ActualPayload {
  stage: string;
  event_type: string;
  hull_no: string;
  block_id: string;
  block_progress_rate: number;
  match_confidence: number;
  scan_id: string;
  occurred_at: string;
  shop: string;
  bay: string;
}

export interface ArtifactPayload {
  artifact_type: string;
  segment_id: string | null;
  file_size_bytes: number | null;
  storage_uri: string | null;
  scan_id: string;
  occurred_at: string;
}

// ── 현장 구조 ─────────────────────────────────────────────────────────────
// 장비 id: LDR-GJ-{A|O}1B{bay}-{nn}. 조립 7 bay × 30대, 의장 7 bay × 20대 (lidar-sim 기본 350대).

export type ZoneKey = "A" | "O";

export interface Bay {
  key: string; // A1B3
  zone: ZoneKey;
  bay: number;
  devices: string[];
}

export const ZONES: { key: ZoneKey; label: string; shop: string; perBay: number }[] = [
  { key: "A", label: "조립", shop: "assembly1", perBay: 30 },
  { key: "O", label: "의장", shop: "outfitting1", perBay: 20 },
];

export const BAYS: Bay[] = ZONES.flatMap((z) =>
  Array.from({ length: 7 }, (_, i) => {
    const key = `${z.key}1B${i + 1}`;
    return {
      key,
      zone: z.key,
      bay: i + 1,
      devices: Array.from({ length: z.perBay }, (_, n) => `LDR-GJ-${key}-${String(n + 1).padStart(2, "0")}`),
    };
  }),
);

export const FLEET_SIZE = BAYS.reduce((sum, b) => sum + b.devices.length, 0);

const DEVICE_ID = /^LDR-[A-Z]+-([AO])1B(\d+)-\d+$/;

export function bayOf(device: string): string | null {
  const m = DEVICE_ID.exec(device);
  return m ? `${m[1]}1B${m[2]}` : null;
}

export function bayLabel(bayKey: string | null): string {
  if (!bayKey) return "위치 미상";
  const zone = ZONES.find((z) => z.key === bayKey[0]);
  return `${zone?.label ?? bayKey[0]} B${bayKey.slice(3)}`;
}

export function shortId(device: string): string {
  return device.replace(/^LDR-GJ-/, "");
}

// ── 용어 ──────────────────────────────────────────────────────────────────

export const STAGE_LABEL: Record<string, string> = {
  ARRANGEMENT: "배재",
  FITTING: "취부",
  WELDING: "용접",
  INSPECTION: "검사",
  PIPING: "배관",
  WIRING: "배선",
};

export const EVENT_LABEL: Record<string, string> = {
  START: "시작",
  PROGRESS: "진행",
  COMPLETE: "완료",
};

export const ERROR_LABEL: Record<string, string> = {
  "E-LDR-0101": "광학창 오염",
  "E-LDR-0203": "모터 회전수 이상",
  "E-LDR-0311": "포인트클라우드 결측",
  "E-LDR-0402": "내부 과열",
  "E-NET-0007": "통신 지연",
};

export const ARTIFACT_LABEL: Record<string, string> = {
  REGISTERED_PCD: "정합 PCD",
  TRANSFORMATION_MATRIX: "변환 행렬",
  SEGMENTED_PCD: "세그먼트 PCD",
};

// ── 판정 ──────────────────────────────────────────────────────────────────

/** 상태는 1초마다 온다. 이만큼 안 오면 수신 끊김으로 본다. */
export const STALE_MS = 5_000;
export const TEMP_WARN_C = 50;
export const TEMP_ALARM_C = 60;
export const RSSI_WARN_DBM = -85;

export type Severity = "critical" | "high" | "warn" | "info";

export const SEVERITY_RANK: Record<Severity, number> = { critical: 0, high: 1, warn: 2, info: 3 };

export interface DeviceState {
  device: string;
  bay: string | null;
  status?: StatusPayload;
  statusSince?: number; // 지금 status 값으로 바뀐 시각(수신 기준, ms)
  lastStatusAt?: number; // 마지막 status 수신 시각(ms)
  lastActual?: ActualPayload;
  lastActualAt?: number;
  artifacts: { type: string; payload: ArtifactPayload; at: number }[];
}

export interface Issue {
  device: string;
  bay: string | null;
  severity: Severity;
  kind: "offline" | "error" | "stale" | "silent" | "hot" | "warm" | "signal" | "calibrating";
  title: string;
  detail: string;
  since?: number;
}

export function issuesOf(d: DeviceState, now: number, connectedAt: number | null): Issue[] {
  const base = { device: d.device, bay: d.bay };
  const out: Issue[] = [];
  const s = d.status;

  if (!d.lastStatusAt) {
    // 접속 직후에는 아직 한 바퀴가 안 돌았을 수 있다
    if (connectedAt && now - connectedAt > STALE_MS) {
      out.push({ ...base, severity: "high", kind: "silent", title: "수신 없음", detail: "접속 후 상태를 한 번도 보내지 않았습니다" });
    }
    return out;
  }
  if (now - d.lastStatusAt > STALE_MS) {
    out.push({
      ...base,
      severity: "high",
      kind: "stale",
      title: "수신 끊김",
      detail: `마지막 상태 ${formatAgo(now - d.lastStatusAt)} 전`,
      since: d.lastStatusAt,
    });
  }
  if (!s) return out;

  if (s.status === "OFFLINE") {
    out.push({
      ...base,
      severity: "critical",
      kind: "offline",
      title: "OFFLINE",
      detail: `하트비트 멈춤 · 마지막 ${formatClock(s.last_heartbeat_at)}`,
      since: d.statusSince,
    });
  } else if (s.status === "ERROR") {
    const code = s.error_code ?? "코드 없음";
    out.push({
      ...base,
      severity: "critical",
      kind: "error",
      title: code,
      detail: ERROR_LABEL[code] ?? "알 수 없는 오류",
      since: d.statusSince,
    });
  } else if (s.status === "CALIBRATING") {
    out.push({ ...base, severity: "info", kind: "calibrating", title: "보정 중", detail: "스캔 속도가 잠시 낮을 수 있습니다", since: d.statusSince });
  }

  if (s.temperature_c >= TEMP_ALARM_C) {
    out.push({ ...base, severity: "high", kind: "hot", title: "과열", detail: `${s.temperature_c.toFixed(1)}℃`, since: undefined });
  } else if (s.temperature_c >= TEMP_WARN_C) {
    out.push({ ...base, severity: "warn", kind: "warm", title: "고온", detail: `${s.temperature_c.toFixed(1)}℃`, since: undefined });
  }
  if (s.connectivity_rssi <= RSSI_WARN_DBM) {
    out.push({ ...base, severity: "warn", kind: "signal", title: "신호 약함", detail: `${s.connectivity_rssi} dBm`, since: undefined });
  }
  return out;
}

/** 장비 하나를 대표하는 가장 나쁜 상태. 타일 색에 쓴다. */
export function worstOf(issues: Issue[]): Severity | null {
  let worst: Severity | null = null;
  for (const i of issues) {
    if (worst === null || SEVERITY_RANK[i.severity] < SEVERITY_RANK[worst]) worst = i.severity;
  }
  return worst;
}

// ── 표기 ──────────────────────────────────────────────────────────────────

export function formatAgo(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}초`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}분 ${s % 60}초`;
  return `${Math.floor(m / 60)}시간 ${m % 60}분`;
}

export function formatClock(iso: string | number | undefined): string {
  if (iso === undefined) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleTimeString("ko-KR", { hour12: false });
}

export function formatBytes(n: number | null | undefined): string {
  if (n == null) return "-";
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(1)} MB`;
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(0)} KB`;
  return `${n} B`;
}
