import { randomBytes } from "node:crypto";

/** HttpOnly capability for one dashboard viewer. Not the shared ``REPLAY_API_TOKEN``. */

export const CAPABILITY_COOKIE = "ce_replay_capability";

const PLAYBACK_ACTIONS = new Set([
  "start",
  "pause",
  "resume",
  "seek",
  "restart",
  "step",
  "speed",
]);

type Capability = {
  recordingId: string | null;
  viewerId: string | null;
  expiresAt: number;
};

const capabilities = new Map<string, Capability>();
const issuedAt: number[] = [];

export function clearCapabilities(): void {
  capabilities.clear();
  issuedAt.length = 0;
}

export function outstandingCapabilities(): number {
  return capabilities.size;
}

export function capabilityTtlSeconds(): number {
  const raw = readEnv("REPLAY_CAPABILITY_TTL_S").trim();
  const parsed = Number(raw);
  if (!raw || !Number.isFinite(parsed) || parsed < 1) return 900;
  return Math.floor(parsed);
}

function positiveInt(name: string, fallback: number): number {
  const raw = readEnv(name).trim();
  const parsed = Number(raw);
  if (!raw || !Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.floor(parsed);
}

export function capabilityMax(): number {
  return positiveInt("REPLAY_CAPABILITY_MAX", 256);
}

export function issueLimit(): number {
  return positiveInt("REPLAY_CAPABILITY_ISSUE_LIMIT", 30);
}

export function issueWindowMs(): number {
  return positiveInt("REPLAY_CAPABILITY_ISSUE_WINDOW_S", 60) * 1000;
}

export function pruneExpired(now = Date.now()): void {
  for (const [id, cap] of capabilities) {
    if (cap.expiresAt <= now) capabilities.delete(id);
  }
}

export type IssueResult = { ok: true; setCookie: string } | { ok: false; error: "rate_limited" };

export function issueCapability(now = Date.now(), opts?: { secure?: boolean }): IssueResult {
  pruneExpired(now);
  const windowMs = issueWindowMs();
  while (issuedAt.length > 0 && now - issuedAt[0] > windowMs) issuedAt.shift();
  if (issuedAt.length >= issueLimit() || capabilities.size >= capabilityMax()) {
    return { ok: false, error: "rate_limited" };
  }
  const id = randomBytes(32).toString("base64url");
  const ttl = capabilityTtlSeconds();
  capabilities.set(id, {
    recordingId: null,
    viewerId: null,
    expiresAt: now + ttl * 1000,
  });
  issuedAt.push(now);
  const flags = ["HttpOnly", "SameSite=Lax", "Path=/", `Max-Age=${ttl}`];
  if (opts?.secure) flags.splice(1, 0, "Secure");
  const setCookie = `${CAPABILITY_COOKIE}=${id}; ${flags.join("; ")}`;
  return { ok: true, setCookie };
}

export function readCapabilityId(cookieHeader: string | null): string {
  if (!cookieHeader) return "";
  for (const part of cookieHeader.split(";")) {
    const trimmed = part.trim();
    const eq = trimmed.indexOf("=");
    if (eq <= 0) continue;
    if (trimmed.slice(0, eq) === CAPABILITY_COOKIE) return decodeURIComponent(trimmed.slice(eq + 1));
  }
  return "";
}

export type ReplayGate =
  | { ok: true; id: string; path: string[] }
  | { ok: false; status: 401 | 403; error: "unauthorized" | "replay_forbidden" };

/**
 * A capability with no viewer may only start a recording. After start binds a
 * viewer id, that capability may command only that viewer's playback actions.
 * It does not authorize retention, configuration, or any other mutation.
 * ``viewer-`` ids are the fork prefix from ``ReplayHost``.
 */
export function authorizeReplayMutation(
  cookieHeader: string | null,
  path: string[],
  now = Date.now(),
): ReplayGate {
  const id = readCapabilityId(cookieHeader);
  const cap = id ? capabilities.get(id) : undefined;
  if (!id || !cap || cap.expiresAt <= now) {
    if (id) capabilities.delete(id);
    return { ok: false, status: 401, error: "unauthorized" };
  }
  if (path[0] !== "replay" || !PLAYBACK_ACTIONS.has(path[2] ?? "")) {
    return { ok: false, status: 403, error: "replay_forbidden" };
  }
  const target = path[1] ?? "";
  const action = path[2] ?? "";
  if (cap.viewerId === null) {
    if (action !== "start" || target.startsWith("viewer-")) {
      return { ok: false, status: 403, error: "replay_forbidden" };
    }
    return { ok: true, id, path };
  }
  if (target === cap.viewerId) return { ok: true, id, path };
  if (cap.recordingId !== null && target === cap.recordingId) {
    return { ok: true, id, path: [path[0] ?? "replay", cap.viewerId, ...path.slice(2)] };
  }
  return { ok: false, status: 403, error: "replay_forbidden" };
}

export function bindReplayCapability(id: string, requestedId: string, replayId: string): void {
  const cap = capabilities.get(id);
  if (!cap) return;
  if (cap.recordingId === null) cap.recordingId = requestedId;
  if (replayId.startsWith("viewer-")) cap.viewerId = replayId;
}

function readEnv(name: string): string {
  const value = process.env[name];
  return typeof value === "string" ? value : "";
}
