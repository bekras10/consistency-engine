import { randomBytes } from "node:crypto";

/** HttpOnly capability for one dashboard viewer. Not the shared ``REPLAY_API_TOKEN``. */

export const CAPABILITY_COOKIE = "ce_replay_capability";

type Capability = {
  recordingId: string | null;
  viewerId: string | null;
  expiresAt: number;
};

const capabilities = new Map<string, Capability>();

export function clearCapabilities(): void {
  capabilities.clear();
}

export function capabilityTtlSeconds(): number {
  const raw = readEnv("REPLAY_CAPABILITY_TTL_S").trim();
  const parsed = Number(raw);
  if (!raw || !Number.isFinite(parsed) || parsed < 1) return 900;
  return Math.floor(parsed);
}

export function issueCapability(now = Date.now()): { setCookie: string } {
  const id = randomBytes(32).toString("base64url");
  const ttl = capabilityTtlSeconds();
  capabilities.set(id, {
    recordingId: null,
    viewerId: null,
    expiresAt: now + ttl * 1000,
  });
  const setCookie = `${CAPABILITY_COOKIE}=${id}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${ttl}`;
  return { setCookie };
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
 * viewer id, that capability may command only that viewer. ``viewer-`` ids are
 * the fork prefix from ``ReplayHost``.
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
