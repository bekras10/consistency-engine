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
const issuedByClient = new Map<string, number[]>();

export function clearCapabilities(): void {
  capabilities.clear();
  issuedAt.length = 0;
  issuedByClient.clear();
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

export function clientIssueLimit(): number {
  return positiveInt("REPLAY_CAPABILITY_CLIENT_LIMIT", 5);
}

export function issueWindowMs(): number {
  return positiveInt("REPLAY_CAPABILITY_ISSUE_WINDOW_S", 60) * 1000;
}

export function capabilityMaxClients(): number {
  return positiveInt("REPLAY_CAPABILITY_MAX_CLIENTS", 4096);
}

export function issuedClientCount(): number {
  return issuedByClient.size;
}

export function pruneExpired(now = Date.now()): void {
  for (const [id, cap] of capabilities) {
    if (cap.expiresAt <= now) capabilities.delete(id);
  }
  pruneIssuedClients(now);
}

function pruneIssuedClients(now: number): void {
  const windowMs = issueWindowMs();
  for (const [client, stamps] of issuedByClient) {
    const fresh = stamps.filter((stamp) => now - stamp <= windowMs);
    if (fresh.length === 0) issuedByClient.delete(client);
    else issuedByClient.set(client, fresh);
  }
}

export type IssueResult = { ok: true; setCookie: string } | { ok: false; error: "rate_limited" };

function siteKey(url: URL): string {
  const name = url.hostname;
  const loopback = name === "localhost" || name === "127.0.0.1" || name === "::1";
  const host = loopback ? "loopback" : name;
  const port = url.port || (url.protocol === "https:" ? "443" : "80");
  return `${url.protocol}//${host}:${port}`;
}

/**
 * Missing ``Origin`` is allowed. Non-browser clients (curl, server-side tools)
 * do not send it. Browsers send ``Origin`` on cross-site POST. The value must
 * match the request URL or the ``Host`` header. ``localhost`` and
 * ``127.0.0.1`` are the same local site on the same port: Next builds
 * ``request.url`` as ``localhost`` when the browser used ``127.0.0.1``.
 * ``null`` and unparsable values are rejected.
 */
export function replayOriginAllowed(request: Request): boolean {
  const origin = request.headers.get("origin");
  if (origin === null || origin.trim() === "") return true;
  if (origin.trim() === "null") return false;
  let presented: URL;
  try {
    presented = new URL(origin);
  } catch {
    return false;
  }
  if (presented.protocol !== "http:" && presented.protocol !== "https:") return false;
  if (siteKey(presented) === siteKey(new URL(request.url))) return true;
  const host = request.headers.get("host");
  if (!host) return false;
  try {
    return siteKey(presented) === siteKey(new URL(`${presented.protocol}//${host}`));
  } catch {
    return false;
  }
}

/**
 * TCP peer of this request, when the server attached a socket.
 *
 * A Fetch ``Request`` in a route handler has no socket. An empty string means
 * the peer is unknown, and ``X-Forwarded-For`` is ignored.
 */
export function requestPeer(request: Request): string {
  const withSocket = request as Request & { socket?: { remoteAddress?: string | null } };
  const addr = withSocket.socket?.remoteAddress;
  return typeof addr === "string" ? addr.trim() : "";
}

/**
 * Client key for issuance throttling.
 *
 * ``X-Forwarded-For`` is ignored unless ``TRUSTED_PROXY_COUNT`` is at least 1,
 * ``TRUSTED_PROXIES`` lists the immediate TCP peer, and that peer was passed in.
 * The key is then the address that many hops from the right (the hop a trusted
 * proxy appended). Entries further left are client-controlled. A missing or
 * unlisted peer shares the key ``direct``, so rotating the header does not open
 * a new bucket.
 */
export function capabilityClientKey(request: Request, peer?: string | null): string {
  const proxies = readEnv("TRUSTED_PROXIES")
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  const rawCount = readEnv("TRUSTED_PROXY_COUNT").trim();
  const count = Number(rawCount);
  const hop = typeof peer === "string" ? peer.trim() : "";
  const trusted =
    proxies.length > 0 &&
    rawCount !== "" &&
    Number.isFinite(count) &&
    count >= 1 &&
    hop.length > 0 &&
    proxies.includes(hop);
  if (!trusted) return "direct";
  const parts = (request.headers.get("x-forwarded-for") ?? "")
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  if (parts.length < count) return "direct";
  return parts[parts.length - count] ?? "direct";
}

export function issueCapability(
  now = Date.now(),
  opts?: { secure?: boolean; client?: string },
): IssueResult {
  pruneExpired(now);
  const windowMs = issueWindowMs();
  while (issuedAt.length > 0 && now - issuedAt[0] > windowMs) issuedAt.shift();
  const client = opts?.client && opts.client.length > 0 ? opts.client : "direct";
  const known = issuedByClient.has(client);
  const mine = issuedByClient.get(client) ?? [];
  if (
    (!known && issuedByClient.size >= capabilityMaxClients()) ||
    mine.length >= clientIssueLimit() ||
    issuedAt.length >= issueLimit() ||
    capabilities.size >= capabilityMax()
  ) {
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
  mine.push(now);
  issuedByClient.set(client, mine);
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
