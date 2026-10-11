import { readFileSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, describe, expect, it } from "vitest";

import { POST as issueCapability } from "../app/app-data/replay-capability/route";
import { GET, POST } from "../app/app-data/[...path]/route";
import { clearCapabilities } from "./replay-capability";

const saved = {
  token: process.env.REPLAY_API_TOKEN,
  flag: process.env.REPLAY_MUTATIONS_PUBLIC,
  gateway: process.env.DASHBOARD_GATEWAY_URL,
  ttl: process.env.REPLAY_CAPABILITY_TTL_S,
};

afterEach(() => {
  clearCapabilities();
  restore("REPLAY_API_TOKEN", saved.token);
  restore("REPLAY_MUTATIONS_PUBLIC", saved.flag);
  restore("DASHBOARD_GATEWAY_URL", saved.gateway);
  restore("REPLAY_CAPABILITY_TTL_S", saved.ttl);
});

function restore(name: string, value: string | undefined) {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}

function cookieFrom(response: Response): string {
  const header = response.headers.get("set-cookie");
  if (!header) throw new Error("capability response did not set a cookie");
  const pair = header.split(";")[0] ?? "";
  if (!pair.startsWith("ce_replay_capability=")) throw new Error(`unexpected cookie ${header}`);
  return pair;
}

function issueRequest(): Request {
  return new Request("http://dashboard.test/app-data/replay-capability", { method: "POST" });
}

async function issue(): Promise<string> {
  const response = await issueCapability(issueRequest());
  return cookieFrom(response);
}

function call(
  method: "GET" | "POST",
  path: string[],
  cookie?: string,
  body?: unknown,
  token?: string,
) {
  const headers = new Headers({ "content-type": "application/json" });
  if (cookie) headers.set("cookie", cookie);
  if (token) headers.set("x-replay-token", token);
  const request = new Request(`http://dashboard.test/app-data/${path.join("/")}`, {
    method,
    headers,
    body: method === "POST" ? JSON.stringify(body ?? {}) : undefined,
  });
  const handler = method === "POST" ? POST : GET;
  return handler(request, { params: Promise.resolve({ path }) });
}

describe("replay proxy authentication", () => {
  it("rejects an unauthorized replay post before calling the gateway", async () => {
    process.env.REPLAY_MUTATIONS_PUBLIC = "false";
    delete process.env.REPLAY_API_TOKEN;
    process.env.DASHBOARD_GATEWAY_URL = "http://127.0.0.1:1";
    const response = await call("POST", ["replay", "ses", "start"]);
    expect(response.status).toBe(401);
    expect(await response.json()).toEqual({ error: "unauthorized" });
  });

  it("does not accept the shared token from the browser", async () => {
    process.env.REPLAY_MUTATIONS_PUBLIC = "false";
    process.env.REPLAY_API_TOKEN = "proxy-token";
    process.env.DASHBOARD_GATEWAY_URL = "http://127.0.0.1:1";
    const response = await call("POST", ["replay", "ses", "start"], undefined, {}, "proxy-token");
    expect(response.status).toBe(401);
    expect(JSON.stringify(await response.json())).not.toContain("proxy-token");
  });

  it("keeps the shared token out of capability and proxy responses", async () => {
    process.env.REPLAY_MUTATIONS_PUBLIC = "false";
    process.env.REPLAY_API_TOKEN = "proxy-token";
    const issued = await issueCapability(issueRequest());
    const issuedBody = await issued.text();
    expect(issued.status).toBe(200);
    expect(issuedBody).not.toContain("proxy-token");
    expect(issued.headers.get("set-cookie") ?? "").not.toContain("proxy-token");
    expect(issued.headers.get("set-cookie") ?? "").toContain("HttpOnly");
    const here = dirname(fileURLToPath(import.meta.url));
    const page = readFileSync(resolve(here, "../app/replay/[id]/page.tsx"), "utf8");
    const desk = readFileSync(resolve(here, "../components/replay-desk.tsx"), "utf8");
    expect(page).not.toContain("expectedReplayToken");
    expect(page).not.toContain("replayToken");
    expect(desk).not.toContain("x-replay-token");
    expect(desk).not.toContain("REPLAY_API_TOKEN");
  });

  it("lets two capabilities seek independently and refuses the other viewer", async () => {
    process.env.REPLAY_MUTATIONS_PUBLIC = "false";
    process.env.REPLAY_API_TOKEN = "proxy-token";
    const viewers = new Map<string, number>();
    const upstreamTokens: string[] = [];
    let seq = 0;
    const server = http.createServer((req, res) => {
      upstreamTokens.push(req.headers["x-replay-token"]?.toString() ?? "");
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      const parts = url.pathname.split("/").filter(Boolean);
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        const raw = Buffer.concat(chunks).toString("utf8");
        const payload = raw ? (JSON.parse(raw) as { timestamp_ms?: number }) : {};
        const id = parts[2] ?? "";
        const action = parts[3];
        let replayId = id;
        let cursor = viewers.get(id) ?? 0;
        if (action === "start") {
          seq += 1;
          replayId = `viewer-${seq}`;
          cursor = 0;
          viewers.set(replayId, cursor);
        } else if (action === "seek") {
          cursor = payload.timestamp_ms === 1 ? 1 : 9;
          viewers.set(id, cursor);
          replayId = id;
        } else if (req.method === "GET") {
          replayId = id;
          cursor = viewers.get(id) ?? 0;
        }
        const body = JSON.stringify({
          ok: true,
          data: { replay_id: replayId, cursor, status: "paused", position_ms: cursor },
        });
        res.writeHead(200, { "content-type": "application/json" });
        res.end(body);
      });
    });
    await new Promise<void>((resolveListen) => server.listen(0, "127.0.0.1", resolveListen));
    const address = server.address() as AddressInfo;
    process.env.DASHBOARD_GATEWAY_URL = `http://127.0.0.1:${address.port}`;
    try {
      const firstCookie = await issue();
      const secondCookie = await issue();
      const first = await call("POST", ["replay", "recording", "start"], firstCookie);
      const second = await call("POST", ["replay", "recording", "start"], secondCookie);
      expect(first.status).toBe(200);
      expect(second.status).toBe(200);
      expect(JSON.stringify(await first.clone().json())).not.toContain("proxy-token");
      const firstId = ((await first.json()) as { data: { replay_id: string } }).data.replay_id;
      const secondId = ((await second.json()) as { data: { replay_id: string } }).data.replay_id;
      expect(firstId).not.toBe(secondId);
      expect(firstId).not.toBe("recording");
      expect(upstreamTokens.every((header) => header === "proxy-token")).toBe(true);
      const early = await call("POST", ["replay", firstId, "seek"], firstCookie, { timestamp_ms: 1 });
      const late = await call("POST", ["replay", secondId, "seek"], secondCookie, { timestamp_ms: 9 });
      expect(((await early.json()) as { data: { cursor: number } }).data.cursor).toBe(1);
      expect(((await late.json()) as { data: { cursor: number } }).data.cursor).toBe(9);
      const crossed = await call("POST", ["replay", secondId, "seek"], firstCookie, { timestamp_ms: 1 });
      expect(crossed.status).toBe(403);
      expect(await crossed.json()).toEqual({ error: "replay_forbidden" });
      const againEarly = await call("GET", ["replay", firstId]);
      const againLate = await call("GET", ["replay", secondId]);
      const againEarlyBody = await againEarly.json();
      const againLateBody = await againLate.json();
      expect((againEarlyBody as { data: { cursor: number } }).data.cursor).toBe(1);
      expect((againLateBody as { data: { cursor: number } }).data.cursor).toBe(9);
      expect(JSON.stringify(againLateBody)).not.toContain("proxy-token");
    } finally {
      await new Promise<void>((resolveClose, reject) =>
        server.close((err) => (err ? reject(err) : resolveClose())),
      );
    }
  });
});
