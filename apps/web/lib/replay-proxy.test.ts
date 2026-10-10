import http from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";

import { POST, GET } from "../app/app-data/[...path]/route";

const saved = {
  token: process.env.REPLAY_API_TOKEN,
  flag: process.env.REPLAY_MUTATIONS_PUBLIC,
  gateway: process.env.DASHBOARD_GATEWAY_URL,
};

afterEach(() => {
  restore("REPLAY_API_TOKEN", saved.token);
  restore("REPLAY_MUTATIONS_PUBLIC", saved.flag);
  restore("DASHBOARD_GATEWAY_URL", saved.gateway);
});

function restore(name: string, value: string | undefined) {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}

function call(method: "GET" | "POST", path: string[], token?: string, body?: unknown) {
  const headers = new Headers({ "content-type": "application/json" });
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

  it("lets two authorized viewers seek independently", async () => {
    process.env.REPLAY_MUTATIONS_PUBLIC = "false";
    process.env.REPLAY_API_TOKEN = "proxy-token";
    const viewers = new Map<string, number>();
    let seq = 0;
    const server = http.createServer((req, res) => {
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
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address() as AddressInfo;
    process.env.DASHBOARD_GATEWAY_URL = `http://127.0.0.1:${address.port}`;
    try {
      const first = await call("POST", ["replay", "recording", "start"], "proxy-token");
      const second = await call("POST", ["replay", "recording", "start"], "proxy-token");
      expect(first.status).toBe(200);
      expect(second.status).toBe(200);
      const firstId = ((await first.json()) as { data: { replay_id: string } }).data.replay_id;
      const secondId = ((await second.json()) as { data: { replay_id: string } }).data.replay_id;
      expect(firstId).not.toBe(secondId);
      expect(firstId).not.toBe("recording");
      const early = await call("POST", ["replay", firstId, "seek"], "proxy-token", { timestamp_ms: 1 });
      const late = await call("POST", ["replay", secondId, "seek"], "proxy-token", { timestamp_ms: 9 });
      expect(((await early.json()) as { data: { cursor: number } }).data.cursor).toBe(1);
      expect(((await late.json()) as { data: { cursor: number } }).data.cursor).toBe(9);
      const againEarly = await call("GET", ["replay", firstId]);
      const againLate = await call("GET", ["replay", secondId]);
      expect(((await againEarly.json()) as { data: { cursor: number } }).data.cursor).toBe(1);
      expect(((await againLate.json()) as { data: { cursor: number } }).data.cursor).toBe(9);
    } finally {
      await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
    }
  });
});
