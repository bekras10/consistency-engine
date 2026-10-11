import { afterEach, describe, expect, it } from "vitest";

import { POST as issueRoute } from "../app/app-data/replay-capability/route";
import { POST as proxy } from "../app/app-data/[...path]/route";
import {
  clearCapabilities,
  issueCapability,
  outstandingCapabilities,
  pruneExpired,
} from "./replay-capability";

const saved = {
  max: process.env.REPLAY_CAPABILITY_MAX,
  limit: process.env.REPLAY_CAPABILITY_ISSUE_LIMIT,
  window: process.env.REPLAY_CAPABILITY_ISSUE_WINDOW_S,
  ttl: process.env.REPLAY_CAPABILITY_TTL_S,
  secure: process.env.REPLAY_COOKIE_SECURE,
  gateway: process.env.DASHBOARD_GATEWAY_URL,
};

afterEach(() => {
  clearCapabilities();
  restore("REPLAY_CAPABILITY_MAX", saved.max);
  restore("REPLAY_CAPABILITY_ISSUE_LIMIT", saved.limit);
  restore("REPLAY_CAPABILITY_ISSUE_WINDOW_S", saved.window);
  restore("REPLAY_CAPABILITY_TTL_S", saved.ttl);
  restore("REPLAY_COOKIE_SECURE", saved.secure);
  restore("DASHBOARD_GATEWAY_URL", saved.gateway);
});

function restore(name: string, value: string | undefined) {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}

describe("replay capability limits", () => {
  it("caps outstanding capabilities", () => {
    process.env.REPLAY_CAPABILITY_MAX = "3";
    process.env.REPLAY_CAPABILITY_ISSUE_LIMIT = "100";
    const results = [issueCapability(1_000), issueCapability(1_001), issueCapability(1_002), issueCapability(1_003)];
    expect(results.slice(0, 3).every((item) => item.ok)).toBe(true);
    expect(results[3]).toEqual({ ok: false, error: "rate_limited" });
    expect(outstandingCapabilities()).toBe(3);
  });

  it("removes expired capabilities before issuing another", () => {
    process.env.REPLAY_CAPABILITY_TTL_S = "1";
    process.env.REPLAY_CAPABILITY_MAX = "1";
    const first = issueCapability(0);
    expect(first.ok).toBe(true);
    expect(outstandingCapabilities()).toBe(1);
    pruneExpired(1_000);
    expect(outstandingCapabilities()).toBe(0);
    const second = issueCapability(1_001);
    expect(second.ok).toBe(true);
    expect(outstandingCapabilities()).toBe(1);
  });

  it("sets HttpOnly, SameSite, and Secure when the request is HTTPS", async () => {
    const http = await issueRoute(
      new Request("http://dashboard.test/app-data/replay-capability", { method: "POST" }),
    );
    const httpCookie = http.headers.get("set-cookie") ?? "";
    expect(httpCookie).toContain("HttpOnly");
    expect(httpCookie).toContain("SameSite=Lax");
    expect(httpCookie).not.toContain("Secure");

    clearCapabilities();
    const https = await issueRoute(
      new Request("https://dashboard.test/app-data/replay-capability", { method: "POST" }),
    );
    const httpsCookie = https.headers.get("set-cookie") ?? "";
    expect(httpsCookie).toContain("HttpOnly");
    expect(httpsCookie).toContain("SameSite=Lax");
    expect(httpsCookie).toContain("Secure");

    clearCapabilities();
    process.env.REPLAY_COOKIE_SECURE = "1";
    const forced = await issueRoute(
      new Request("http://dashboard.test/app-data/replay-capability", { method: "POST" }),
    );
    expect(forced.headers.get("set-cookie") ?? "").toContain("Secure");
  });

  it("does not let a viewer capability call a non-replay mutation", async () => {
    process.env.DASHBOARD_GATEWAY_URL = "http://127.0.0.1:1";
    const issued = await issueRoute(
      new Request("http://dashboard.test/app-data/replay-capability", { method: "POST" }),
    );
    const cookie = (issued.headers.get("set-cookie") ?? "").split(";")[0] ?? "";
    expect(cookie.startsWith("ce_replay_capability=")).toBe(true);
    const headers = new Headers({ "content-type": "application/json", cookie });
    const retention = await proxy(
      new Request("http://dashboard.test/app-data/admin/retention", {
        method: "POST",
        headers,
        body: "{}",
      }),
      { params: Promise.resolve({ path: ["admin", "retention"] }) },
    );
    expect(retention.status).toBe(403);
    expect(await retention.json()).toEqual({ error: "replay_forbidden" });
    const wipe = await proxy(
      new Request("http://dashboard.test/app-data/replay/viewer-1/wipe", {
        method: "POST",
        headers,
        body: "{}",
      }),
      { params: Promise.resolve({ path: ["replay", "viewer-1", "wipe"] }) },
    );
    expect(wipe.status).toBe(403);
    expect(await wipe.json()).toEqual({ error: "replay_forbidden" });
  });
});
