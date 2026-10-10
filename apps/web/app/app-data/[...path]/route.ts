import { gatewayBase } from "@/lib/gateway";
import {
  authorizeReplayMutation,
  bindReplayCapability,
} from "@/lib/replay-capability";
import { expectedReplayToken } from "@/lib/replay-auth";

export const dynamic = "force-dynamic";

function isReplayMutation(method: string, path: string[]): boolean {
  return method === "POST" && path[0] === "replay";
}

async function proxy(request: Request, path: string[]): Promise<Response> {
  if (path.some((part) => part === ".." || part === "")) {
    return Response.json({ ok: false, error: "invalid_input" }, { status: 400 });
  }
  let capabilityId: string | null = null;
  let upstreamPath = path;
  if (isReplayMutation(request.method, path)) {
    const gate = authorizeReplayMutation(request.headers.get("cookie"), path);
    if (!gate.ok) {
      return Response.json({ error: gate.error }, { status: gate.status });
    }
    capabilityId = gate.id;
    upstreamPath = gate.path;
  }
  const incoming = new URL(request.url);
  const target = `${gatewayBase()}/internal/${upstreamPath.join("/")}${incoming.search}`;
  try {
    const init: RequestInit = { method: request.method, cache: "no-store" };
    if (request.method !== "GET" && request.method !== "HEAD") {
      const headers: Record<string, string> = { "content-type": "application/json" };
      if (isReplayMutation(request.method, path)) {
        const token = expectedReplayToken();
        if (token) headers["x-replay-token"] = token;
      }
      init.body = await request.text();
      init.headers = headers;
    }
    const response = await fetch(target, init);
    const text = await response.text();
    if (capabilityId && response.ok) {
      bindFromBody(capabilityId, path[1] ?? "", text);
    }
    return new Response(text, {
      status: response.status,
      headers: { "content-type": "application/json", "cache-control": "no-store" },
    });
  } catch {
    return Response.json(
      {
        ok: false,
        error: "database_unavailable",
        detail: "The dashboard gateway did not respond. No rows were invented.",
      },
      { status: 503 },
    );
  }
}

function bindFromBody(capabilityId: string, requestedId: string, text: string) {
  try {
    const parsed = JSON.parse(text) as { data?: { replay_id?: unknown } };
    const replayId = parsed.data?.replay_id;
    if (typeof replayId === "string" && replayId) {
      bindReplayCapability(capabilityId, requestedId, replayId);
    }
  } catch {
    // A non-JSON gateway body does not bind a viewer.
  }
}

export async function GET(request: Request, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return proxy(request, path);
}

export async function POST(request: Request, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return proxy(request, path);
}
