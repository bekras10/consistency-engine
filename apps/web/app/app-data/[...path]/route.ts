import { gatewayBase } from "@/lib/gateway";
import { replayTokenAccepted } from "@/lib/replay-auth";

export const dynamic = "force-dynamic";

function isReplayMutation(method: string, path: string[]): boolean {
  return method === "POST" && path[0] === "replay";
}

async function proxy(request: Request, path: string[]): Promise<Response> {
  if (path.some((part) => part === ".." || part === "")) {
    return Response.json({ ok: false, error: "invalid_input" }, { status: 400 });
  }
  const presented = request.headers.get("x-replay-token") ?? "";
  if (isReplayMutation(request.method, path) && !replayTokenAccepted(presented)) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const incoming = new URL(request.url);
  const target = `${gatewayBase()}/internal/${path.join("/")}${incoming.search}`;
  try {
    const init: RequestInit = { method: request.method, cache: "no-store" };
    if (request.method !== "GET" && request.method !== "HEAD") {
      const headers: Record<string, string> = { "content-type": "application/json" };
      const token = request.headers.get("x-replay-token");
      if (token) headers["x-replay-token"] = token;
      init.body = await request.text();
      init.headers = headers;
    }
    const response = await fetch(target, init);
    const text = await response.text();
    return new Response(text, {
      status: response.status,
      headers: { "content-type": "application/json" },
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

export async function GET(request: Request, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return proxy(request, path);
}

export async function POST(request: Request, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return proxy(request, path);
}
