import { gatewayBase } from "@/lib/gateway";

export const dynamic = "force-dynamic";

async function proxy(request: Request, path: string[]): Promise<Response> {
  if (path.some((part) => part === ".." || part === "")) {
    return Response.json({ ok: false, error: "invalid_input" }, { status: 400 });
  }
  const incoming = new URL(request.url);
  const target = `${gatewayBase()}/internal/${path.join("/")}${incoming.search}`;
  try {
    const init: RequestInit = { method: request.method, cache: "no-store" };
    if (request.method !== "GET" && request.method !== "HEAD") {
      init.body = await request.text();
      init.headers = { "content-type": "application/json" };
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
