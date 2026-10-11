import { capabilityClientKey, issueCapability, replayOriginAllowed } from "@/lib/replay-capability";

export const dynamic = "force-dynamic";

function requestIsSecure(request: Request): boolean {
  const flag = process.env["REPLAY_COOKIE_SECURE"];
  if (flag === "1") return true;
  return new URL(request.url).protocol === "https:";
}

/** Issue an httpOnly viewer capability. The body is not a credential. */
export async function POST(request: Request) {
  if (!replayOriginAllowed(request)) {
    return new Response(JSON.stringify({ error: "cross_origin" }), {
      status: 403,
      headers: { "content-type": "application/json", "cache-control": "no-store" },
    });
  }
  const issued = issueCapability(Date.now(), {
    secure: requestIsSecure(request),
    client: capabilityClientKey(request),
  });
  if (!issued.ok) {
    return new Response(JSON.stringify({ error: "rate_limited" }), {
      status: 429,
      headers: { "content-type": "application/json", "cache-control": "no-store" },
    });
  }
  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
      "set-cookie": issued.setCookie,
    },
  });
}
