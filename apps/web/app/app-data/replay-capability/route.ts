import { issueCapability } from "@/lib/replay-capability";

export const dynamic = "force-dynamic";

function requestIsSecure(request: Request): boolean {
  if (process.env.REPLAY_COOKIE_SECURE === "1") return true;
  return new URL(request.url).protocol === "https:";
}

/** Issue an httpOnly viewer capability. The body is not a credential. */
export async function POST(request: Request) {
  const issued = issueCapability(Date.now(), { secure: requestIsSecure(request) });
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
