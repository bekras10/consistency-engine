import { issueCapability } from "@/lib/replay-capability";

export const dynamic = "force-dynamic";

/** Issue an httpOnly viewer capability. The body is not a credential. */
export async function POST() {
  const issued = issueCapability();
  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
      "set-cookie": issued.setCookie,
    },
  });
}
