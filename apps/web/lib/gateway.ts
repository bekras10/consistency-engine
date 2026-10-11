import type { Envelope } from "@/lib/types";

function readEnv(name: string): string {
  const value = process.env[name];
  return typeof value === "string" ? value : "";
}

/** Runtime URL. A direct ``process.env.NAME`` read is inlined by ``next build``. */
export function gatewayBase(): string {
  const configured = readEnv("DASHBOARD_GATEWAY_URL").trim();
  return configured || "http://127.0.0.1:8765";
}

export async function readGateway<T>(path: string, init?: RequestInit): Promise<Envelope<T>> {
  try {
    const response = await fetch(`${gatewayBase()}${path}`, { ...init, cache: "no-store" });
    const body = (await response.json()) as {
      ok?: boolean;
      data?: T;
      error?: string;
      detail?: string;
    };
    if (!response.ok || body.ok !== true || body.data === undefined) {
      return {
        ok: false,
        error: body.error ?? "database_unavailable",
        detail: body.detail,
      };
    }
    return { ok: true, data: body.data };
  } catch {
    return {
      ok: false,
      error: "database_unavailable",
      detail: "The dashboard gateway did not respond. No rows were invented.",
    };
  }
}
