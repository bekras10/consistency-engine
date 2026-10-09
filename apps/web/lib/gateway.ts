import type { Envelope } from "@/lib/types";

export function gatewayBase(): string {
  return process.env.DASHBOARD_GATEWAY_URL ?? "http://127.0.0.1:8765";
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
