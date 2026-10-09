"use client";

import { useQuery } from "@tanstack/react-query";

import { PollLabel } from "@/components/states";
import type { Envelope } from "@/lib/types";

export function usePolled<T>(path: string, initial: T, intervalMs = 5000) {
  const query = useQuery({
    queryKey: ["app-data", path],
    queryFn: async (): Promise<Envelope<T>> => {
      const response = await fetch(`/app-data${path}`, { cache: "no-store" });
      return (await response.json()) as Envelope<T>;
    },
    initialData: { ok: true, data: initial } satisfies Envelope<T>,
    refetchInterval: intervalMs,
  });
  return {
    data: query.data.ok ? query.data.data : initial,
    error: query.data.ok ? null : query.data.error,
    fetching: query.isFetching,
  };
}

export function PollStatus({ error }: { error: string | null }) {
  return (
    <div className="space-y-1">
      <PollLabel />
      {error ? (
        <p className="text-xs text-[#f0b4b4]">
          The latest poll failed ({error}). The table still shows the last successful read and does
          not fill in missing rows.
        </p>
      ) : null}
    </div>
  );
}
