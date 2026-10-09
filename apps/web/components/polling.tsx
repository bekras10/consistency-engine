"use client";

import { useRef } from "react";
import { useQuery } from "@tanstack/react-query";

import { PollLabel } from "@/components/states";
import { reducePoll, type PollConnection, type PollSnapshot } from "@/lib/poll";
import type { Envelope } from "@/lib/types";

export function usePolled<T>(path: string, initial: T, intervalMs = 5000) {
  const snapshot = useRef<PollSnapshot<T>>({
    data: initial,
    initial,
    error: null,
    status: "connected",
    hasSuccess: false,
  });
  const query = useQuery({
    queryKey: ["app-data", path],
    queryFn: async (): Promise<Envelope<T>> => {
      const response = await fetch(`/app-data${path}`, { cache: "no-store" });
      return (await response.json()) as Envelope<T>;
    },
    initialData: { ok: true, data: initial } satisfies Envelope<T>,
    refetchInterval: intervalMs,
  });
  const envelope: Envelope<T> = query.isError
    ? { ok: false, error: "request_failed" }
    : query.data;
  snapshot.current = reducePoll(snapshot.current, envelope);
  return {
    data: snapshot.current.data,
    error: snapshot.current.error,
    status: snapshot.current.status,
    fetching: query.isFetching,
  };
}

export type { PollConnection };

export function PollStatus({
  error,
  status = "connected",
}: {
  error: string | null;
  status?: PollConnection;
}) {
  return (
    <div className="space-y-1">
      <PollLabel />
      <p className="sr-only" data-testid="connection-status">
        {status}
      </p>
      {status === "stale" && error ? (
        <p className="text-xs text-[#f0b4b4]">
          The latest poll failed ({error}). The table still shows the last successful read and does
          not fill in missing rows.
        </p>
      ) : null}
      {status === "error" && error ? (
        <p className="text-xs text-[#f0b4b4]">
          The latest poll failed ({error}). No successful read is available to keep on screen.
        </p>
      ) : null}
    </div>
  );
}
