"use client";

import Link from "next/link";

import { ActivityChart } from "@/components/activity-chart";
import { DetectionTable } from "@/components/detection-table";
import { PollStatus, usePolled } from "@/components/polling";
import { EmptyState, SyntheticBanner } from "@/components/states";
import { Card } from "@/components/ui/card";
import type { Overview } from "@/lib/types";

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <Card className="px-3 py-2">
      <p className="text-[11px] uppercase tracking-wide text-[#9aa0a6]">{label}</p>
      <p className="mt-1 font-mono text-lg tabular-nums">{value}</p>
    </Card>
  );
}

export function OverviewView({ initial }: { initial: Overview }) {
  const { data, error, status } = usePolled<Overview>("/overview", initial);
  const latency = data.internal_latency_ns;
  const sync = data.sync_health;
  return (
    <div className="space-y-4" data-testid="overview">
      <PollStatus error={error} status={status} />
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-xl font-medium">Overview</h1>
          <p className="text-sm text-[#9aa0a6]">
            Source {data.data_source.label ?? "none"} · session {data.data_source.session_status ?? "none"}
          </p>
        </div>
        {data.data_source.session_id ? (
          <Link className="text-sm underline" href={`/replay/${encodeURIComponent(data.data_source.session_id)}`}>
            Replay latest session
          </Link>
        ) : null}
      </div>
      <SyntheticBanner show={data.data_source.synthetic} />
      {data.empty ? <EmptyState hint={data.seed_hint} /> : null}
      <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
        <Stat label="Markets monitored" value={String(data.markets_monitored)} />
        <Stat label="Verified relationships" value={String(data.relationships_verified)} />
        <Stat label="Detections" value={String(data.detections_total)} />
        <Stat label="Open detections" value={String(data.detections_open)} />
      </div>
      <div className="grid gap-3 md:grid-cols-2">
        <Card className="p-3">
          <h2 className="text-sm font-medium">Source health</h2>
          <p className="mt-2 text-sm text-[#c5cad0]">
            {data.source_health
              ? `${data.source_health.component}: ${data.source_health.status} at ${data.source_health.checked_at}`
              : "No system_health row is stored."}
          </p>
          <p className="mt-2 text-sm text-[#c5cad0]">
            Sync{" "}
            {sync
              ? `${String(sync.synchronized ?? "—")} synchronized / ${String(sync.total ?? "—")} books`
              : "No journal book state is available."}
          </p>
        </Card>
        <Card className="p-3">
          <h2 className="text-sm font-medium">Internal processing latency</h2>
          {latency.stored ? (
            <p className="mt-2 font-mono text-sm">
              p50 {latency.p50} ns · p95 {latency.p95} ns · {latency.samples} stored samples
            </p>
          ) : (
            <p className="mt-2 text-sm text-[#9aa0a6]">
              p50/p95 are not shown. These detection rows do not store both processing timestamps.
            </p>
          )}
        </Card>
      </div>
      <Card className="p-3">
        <h2 className="mb-2 text-sm font-medium">Detection activity</h2>
        <ActivityChart points={data.activity} />
      </Card>
      <Card className="p-3">
        <h2 className="mb-2 text-sm font-medium">Recent detections</h2>
        {data.recent_detections.length === 0 ? (
          <p className="text-sm text-[#9aa0a6]">No detections are stored.</p>
        ) : (
          <DetectionTable rows={data.recent_detections} />
        )}
      </Card>
    </div>
  );
}
