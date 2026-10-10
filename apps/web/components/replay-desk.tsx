"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useRef, useState } from "react";

import { DepthChart } from "@/components/depth-chart";
import { Money } from "@/components/money";
import { PollLabel } from "@/components/states";
import { Button } from "@/components/ui/button";
import type { Envelope, ReplaySnapshot } from "@/lib/types";

const speeds = ["0.5", "1", "2", "5", "10"];

async function send(
  replayId: string,
  action: string,
  replayToken: string,
  body?: Record<string, string | number>,
) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (replayToken) headers["x-replay-token"] = replayToken;
  const response = await fetch(`/app-data/replay/${encodeURIComponent(replayId)}/${action}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body ?? {}),
  });
  return (await response.json()) as Envelope<ReplaySnapshot>;
}

export function ReplayDesk({
  replayId,
  initial,
  replayToken,
}: {
  replayId: string;
  initial: ReplaySnapshot;
  replayToken: string;
}) {
  const client = useQueryClient();
  const viewerRef = useRef(replayId);
  const [viewerId, setViewerId] = useState(replayId);
  const [seek, setSeek] = useState(initial.position_ms == null ? "" : String(initial.position_ms));
  const [note, setNote] = useState<string | null>(null);
  const query = useQuery({
    queryKey: ["replay", viewerId],
    queryFn: async () => {
      const response = await fetch(`/app-data/replay/${encodeURIComponent(viewerId)}`, { cache: "no-store" });
      return (await response.json()) as Envelope<ReplaySnapshot>;
    },
    initialData: { ok: true, data: initial } satisfies Envelope<ReplaySnapshot>,
    refetchInterval: (current) => (current.state.data?.ok && current.state.data.data.status === "playing" ? 1000 : 5000),
  });
  const data = query.data.ok ? query.data.data : initial;

  async function run(action: string, body?: Record<string, string | number>) {
    const result = await send(viewerRef.current, action, replayToken, body);
    if (!result.ok) {
      setNote(result.detail ?? result.error);
      return;
    }
    setNote(null);
    if (result.data.replay_id && result.data.replay_id !== viewerRef.current) {
      viewerRef.current = result.data.replay_id;
      setViewerId(result.data.replay_id);
    }
    client.setQueryData(["replay", viewerRef.current], result);
  }

  const focus = data.books.find((book) => book.yes_bids.length > 0 || book.yes_asks.length > 0) ?? data.books[0];
  return (
    <div className="space-y-4" data-testid="replay">
      <PollLabel />
      <div className="flex flex-wrap items-center gap-2">
        <Button data-testid="replay-start" type="button" onClick={() => run("start")}>Start</Button>
        <Button data-testid="replay-pause" type="button" onClick={() => run("pause")}>Pause</Button>
        <Button type="button" onClick={() => run("resume")}>Resume</Button>
        <Button type="button" onClick={() => run("restart")}>Restart</Button>
        <Button type="button" onClick={() => run("step")}>Step</Button>
        <label className="text-sm text-[#9aa0a6]">
          Speed
          <select
            aria-label="Speed"
            className="ml-2 h-8 rounded-md border border-[#3a4046] bg-[#101214] px-2"
            value={data.speed}
            onChange={(event) => run("speed", { speed: event.target.value })}
          >
            {speeds.map((speed) => (
              <option key={speed} value={speed}>{speed}</option>
            ))}
          </select>
        </label>
      </div>
      <form
        className="flex flex-wrap items-center gap-2"
        onSubmit={(event) => {
          event.preventDefault();
          const timestamp = Number(seek);
          if (!Number.isInteger(timestamp)) {
            setNote("Seek needs an integer pipeline timestamp in milliseconds.");
            return;
          }
          void run("seek", { timestamp_ms: timestamp });
        }}
      >
        <label className="text-sm text-[#9aa0a6]">
          Seek ms
          <input
            data-testid="replay-seek"
            className="ml-2 h-8 rounded-md border border-[#3a4046] bg-[#101214] px-2 font-mono"
            value={seek}
            onChange={(event) => setSeek(event.target.value)}
          />
        </label>
        <Button type="submit">Seek</Button>
        <span className="text-xs text-[#9aa0a6]">
          Range {data.first_ms ?? "—"} to {data.last_ms ?? "—"}
        </span>
      </form>
      {note ? <p className="text-sm text-[#f0b4b4]">{note}</p> : null}
      <p className="font-mono text-sm" data-testid="replay-status">
        {data.status} · cursor {data.cursor}/{data.entry_count} · position {data.position_ms ?? "—"} · speed {data.speed}
      </p>
      <section>
        <h2 className="text-sm font-medium">Detection state</h2>
        {data.detections.length === 0 ? (
          <p className="text-sm text-[#9aa0a6]">No active detection at this cursor.</p>
        ) : (
          <ul className="mt-2 space-y-1 text-sm">
            {data.detections.map((row) => (
              <li key={row.detection_id}>
                {row.relationship_id} · {row.status} · {row.classification} · net <Money value={row.net_edge} /> · max <Money value={row.max_net_edge} /> · deviation <Money value={row.theoretical_deviation} />
              </li>
            ))}
          </ul>
        )}
      </section>
      <section>
        <h2 className="text-sm font-medium">Book</h2>
        {focus ? (
          <>
            <p className="text-sm">
              {focus.market_id} · {focus.sync_status} · YES <Money value={focus.best_yes_bid} /> / <Money value={focus.best_yes_ask} />
            </p>
            <DepthChart bids={focus.yes_bids} asks={focus.yes_asks} />
          </>
        ) : (
          <p className="text-sm text-[#9aa0a6]">The replay has not applied a book yet.</p>
        )}
      </section>
      <section>
        <h2 className="text-sm font-medium">Timeline</h2>
        <ol className="mt-2 max-h-48 space-y-1 overflow-auto font-mono text-xs">
          {data.timeline.map((point) => (
            <li key={point.ordinal} className={point.ordinal + 1 === data.cursor ? "text-[#f0d48a]" : ""}>
              {point.ordinal} {point.now_ms} {point.kind}
            </li>
          ))}
        </ol>
      </section>
      <section>
        <h2 className="text-sm font-medium">Recent lifecycle events</h2>
        <ul className="mt-2 font-mono text-xs">
          {data.events.map((event, index) => (
            <li key={`${event.detection_id}-${index}`}>
              {event.at_ms} {event.kind} {event.classification ?? ""}
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
