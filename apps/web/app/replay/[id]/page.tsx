import { ReplayDesk } from "@/components/replay-desk";
import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";
import type { ReplaySnapshot } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function ReplayPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const result = await readGateway<ReplaySnapshot>(`/internal/replay/${encodeURIComponent(id)}`);
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  const source = result.data.source_label ?? "";
  const synthetic = source.startsWith("synthetic") || source.startsWith("replay:");
  return (
    <div className="space-y-3">
      <h1 className="text-xl font-medium">Replay</h1>
      <p data-testid="synthetic-replay" className="rounded-md border border-[#8a6a22] bg-[#2a2416] px-3 py-2 text-sm text-[#f0d48a]">
        {synthetic
          ? "Synthetic recording replay. This cursor plays the archived journal. It is not a live Kalshi feed."
          : "Archived journal playback. This is not a live Kalshi feed."}
      </p>
      <p className="text-sm text-[#9aa0a6]">
        Controls call the in-process PlaybackService. Each start opens a separate viewer. Playback is polled.
      </p>
      <ReplayDesk replayId={id} initial={result.data} />
    </div>
  );
}
