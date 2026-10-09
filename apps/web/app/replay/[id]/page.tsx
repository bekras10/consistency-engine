import { ReplayDesk } from "@/components/replay-desk";
import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";
import type { ReplaySnapshot } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function ReplayPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const result = await readGateway<ReplaySnapshot>(`/internal/replay/${encodeURIComponent(id)}`);
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  return (
    <div className="space-y-3">
      <h1 className="text-xl font-medium">Replay</h1>
      <p className="text-sm text-[#9aa0a6]">
        Controls call the in-process PlaybackService. Playback is polled. Phase 11 has not added a stream.
      </p>
      <ReplayDesk replayId={id} initial={result.data} />
    </div>
  );
}
