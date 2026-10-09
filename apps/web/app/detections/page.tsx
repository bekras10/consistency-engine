import { DetectionsView } from "@/components/detections-view";
import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";
import { queryString } from "@/lib/search";
import type { DetectionList } from "@/lib/types";

export const dynamic = "force-dynamic";

function one(value: string | string[] | undefined): string {
  return Array.isArray(value) ? (value[0] ?? "") : (value ?? "");
}

export default async function DetectionsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const sp = await searchParams;
  const filters = {
    classification: one(sp.classification),
    relationship_type: one(sp.relationship_type),
    since_ms: one(sp.since_ms),
    until_ms: one(sp.until_ms),
    min_net_edge: one(sp.min_net_edge),
    min_quantity: one(sp.min_quantity),
    market: one(sp.market),
    sort: one(sp.sort) || "time",
    direction: one(sp.direction) || "desc",
  };
  const result = await readGateway<DetectionList>(`/internal/detections${queryString(filters)}`);
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  return <DetectionsView initial={result.data} filters={filters} />;
}
