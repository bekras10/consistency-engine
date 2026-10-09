import { RelationshipsView } from "@/components/relationships-view";
import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";
import { queryString } from "@/lib/search";
import type { RelationshipList } from "@/lib/types";

export const dynamic = "force-dynamic";

function one(value: string | string[] | undefined): string {
  return Array.isArray(value) ? (value[0] ?? "") : (value ?? "");
}

export default async function RelationshipsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const sp = await searchParams;
  const filters = {
    type: one(sp.type),
    status: one(sp.status),
    category: one(sp.category),
    market: one(sp.market),
    min_members: one(sp.min_members),
    max_members: one(sp.max_members),
  };
  const result = await readGateway<RelationshipList>(`/internal/relationships${queryString(filters)}`);
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  return <RelationshipsView initial={result.data} filters={filters} />;
}
