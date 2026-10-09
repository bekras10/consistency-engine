import { MarketsView } from "@/components/markets-view";
import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";
import type { MarketListItem } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function MarketsPage() {
  const result = await readGateway<{ markets: MarketListItem[] }>("/internal/markets");
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  return <MarketsView initial={result.data} />;
}
