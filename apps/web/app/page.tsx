import { OverviewView } from "@/components/overview-view";
import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";
import type { Overview } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  const result = await readGateway<Overview>("/internal/overview");
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  return <OverviewView initial={result.data} />;
}
