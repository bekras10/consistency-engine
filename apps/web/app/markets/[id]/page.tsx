import { DepthChart } from "@/components/depth-chart";
import { Money } from "@/components/money";
import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";
import type { BookView } from "@/lib/types";

export const dynamic = "force-dynamic";

type MarketDetail = {
  market_id: string;
  ticker: string;
  title: string;
  status: string;
  category: string;
  data_source: string;
  rules_hash: string;
  settlement_rules: string | null;
  settlement_source: string | null;
  book: BookView | null;
};

export default async function MarketDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const result = await readGateway<MarketDetail>(`/internal/markets/${encodeURIComponent(id)}`);
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  const data = result.data;
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-medium">{data.ticker}</h1>
      <p className="text-sm text-[#c5cad0]">{data.title}</p>
      <p className="text-sm">
        {data.category} · {data.status} · source {data.data_source}
        {data.data_source === "synthetic" ? " · synthetic simulation" : ""}
      </p>
      <p className="text-sm">{data.settlement_rules ?? "No settlement text stored."}</p>
      <p className="text-xs text-[#9aa0a6]">
        Settlement source {data.settlement_source ?? "—"} · rules {data.rules_hash}
      </p>
      {data.book ? (
        <>
          <dl className="grid grid-cols-2 gap-2 text-sm md:grid-cols-4">
            <div><dt className="text-[#9aa0a6]">YES bid</dt><dd><Money value={data.book.best_yes_bid} /></dd></div>
            <div><dt className="text-[#9aa0a6]">YES ask</dt><dd><Money value={data.book.best_yes_ask} /></dd></div>
            <div><dt className="text-[#9aa0a6]">NO bid</dt><dd><Money value={data.book.best_no_bid} /></dd></div>
            <div><dt className="text-[#9aa0a6]">NO ask</dt><dd><Money value={data.book.best_no_ask} /></dd></div>
            <div><dt className="text-[#9aa0a6]">Spread</dt><dd><Money value={data.book.spread} /></dd></div>
            <div><dt className="text-[#9aa0a6]">YES depth</dt><dd><Money value={data.book.yes_depth} /></dd></div>
            <div><dt className="text-[#9aa0a6]">Sync</dt><dd>{data.book.sync_status}</dd></div>
            <div><dt className="text-[#9aa0a6]">Received ms</dt><dd className="font-mono">{data.book.received_ts_ms}</dd></div>
          </dl>
          <p className="text-xs text-[#9aa0a6]">{data.book.depth_note}</p>
          <DepthChart bids={data.book.yes_bids} asks={data.book.yes_asks} />
        </>
      ) : (
        <p className="text-sm text-[#9aa0a6]">No persisted book is available for this market.</p>
      )}
    </div>
  );
}
