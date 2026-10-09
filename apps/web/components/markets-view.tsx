"use client";

import Link from "next/link";

import { Money } from "@/components/money";
import { PollStatus, usePolled } from "@/components/polling";
import type { MarketListItem } from "@/lib/types";

export function MarketsView({ initial }: { initial: { markets: MarketListItem[] } }) {
  const { data, error } = usePolled<{ markets: MarketListItem[] }>("/markets", initial);
  return (
    <div className="space-y-4">
      <PollStatus error={error} />
      <h1 className="text-xl font-medium">Markets</h1>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[880px] text-left text-sm">
          <thead className="text-xs uppercase tracking-wide text-[#9aa0a6]">
            <tr>
              {["Market", "Category", "Status", "YES bid/ask", "NO bid/ask", "Spread", "Depth", "Source"].map((label) => (
                <th key={label} className="border-b border-[#2c3136] px-2 py-2 font-medium">{label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.markets.map((market) => (
              <tr key={market.market_id} className="border-b border-[#23282d]">
                <td className="px-2 py-1.5">
                  <Link className="underline" href={`/markets/${encodeURIComponent(market.market_id)}`}>
                    {market.ticker}
                  </Link>
                  <div className="text-xs text-[#9aa0a6]">{market.title}</div>
                </td>
                <td className="px-2 py-1.5">{market.category}</td>
                <td className="px-2 py-1.5">{market.status}</td>
                <td className="px-2 py-1.5">
                  <Money value={market.book?.best_yes_bid} /> / <Money value={market.book?.best_yes_ask} />
                </td>
                <td className="px-2 py-1.5">
                  <Money value={market.book?.best_no_bid} /> / <Money value={market.book?.best_no_ask} />
                </td>
                <td className="px-2 py-1.5"><Money value={market.book?.spread} /></td>
                <td className="px-2 py-1.5"><Money value={market.book?.yes_depth} /></td>
                <td className="px-2 py-1.5">{market.data_source}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.markets.length === 0 ? (
        <p className="text-sm text-[#9aa0a6]">No markets are stored. Run make seed.</p>
      ) : null}
    </div>
  );
}
