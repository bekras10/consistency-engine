/** Chart geometry uses an integer scale. Displayed prices stay the original strings. */

import { addFixed, compareFixed, fixedToScaled } from "./format";

export type DepthLevel = { price: string; quantity: string };

export type DepthPoint = {
  price: string;
  priceScale: number;
  bid: string | null;
  ask: string | null;
  bidScale: number | null;
  askScale: number | null;
};

function accumulate(levels: DepthLevel[], direction: "asc" | "desc"): Map<string, string> {
  const ordered = [...levels].sort((left, right) => {
    const compared = compareFixed(left.price, right.price);
    return direction === "asc" ? compared : -compared;
  });
  const totals = new Map<string, string>();
  let running = "0.00";
  for (const level of ordered) {
    const next = addFixed(running, level.quantity, 2);
    if (next == null) continue;
    running = next;
    totals.set(level.price, running);
  }
  return totals;
}

export function buildDepthSeries(bids: DepthLevel[], asks: DepthLevel[]): DepthPoint[] {
  const bidTotals = accumulate(bids, "desc");
  const askTotals = accumulate(asks, "asc");
  const prices = new Map<string, number>();
  for (const level of [...bids, ...asks]) {
    const scale = fixedToScaled(level.price, 4);
    if (scale == null || prices.has(level.price)) continue;
    prices.set(level.price, scale);
  }
  return [...prices.entries()]
    .sort((left, right) => left[1] - right[1])
    .map(([price, priceScale]) => {
      const bid = bidTotals.get(price) ?? null;
      const ask = askTotals.get(price) ?? null;
      return {
        price,
        priceScale,
        bid,
        ask,
        bidScale: bid == null ? null : fixedToScaled(bid, 2),
        askScale: ask == null ? null : fixedToScaled(ask, 2),
      };
    });
}
