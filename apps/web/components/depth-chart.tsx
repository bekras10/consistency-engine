"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { fixedToScaled, scaledToFixed } from "@/lib/format";
import type { Level } from "@/lib/types";

type Point = { price: number; bid: number | null; ask: number | null; priceLabel: string };

function cumulative(levels: Level[], digits: number): Map<number, number> {
  const totals = new Map<number, number>();
  let running = 0;
  const ordered = [...levels].sort((left, right) => {
    const a = fixedToScaled(left.price, 4) ?? 0;
    const b = fixedToScaled(right.price, 4) ?? 0;
    return a - b;
  });
  for (const level of ordered) {
    const price = fixedToScaled(level.price, 4);
    const quantity = fixedToScaled(level.quantity, digits);
    if (price == null || quantity == null) continue;
    running += quantity;
    totals.set(price, running);
  }
  return totals;
}

export function DepthChart({
  bids,
  asks,
}: {
  bids: Level[];
  asks: Level[];
}) {
  const bidTotals = cumulative(bids, 2);
  const askTotals = cumulative(asks, 2);
  const prices = [...new Set([...bidTotals.keys(), ...askTotals.keys()])].sort((a, b) => a - b);
  const points: Point[] = prices.map((price) => ({
    price,
    priceLabel: scaledToFixed(price, 4),
    bid: bidTotals.get(price) ?? null,
    ask: askTotals.get(price) ?? null,
  }));
  if (points.length === 0) {
    return <p className="text-sm text-[#9aa0a6]">No persisted depth levels for this book.</p>;
  }
  return (
    <div data-testid="depth-chart" className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
          <CartesianGrid stroke="#2c3136" />
          <XAxis
            dataKey="price"
            type="number"
            domain={["dataMin", "dataMax"]}
            tickFormatter={(value: number) => scaledToFixed(value, 4)}
            stroke="#9aa0a6"
            label={{ value: "Price (dollars)", position: "insideBottom", offset: -2, fill: "#9aa0a6" }}
          />
          <YAxis
            stroke="#9aa0a6"
            tickFormatter={(value: number) => scaledToFixed(value, 2)}
            label={{ value: "Cumulative quantity", angle: -90, position: "insideLeft", fill: "#9aa0a6" }}
          />
          <Tooltip
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const point = payload[0]?.payload as Point | undefined;
              if (!point) return null;
              return (
                <div className="rounded border border-[#3a4046] bg-[#101214] px-2 py-1 text-xs">
                  <p className="font-mono">{point.priceLabel}</p>
                  <p>YES bid qty {point.bid == null ? "—" : scaledToFixed(point.bid, 2)}</p>
                  <p>YES ask qty {point.ask == null ? "—" : scaledToFixed(point.ask, 2)}</p>
                </div>
              );
            }}
          />
          <Legend />
          <Line type="stepAfter" dataKey="bid" name="YES bids" stroke="#8fd9b0" dot={false} connectNulls />
          <Line type="stepAfter" dataKey="ask" name="YES asks" stroke="#f0b4b4" dot={false} connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
