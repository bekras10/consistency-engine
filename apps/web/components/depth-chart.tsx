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

import { buildDepthSeries } from "@/lib/depth";
import { scaledToFixed } from "@/lib/format";
import type { Level } from "@/lib/types";

export function DepthChart({
  bids,
  asks,
}: {
  bids: Level[];
  asks: Level[];
}) {
  const points = buildDepthSeries(bids, asks);
  const priceLabels = new Map(points.map((point) => [point.priceScale, point.price]));
  if (points.length === 0) {
    return <p className="text-sm text-[#9aa0a6]">No persisted depth levels for this book.</p>;
  }
  return (
    <div data-testid="depth-chart" className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
          <CartesianGrid stroke="#2c3136" />
          <XAxis
            dataKey="priceScale"
            type="number"
            domain={["dataMin", "dataMax"]}
            tickFormatter={(value: number) => priceLabels.get(value) ?? scaledToFixed(value, 4)}
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
              const point = payload[0]?.payload as (typeof points)[number] | undefined;
              if (!point) return null;
              return (
                <div className="rounded border border-[#3a4046] bg-[#101214] px-2 py-1 text-xs">
                  <p className="font-mono">{point.price}</p>
                  <p>YES bid qty {point.bid ?? "—"}</p>
                  <p>YES ask qty {point.ask ?? "—"}</p>
                </div>
              );
            }}
          />
          <Legend />
          <Line type="stepAfter" dataKey="bidScale" name="YES bids" stroke="#8fd9b0" dot={false} connectNulls />
          <Line type="stepAfter" dataKey="askScale" name="YES asks" stroke="#f0b4b4" dot={false} connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
