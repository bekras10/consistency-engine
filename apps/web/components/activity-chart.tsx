"use client";

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export function ActivityChart({ points }: { points: { start_ms: number; count: number }[] }) {
  if (points.length === 0) {
    return <p className="text-sm text-[#9aa0a6]">No detection timestamps are stored.</p>;
  }
  return (
    <div className="h-48 w-full" data-testid="activity-chart">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={points}>
          <CartesianGrid stroke="#2c3136" />
          <XAxis
            dataKey="start_ms"
            stroke="#9aa0a6"
            tickFormatter={(value: number) => String(value)}
            label={{ value: "Pipeline clock (ms)", position: "insideBottom", offset: -2, fill: "#9aa0a6" }}
          />
          <YAxis stroke="#9aa0a6" allowDecimals={false} label={{ value: "Detections", angle: -90, position: "insideLeft", fill: "#9aa0a6" }} />
          <Tooltip />
          <Bar dataKey="count" name="Detections opened" fill="#8aa0b4" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
