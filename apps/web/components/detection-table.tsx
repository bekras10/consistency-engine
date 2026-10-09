import Link from "next/link";

import { Money } from "@/components/money";
import type { DetectionRow } from "@/lib/types";

const columns: { key: string; label: string }[] = [
  { key: "time", label: "Time" },
  { key: "relationship", label: "Relationship" },
  { key: "type", label: "Type" },
  { key: "theoretical_deviation", label: "Deviation" },
  { key: "gross_edge", label: "Gross edge" },
  { key: "fees", label: "Fees" },
  { key: "net_edge", label: "Net edge" },
  { key: "quantity", label: "Depth qty" },
  { key: "duration", label: "Duration" },
  { key: "classification", label: "Classification" },
];

export function DetectionTable({
  rows,
  sortHref,
}: {
  rows: DetectionRow[];
  sortHref?: (key: string) => string;
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[960px] text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-[#9aa0a6]">
          <tr>
            {columns.map((column) => (
              <th key={column.key} className="border-b border-[#2c3136] px-2 py-2 font-medium">
                {sortHref ? (
                  <Link href={sortHref(column.key)} className="hover:text-[#e8eaed]">
                    {column.label}
                  </Link>
                ) : (
                  column.label
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.detection_id} className="border-b border-[#23282d]">
              <td className="px-2 py-1.5 font-mono">{row.time_ms}</td>
              <td className="px-2 py-1.5">
                <Link
                  data-testid="detection-link"
                  href={`/detections/${encodeURIComponent(row.detection_id)}`}
                  className="underline decoration-[#3a4046] underline-offset-2"
                >
                  {row.relationship_id}
                </Link>
              </td>
              <td className="px-2 py-1.5">{row.relationship_type ?? "—"}</td>
              <td className="px-2 py-1.5">
                <Money value={row.theoretical_deviation} />
              </td>
              <td className="px-2 py-1.5">
                <Money value={row.gross_edge} />
              </td>
              <td className="px-2 py-1.5">
                <Money value={row.fees} />
              </td>
              <td className="px-2 py-1.5">
                <div>
                  <Money value={row.net_edge} />
                </div>
                <div className="text-[11px] text-[#9aa0a6]">
                  max <Money value={row.max_net_edge} />
                </div>
              </td>
              <td className="px-2 py-1.5">
                <Money value={row.depth_supported_quantity} />
              </td>
              <td className="px-2 py-1.5 font-mono">{row.duration_ms} ms</td>
              <td className="px-2 py-1.5">{row.classification}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
