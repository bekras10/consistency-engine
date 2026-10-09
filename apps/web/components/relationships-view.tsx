"use client";

import Link from "next/link";

import { PollStatus, usePolled } from "@/components/polling";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { queryString } from "@/lib/search";
import type { RelationshipList } from "@/lib/types";

export function RelationshipsView({
  initial,
  filters,
}: {
  initial: RelationshipList;
  filters: Record<string, string>;
}) {
  const { data, error, status } = usePolled<RelationshipList>(
    `/relationships${queryString(filters)}`,
    initial,
  );
  return (
    <div className="space-y-4">
      <PollStatus error={error} status={status} />
      <h1 className="text-xl font-medium">Relationships</h1>
      <form data-testid="relationship-filter" className="grid gap-2 md:grid-cols-6" action="/relationships">
        <select name="type" defaultValue={filters.type} className="h-8 rounded-md border border-[#3a4046] bg-[#101214] px-2 text-sm" aria-label="Type">
          <option value="">Type</option>
          {data.types.map((type) => (
            <option key={type} value={type}>{type}</option>
          ))}
        </select>
        <select name="status" defaultValue={filters.status} className="h-8 rounded-md border border-[#3a4046] bg-[#101214] px-2 text-sm" aria-label="Status">
          <option value="">Status</option>
          {data.statuses.map((status) => (
            <option key={status} value={status}>{status}</option>
          ))}
        </select>
        <select name="category" defaultValue={filters.category} className="h-8 rounded-md border border-[#3a4046] bg-[#101214] px-2 text-sm" aria-label="Category">
          <option value="">Category</option>
          {data.categories.map((category) => (
            <option key={category} value={category}>{category}</option>
          ))}
        </select>
        <Input name="market" defaultValue={filters.market} placeholder="Market id" aria-label="Market" />
        <Input name="min_members" defaultValue={filters.min_members} placeholder="Min constituents" aria-label="Minimum constituents" />
        <div className="flex gap-2">
          <Input name="max_members" defaultValue={filters.max_members} placeholder="Max" aria-label="Maximum constituents" />
          <Button type="submit">Filter</Button>
        </div>
      </form>
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead className="text-xs uppercase tracking-wide text-[#9aa0a6]">
            <tr>
              {["Type", "Constituents", "Status", "Category", "Evaluation", "Updated", "Proof"].map((label) => (
                <th key={label} className="border-b border-[#2c3136] px-2 py-2 font-medium">{label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.relationships.map((row) => (
              <tr key={row.relationship_id} data-testid="relationship-row" className="border-b border-[#23282d]">
                <td className="px-2 py-1.5">
                  <Link className="underline" href={`/relationships/${encodeURIComponent(row.relationship_id)}`}>
                    {row.relationship_type}
                  </Link>
                </td>
                <td className="px-2 py-1.5 font-mono">{row.member_count}</td>
                <td className="px-2 py-1.5">{row.verification_status}</td>
                <td className="px-2 py-1.5">{row.categories.join(", ") || "—"}</td>
                <td className="px-2 py-1.5">
                  {row.current_evaluation
                    ? `${row.current_evaluation.status} ${row.current_evaluation.classification}`
                    : "No open detection"}
                </td>
                <td className="px-2 py-1.5 font-mono text-xs">{row.updated_at}</td>
                <td className="px-2 py-1.5">
                  {row.proof_detection_id ? (
                    <Link href={`/detections/${encodeURIComponent(row.proof_detection_id)}`}>Proof</Link>
                  ) : (
                    "—"
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.relationships.length === 0 ? (
        <p className="text-sm text-[#9aa0a6]">No relationships match these filters.</p>
      ) : null}
    </div>
  );
}
