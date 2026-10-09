"use client";

import { DetectionTable } from "@/components/detection-table";
import { PollStatus, usePolled } from "@/components/polling";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { queryString } from "@/lib/search";
import type { DetectionList } from "@/lib/types";

export function DetectionsView({
  initial,
  filters,
}: {
  initial: DetectionList;
  filters: Record<string, string>;
}) {
  const { data, error } = usePolled<DetectionList>(`/detections${queryString(filters)}`, initial);
  return (
    <div className="space-y-4">
      <PollStatus error={error} />
      <h1 className="text-xl font-medium">Detections</h1>
      <p className="text-xs text-[#9aa0a6]">
        Net edge is the current quote. The maximum is shown on its own line and is not used as the current edge.
      </p>
      <form className="grid gap-2 md:grid-cols-4" action="/detections">
        <select name="classification" defaultValue={filters.classification} className="h-8 rounded-md border border-[#3a4046] bg-[#101214] px-2 text-sm" aria-label="Classification">
          <option value="">Classification</option>
          {data.classifications.map((item) => (
            <option key={item} value={item}>{item}</option>
          ))}
        </select>
        <select name="relationship_type" defaultValue={filters.relationship_type} className="h-8 rounded-md border border-[#3a4046] bg-[#101214] px-2 text-sm" aria-label="Relationship type">
          <option value="">Relationship type</option>
          {data.relationship_types.map((item) => (
            <option key={item} value={item}>{item}</option>
          ))}
        </select>
        <Input name="since_ms" defaultValue={filters.since_ms} placeholder="From ms" aria-label="From time" />
        <Input name="until_ms" defaultValue={filters.until_ms} placeholder="To ms" aria-label="To time" />
        <Input name="min_net_edge" defaultValue={filters.min_net_edge} placeholder="Min current net edge" aria-label="Minimum net edge" />
        <Input name="min_quantity" defaultValue={filters.min_quantity} placeholder="Min quantity" aria-label="Minimum quantity" />
        <Input name="market" defaultValue={filters.market} placeholder="Market id" aria-label="Market" />
        <input type="hidden" name="sort" value={filters.sort} />
        <input type="hidden" name="direction" value={filters.direction} />
        <Button type="submit">Filter</Button>
      </form>
      <p className="text-xs text-[#9aa0a6]">
        {data.total} matching{data.truncated ? " (showing 500)" : ""}
      </p>
      <DetectionTable
        rows={data.detections}
        sortHref={(key) => {
          const next = {
            ...filters,
            sort: key,
            direction: filters.sort === key && filters.direction === "desc" ? "asc" : "desc",
          };
          return `/detections${queryString(next)}`;
        }}
      />
    </div>
  );
}
