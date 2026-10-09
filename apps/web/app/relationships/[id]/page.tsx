import Link from "next/link";

import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";

export const dynamic = "force-dynamic";

type Detail = {
  relationship_id: string;
  relationship_type: string;
  verification_status: string;
  exhaustive: boolean;
  categories: string[];
  updated_at: string;
  created_at: string;
  members: {
    market_id: string;
    title?: string | null;
    ticker?: string;
    status?: string;
    rules_hash?: string;
    settlement_rules?: string | null;
  }[];
  document: unknown;
  proof_detection_id: string | null;
};

function asRecord(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === "object" && !Array.isArray(value)) return value as Record<string, unknown>;
  return null;
}

export default async function RelationshipDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const result = await readGateway<Detail>(`/internal/relationships/${encodeURIComponent(id)}`);
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  const data = result.data;
  const document = asRecord(data.document);
  const constraints = Array.isArray(document?.constraints) ? document.constraints : [];
  const evidence = Array.isArray(document?.evidence) ? document.evidence : [];
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-medium">{data.relationship_id}</h1>
      <p className="text-sm text-[#c5cad0]">
        {data.relationship_type} · {data.verification_status} · exhaustive {String(data.exhaustive)} · updated {data.updated_at}
      </p>
      <p className="text-sm">Categories: {data.categories.join(", ") || "none stored"}</p>
      <section>
        <h2 className="text-sm font-medium">Constituents</h2>
        <ul className="mt-2 space-y-2 text-sm">
          {data.members.map((member) => (
            <li key={member.market_id}>
              <Link className="underline" href={`/markets/${encodeURIComponent(member.market_id)}`}>
                {member.market_id}
              </Link>
              <span className="text-[#9aa0a6]"> {member.title ?? ""}</span>
              {member.settlement_rules ? <p className="text-[#c5cad0]">{member.settlement_rules}</p> : null}
            </li>
          ))}
        </ul>
      </section>
      <section>
        <h2 className="text-sm font-medium">Formal constraints</h2>
        <ul className="mt-2 list-disc pl-5 text-sm">
          {constraints.map((item, index) => (
            <li key={index}>{String(item)}</li>
          ))}
        </ul>
      </section>
      <section>
        <h2 className="text-sm font-medium">Evidence</h2>
        <pre className="mt-2 overflow-auto rounded border border-[#2c3136] bg-[#101214] p-3 font-mono text-xs">
          {JSON.stringify(evidence, null, 2)}
        </pre>
      </section>
      <section>
        <h2 className="text-sm font-medium">Scenario provenance</h2>
        <pre className="mt-2 overflow-auto rounded border border-[#2c3136] bg-[#101214] p-3 font-mono text-xs">
          {JSON.stringify(document?.scenario_provenance ?? document?.scenario_spec ?? null, null, 2)}
        </pre>
      </section>
      <p>
        {data.proof_detection_id ? (
          <Link className="underline" href={`/detections/${encodeURIComponent(data.proof_detection_id)}`}>
            Latest stored proof
          </Link>
        ) : (
          "No detection is stored for this relationship, so there is no proof to open."
        )}
      </p>
    </div>
  );
}
