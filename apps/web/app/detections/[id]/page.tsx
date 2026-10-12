import Link from "next/link";

import { CopyReport } from "@/components/copy-report";
import { DepthChart } from "@/components/depth-chart";
import { Money } from "@/components/money";
import { Unavailable } from "@/components/states";
import { readGateway } from "@/lib/gateway";
import type { BookView } from "@/lib/types";

export const dynamic = "force-dynamic";

type Detail = {
  detection_id: string;
  session_id: string;
  relationship_id: string;
  relationship_type: string | null;
  classification: string;
  status: string;
  explanation: string;
  net_edge: string | null;
  max_net_edge: string | null;
  theoretical_deviation: string | null;
  max_deviation: string | null;
  gross_edge: string | null;
  fees: string | null;
  depth_supported_quantity: string | null;
  worst_case_payoff?: string | null;
  certificate_hash: string;
  proof_status?: string;
  executable?: boolean;
  quote_label?: string;
  closing_classification?: string | null;
  closing_certificate_hash?: string | null;
  closing_certificate?: unknown;
  first_observed_ms: number;
  last_observed_ms: number;
  first_position: number;
  last_position: number;
  duration_ms: number;
  close_reason: string | null;
  close_detail: string | null;
  reason_codes: string[];
  technical_report: string;
  legs: {
    leg_index: number;
    market_id: string;
    side: string;
    ratio: string | null;
    quantity: string | null;
    premium: string | null;
    leg_cost: string | null;
  }[];
  scenarios: {
    scenario_index: number;
    state_json: string;
    payoff_per_unit: string | null;
    is_worst: boolean;
  }[];
  markets: {
    market_id: string;
    title: string;
    ticker: string;
    status: string;
    rules_hash: string;
    settlement_rules: string | null;
    settlement_source: string | null;
  }[];
  books: BookView[];
  certificate: unknown;
  source_kind?: string | null;
  replay_available?: boolean;
  raw_persisted?: boolean;
};

function section(title: string, value: unknown) {
  if (value == null) return null;
  return (
    <section className="space-y-2">
      <h2 className="text-sm font-medium">{title}</h2>
      <pre className="max-h-72 overflow-auto rounded border border-[#2c3136] bg-[#101214] p-3 font-mono text-xs">
        {JSON.stringify(value, null, 2)}
      </pre>
    </section>
  );
}

export default async function DetectionDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const result = await readGateway<Detail>(`/internal/detections/${encodeURIComponent(id)}`);
  if (!result.ok) return <Unavailable error={result.error} detail={result.detail} />;
  const data = result.data;
  const certificate = data.certificate && typeof data.certificate === "object" ? (data.certificate as Record<string, unknown>) : null;
  const closingCertificate = data.closing_certificate && typeof data.closing_certificate === "object" ? (data.closing_certificate as Record<string, unknown>) : null;
  const closed = data.executable === false;
  const quoteLabel = data.quote_label ?? (closed ? "Last qualifying quote" : "Open quote");
  const certified = data.proof_status === "certified";
  const sourceKind = data.source_kind ?? "synthetic";
  const replayAvailable = data.replay_available !== false && sourceKind !== "kalshi_authorized";
  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-medium">Detection</h1>
          <p className="font-mono text-xs text-[#9aa0a6]">{data.detection_id}</p>
        </div>
        {replayAvailable ? (
          <Link
            data-testid="replay-link"
            className="rounded border border-[#3a4046] px-3 py-1 text-sm"
            href={`/replay/${encodeURIComponent(data.session_id)}`}
          >
            Replay
          </Link>
        ) : (
          <p className="text-sm text-[#9aa0a6]">Replay unavailable</p>
        )}
      </div>
      {sourceKind === "kalshi_authorized" ? (
        <p data-testid="kalshi-detection" className="rounded-md border border-[#2f5d45] bg-[#15231c] px-3 py-2 text-sm text-[#c9ead8]">
          Authorized Kalshi live feed. This detection is a stored observation, not an executed trade.
          {closed ? " The quote is the last qualifying observation." : ""}
        </p>
      ) : (
        <p data-testid="synthetic-detection" className="rounded-md border border-[#8a6a22] bg-[#2a2416] px-3 py-2 text-sm text-[#f0d48a]">
          Synthetic simulation. This detection is from the archived recording, not a live Kalshi feed.
        </p>
      )}
      <p data-testid="explanation" className="max-w-3xl text-sm leading-6">{data.explanation}</p>
      {closed ? (
        <p data-testid="quote-not-executable" className="max-w-3xl text-sm">
          {quoteLabel}. The figures below are the retained qualifying quote. They are not a currently executable opportunity.
        </p>
      ) : (
        <p data-testid="quote-open" className="text-sm">{quoteLabel}. This detection is still open.</p>
      )}
      <p className="text-sm">
        Relationship{" "}
        <Link className="underline" href={`/relationships/${encodeURIComponent(data.relationship_id)}`}>
          {data.relationship_id}
        </Link>{" "}
        ({data.relationship_type ?? "type not stored"}) · {data.status} · qualifying {data.classification}
      </p>
      <h2 className="text-sm font-medium">{quoteLabel}</h2>
      <dl className="grid grid-cols-2 gap-2 text-sm md:grid-cols-4">
        <div><dt className="text-[#9aa0a6]">{closed ? "Qualifying net edge" : "Net edge"}</dt><dd><Money value={data.net_edge} /></dd></div>
        <div><dt className="text-[#9aa0a6]">Maximum net edge</dt><dd><Money value={data.max_net_edge} /></dd></div>
        <div><dt className="text-[#9aa0a6]">Gross edge</dt><dd><Money value={data.gross_edge} /></dd></div>
        <div><dt className="text-[#9aa0a6]">Fees</dt><dd><Money value={data.fees} /></dd></div>
        <div><dt className="text-[#9aa0a6]">Deviation</dt><dd><Money value={data.theoretical_deviation} /></dd></div>
        <div><dt className="text-[#9aa0a6]">Max deviation</dt><dd><Money value={data.max_deviation} /></dd></div>
        <div><dt className="text-[#9aa0a6]">Depth quantity</dt><dd><Money value={data.depth_supported_quantity} /></dd></div>
        <div><dt className="text-[#9aa0a6]">Duration</dt><dd className="font-mono">{data.duration_ms} ms</dd></div>
      </dl>
      <section>
        <h2 className="text-sm font-medium">Settlement rules</h2>
        <ul className="mt-2 space-y-2 text-sm">
          {data.markets.map((market) => (
            <li key={market.market_id}>
              <Link className="underline" href={`/markets/${encodeURIComponent(market.market_id)}`}>{market.ticker}</Link>
              <span className="text-[#9aa0a6]"> {market.title}</span>
              <p>{market.settlement_rules ?? "No settlement text stored."}</p>
              <p className="text-xs text-[#9aa0a6]">source {market.settlement_source ?? "—"} · rules {market.rules_hash}</p>
            </li>
          ))}
        </ul>
      </section>
      <section>
        <h2 className="text-sm font-medium">Bid and ask</h2>
        <p className="text-xs text-[#9aa0a6]">
          These levels are the latest persisted book for the constituent markets. They are not a
          reconstruction of the quote at first observation. Replay shows the book at a cursor.
        </p>
        {data.books.length === 0 ? (
          <p className="text-sm text-[#9aa0a6]">No journal book is available for these markets.</p>
        ) : (
          data.books.map((book) => (
            <div key={book.market_id} className="mt-3 space-y-2">
              <p className="text-sm">
                {book.market_id} · {book.sync_status} · YES {book.best_yes_bid ?? "—"} / {book.best_yes_ask ?? "—"}
              </p>
              <p className="text-xs text-[#9aa0a6]">{book.depth_note}</p>
              <DepthChart bids={book.yes_bids} asks={book.yes_asks} />
            </div>
          ))
        )}
      </section>
      <section>
        <h2 className="text-sm font-medium">Legs</h2>
        <table className="mt-2 w-full text-left text-sm">
          <thead className="text-xs uppercase text-[#9aa0a6]">
            <tr>
              {["#", "Market", "Side", "Ratio", "Quantity", "Premium", "Leg cost"].map((label) => (
                <th key={label} className="border-b border-[#2c3136] px-2 py-1">{label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.legs.map((leg) => (
              <tr key={leg.leg_index}>
                <td className="px-2 py-1 font-mono">{leg.leg_index}</td>
                <td className="px-2 py-1">{leg.market_id}</td>
                <td className="px-2 py-1">{leg.side}</td>
                <td className="px-2 py-1"><Money value={leg.ratio} /></td>
                <td className="px-2 py-1"><Money value={leg.quantity} /></td>
                <td className="px-2 py-1"><Money value={leg.premium} /></td>
                <td className="px-2 py-1"><Money value={leg.leg_cost} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section data-testid="proof">
        <h2 className="text-sm font-medium">Scenario payoffs</h2>
        <p className="text-xs text-[#9aa0a6]">Rows from detection_scenarios. Payoff is per basket unit.</p>
        <table className="mt-2 w-full text-left text-sm">
          <thead className="text-xs uppercase text-[#9aa0a6]">
            <tr>
              {["State", "Payoff per unit", "Worst"].map((label) => (
                <th key={label} className="border-b border-[#2c3136] px-2 py-1">{label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.scenarios.map((scenario) => (
              <tr key={scenario.scenario_index}>
                <td className="px-2 py-1 font-mono text-xs">{scenario.state_json}</td>
                <td className="px-2 py-1"><Money value={scenario.payoff_per_unit} /></td>
                <td className="px-2 py-1">{scenario.is_worst ? "worst case" : ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {certified ? (
          <p data-testid="certificate-hash" className="mt-2 font-mono text-xs">certificate {data.certificate_hash}</p>
        ) : (
          <p data-testid="certificate-hash" className="mt-2 font-mono text-xs">
            stored hash {data.certificate_hash} (not a verified proof)
          </p>
        )}
        {certified ? (
          <p className="text-xs text-[#9aa0a6]">
            Qualifying certificate. Its hash is SHA-256 of this proof, not of the closing evaluation.
          </p>
        ) : (
          <p data-testid="proof-uncertified" className="mt-2 text-sm">
            Not a proof. This row is uncertified. The stored bytes are kept for
            investigation and are not a verified certificate.
          </p>
        )}
      </section>
      {certified ? section("Qualifying walked levels and evaluation", certificate?.evaluation) : null}
      {certified ? section("Fees in the qualifying certificate", certificate?.fees ?? certificate?.fee_schedules) : null}
      {certified ? section("Timing and sequence", certificate?.timing) : null}
      <section data-testid="closing-condition">
        <h2 className="text-sm font-medium">Closing condition</h2>
        <p className="text-sm">
          {closed
            ? `Closed as ${data.close_reason ?? data.status}. Closing classification ${data.closing_classification ?? "not stored"}.`
            : "This detection is still open. There is no later closing evaluation."}
          {" "}
          {data.close_detail ?? ""}
        </p>
        {data.closing_certificate_hash ? (
          <p className="mt-2 font-mono text-xs">closing certificate {data.closing_certificate_hash}</p>
        ) : null}
        {closingCertificate ? (
          <p className="mt-2 text-xs text-[#9aa0a6]">
            Closing classification {String(closingCertificate.classification ?? data.closing_classification)}. This object is not the qualifying proof.
          </p>
        ) : null}
      </section>
      <section>
        <h2 className="text-sm font-medium">Lifecycle</h2>
        <p className="text-sm">
          Observed {data.first_observed_ms}–{data.last_observed_ms} ms. Positions {data.first_position}–{data.last_position}.
          Close {data.close_reason ?? "still recorded as " + data.status}. {data.close_detail ?? ""}
        </p>
        <p className="text-xs text-[#9aa0a6]">Reason codes: {data.reason_codes.join(", ") || "none"}</p>
        <p className="mt-2 text-sm">
          Execution caveat: books are quoted independently. A positive worst-case theoretical edge is not a fill and not a profit.
        </p>
      </section>
      <CopyReport text={data.technical_report} />
    </div>
  );
}
