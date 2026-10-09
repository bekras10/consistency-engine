import { MarkdownView } from "@/components/markdown-view";
import { readRepoDoc } from "@/lib/docs";

export const dynamic = "force-dynamic";

export default async function AboutPage() {
  const readme = await readRepoDoc("README.md");
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-medium">About</h1>
      <p className="max-w-3xl text-sm leading-6">
        Consistency Engine is an independent open-source engineering project. It is not an official
        Kalshi product, it is not endorsed by Kalshi, and this dashboard does not place orders.
        The default source is a synthetic exchange. Architecture notes live in docs/architecture.md.
        The public REST catalog and the event stream are Phase 11 and are not part of this UI.
      </p>
      <MarkdownView source={readme} />
    </div>
  );
}
