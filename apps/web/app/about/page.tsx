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
        This page reads persisted rows. It polls and does not subscribe to GET /api/v1/stream.
      </p>
      <MarkdownView source={readme} />
    </div>
  );
}
