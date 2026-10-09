import { MarkdownView } from "@/components/markdown-view";
import { readRepoDoc } from "@/lib/docs";

export const dynamic = "force-dynamic";

export default async function MethodologyPage() {
  const model = await readRepoDoc("docs/mathematical-model.md");
  const compliance = await readRepoDoc("docs/compliance.md");
  return (
    <div className="space-y-6">
      <h1 className="text-xl font-medium">Methodology</h1>
      <p className="max-w-3xl text-sm leading-6 text-[#c5cad0]">
        The text below is the engine&apos;s mathematical model and the compliance note, read from
        the repository. A fee-adjusted candidate is a worst-case theoretical result after depth and
        fees. Asynchronous quotes can line up on the screen without being simultaneously
        executable. Synthetic output is not a measurement of a live venue.
      </p>
      <MarkdownView source={model} />
      <h2 className="text-lg font-medium">Data-source limits</h2>
      <MarkdownView source={compliance} />
    </div>
  );
}
