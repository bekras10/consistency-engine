export function MarkdownView({ source }: { source: string }) {
  const blocks = source.replace(/\r\n/g, "\n").split(/\n{2,}/);
  return (
    <article className="max-w-3xl space-y-4 text-sm leading-6 text-[#d5d8dc]">
      {blocks.map((block, index) => {
        const trimmed = block.trim();
        if (trimmed.startsWith("```")) {
          const body = trimmed.replace(/^```[^\n]*\n?/, "").replace(/```$/, "");
          return (
            <pre key={index} className="overflow-auto rounded border border-[#2c3136] bg-[#101214] p-3 font-mono text-xs">
              {body}
            </pre>
          );
        }
        if (trimmed.startsWith("#")) {
          const level = trimmed.match(/^#+/)?.[0].length ?? 1;
          const text = trimmed.replace(/^#+\s*/, "");
          if (level <= 2) return <h2 key={index} className="text-lg font-medium text-[#e8eaed]">{text}</h2>;
          return <h3 key={index} className="text-base font-medium text-[#e8eaed]">{text}</h3>;
        }
        return (
          <p key={index} className="whitespace-pre-wrap">
            {trimmed}
          </p>
        );
      })}
    </article>
  );
}
