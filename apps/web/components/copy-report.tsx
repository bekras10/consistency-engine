"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";

export function CopyReport({ text }: { text: string }) {
  const [status, setStatus] = useState<"idle" | "copied" | "failed">("idle");
  return (
    <div className="space-y-2">
      <Button
        type="button"
        data-testid="copy-report"
        onClick={async () => {
          try {
            await navigator.clipboard.writeText(text);
            setStatus("copied");
          } catch {
            setStatus("failed");
          }
        }}
      >
        Copy technical report
      </Button>
      {status === "copied" ? <p>Copied</p> : null}
      {status === "failed" ? <p>Copy failed. The report text is still in the box below.</p> : null}
      <pre
        data-testid="technical-report"
        className="max-h-80 overflow-auto whitespace-pre-wrap rounded border border-[#2c3136] bg-[#101214] p-3 font-mono text-xs"
      >
        {text}
      </pre>
    </div>
  );
}
