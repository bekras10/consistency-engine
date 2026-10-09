import { cn } from "@/lib/utils";

export function Badge({
  className,
  tone = "neutral",
  children,
}: {
  className?: string;
  tone?: "neutral" | "positive" | "negative" | "synthetic";
  children: React.ReactNode;
}) {
  const tones = {
    neutral: "border-[#3a4046] text-[#c5cad0]",
    positive: "border-[#1f6b45] text-[#8fd9b0]",
    negative: "border-[#7a3030] text-[#f0b4b4]",
    synthetic: "border-[#8a6a22] bg-[#2a2416] text-[#f0d48a]",
  };
  return (
    <span
      className={cn(
        "inline-flex items-center rounded border px-1.5 py-0.5 text-[11px] uppercase tracking-wide",
        tones[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}
