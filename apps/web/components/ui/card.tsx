import { cn } from "@/lib/utils";

export function Card({ className, children }: { className?: string; children: React.ReactNode }) {
  return (
    <section className={cn("rounded-md border border-[#2c3136] bg-[#1b1e22]", className)}>
      {children}
    </section>
  );
}
