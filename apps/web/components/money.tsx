import { moneyText } from "@/lib/format";

export function Money({ value }: { value: string | null | undefined }) {
  return <span className="font-mono tabular-nums">{moneyText(value)}</span>;
}
