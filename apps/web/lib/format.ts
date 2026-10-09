/** Fixed-point helpers. Money stays a string. Chart positions use integer scales. */

const MONEY = /^-?\d+(\.\d+)?$/;

export function isFixed(value: string): boolean {
  return MONEY.test(value);
}

export function moneyText(value: string | null | undefined): string {
  if (value == null || value === "") return "—";
  if (!isFixed(value)) return "—";
  return value;
}

export function fixedToScaled(value: string, digits: number): number | null {
  if (!isFixed(value) || digits < 0 || digits > 8) return null;
  const negative = value.startsWith("-");
  const body = negative ? value.slice(1) : value;
  const [whole, frac = ""] = body.split(".");
  if (whole == null) return null;
  const padded = (frac + "0".repeat(digits)).slice(0, digits);
  const scale = 10 ** digits;
  const scaled = Number(whole) * scale + Number(padded === "" ? "0" : padded);
  if (!Number.isSafeInteger(scaled)) return null;
  return negative ? -scaled : scaled;
}

export function scaledToFixed(scaled: number, digits: number): string {
  const sign = scaled < 0 ? "-" : "";
  const abs = Math.abs(scaled);
  const scale = 10 ** digits;
  const whole = Math.floor(abs / scale);
  const frac = String(abs % scale).padStart(digits, "0");
  return digits === 0 ? `${sign}${whole}` : `${sign}${whole}.${frac}`;
}

export function compareFixed(left: string | null, right: string | null): number {
  const a = left == null ? null : fixedToScaled(left, 8);
  const b = right == null ? null : fixedToScaled(right, 8);
  if (a == null && b == null) return 0;
  if (a == null) return -1;
  if (b == null) return 1;
  return a - b;
}

export function addFixed(left: string, right: string, digits: number): string | null {
  const a = fixedToScaled(left, digits);
  const b = fixedToScaled(right, digits);
  if (a == null || b == null) return null;
  return scaledToFixed(a + b, digits);
}
