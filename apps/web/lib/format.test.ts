import { describe, expect, it } from "vitest";

import { addFixed, compareFixed, fixedToScaled, moneyText, scaledToFixed } from "./format";

describe("fixed-point display", () => {
  it("keeps the stored string and refuses exponent text", () => {
    expect(moneyText("0.10")).toBe("0.10");
    expect(moneyText("1E-7")).toBe("—");
    expect(moneyText(null)).toBe("—");
  });

  it("compares decimal strings without treating 0.10 as less than 0.9 by character order only", () => {
    expect(compareFixed("0.10", "0.9")).toBeLessThan(0);
    expect(compareFixed("0.10", "0.10")).toBe(0);
    expect(compareFixed("10", "9")).toBeGreaterThan(0);
  });

  it("adds quantities on an integer scale", () => {
    expect(addFixed("0.10", "0.20", 2)).toBe("0.30");
    expect(addFixed("1.5", "2.25", 2)).toBe("3.75");
  });

  it("round-trips a 4dp price through the chart scale", () => {
    const scaled = fixedToScaled("0.3333", 4);
    expect(scaled).toBe(3333);
    expect(scaledToFixed(3333, 4)).toBe("0.3333");
  });
});
