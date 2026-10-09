import { describe, expect, it } from "vitest";

import { buildDepthSeries } from "./depth";

/** Locked to fixtures/golden/depth-multilevel.yaml. */
const bids = [
  { price: "0.40", quantity: "5.00" },
  { price: "0.60", quantity: "10.00" },
  { price: "0.55", quantity: "20.50" },
  { price: "0.3333", quantity: "1.25" },
];

const asks = [
  { price: "0.80", quantity: "4.00" },
  { price: "0.62", quantity: "8.00" },
  { price: "0.70", quantity: "12.25" },
];

describe("multi-level depth chart", () => {
  it("accumulates bids from the highest price and asks from the lowest", () => {
    const points = buildDepthSeries(bids, asks);
    const at = (price: string) => points.find((point) => point.price === price);
    expect(points.map((point) => point.price).sort()).toEqual(
      ["0.3333", "0.40", "0.55", "0.60", "0.62", "0.70", "0.80"].sort(),
    );
    expect(at("0.60")?.bid).toBe("10.00");
    expect(at("0.55")?.bid).toBe("30.50");
    expect(at("0.40")?.bid).toBe("35.50");
    expect(at("0.3333")?.bid).toBe("36.75");
    expect(at("0.62")?.ask).toBe("8.00");
    expect(at("0.70")?.ask).toBe("20.25");
    expect(at("0.80")?.ask).toBe("24.25");
    expect(at("0.40")?.price).toBe("0.40");
    expect(at("0.70")?.price).toBe("0.70");
  });
});
