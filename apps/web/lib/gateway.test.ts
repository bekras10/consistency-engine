import { afterEach, expect, it } from "vitest";

import { gatewayBase } from "./gateway";

const previous = process.env.DASHBOARD_GATEWAY_URL;

afterEach(() => {
  if (previous === undefined) delete process.env.DASHBOARD_GATEWAY_URL;
  else process.env.DASHBOARD_GATEWAY_URL = previous;
});

it("reads the gateway URL from the process environment", () => {
  process.env.DASHBOARD_GATEWAY_URL = "http://gateway:8765";
  expect(gatewayBase()).toBe("http://gateway:8765");
  delete process.env.DASHBOARD_GATEWAY_URL;
  expect(gatewayBase()).toBe("http://127.0.0.1:8765");
});
