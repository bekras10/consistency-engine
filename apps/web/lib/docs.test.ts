import { expect, it } from "vitest";

import { readRepoDoc } from "./docs";

it("reads the mathematical model from the repository root when the web app is the cwd", async () => {
  const model = await readRepoDoc("docs/mathematical-model.md");
  expect(model.startsWith("# Mathematical model")).toBe(true);
});
