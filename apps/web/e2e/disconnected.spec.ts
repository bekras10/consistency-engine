import { expect, test } from "@playwright/test";

test("a dead gateway does not invent rows", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("unavailable")).toBeVisible();
  await expect(page.getByTestId("unavailable")).toContainText("No markets");
  await expect(page.getByTestId("detection-link")).toHaveCount(0);
  await expect(page.getByTestId("synthetic-banner")).toHaveCount(0);
});
