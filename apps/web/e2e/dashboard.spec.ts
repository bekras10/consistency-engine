import { expect, test } from "@playwright/test";

test("dashboard journeys use seeded synthetic rows", async ({ page }) => {
  test.setTimeout(120_000);
  await page.goto("/");
  await expect(page.getByTestId("synthetic-banner")).toBeVisible();
  await expect(page.getByTestId("unavailable")).toHaveCount(0);
  await expect(page.getByTestId("empty-state")).toHaveCount(0);
  await expect(page.getByTestId("detection-link").first()).toBeVisible();
  await expect(page.getByTestId("poll-label")).toContainText("polling");

  await page.goto("/relationships");
  const type = page.getByLabel("Type");
  const option = type.locator("option").nth(1);
  const typeValue = await option.getAttribute("value");
  expect(typeValue).toBeTruthy();
  await type.selectOption(typeValue ?? "");
  await page.getByRole("button", { name: "Filter" }).click();
  await expect(page).toHaveURL(new RegExp(`type=${typeValue}`));
  await expect(page.getByTestId("relationship-row").first()).toContainText(typeValue ?? "");

  await page.goto("/detections");
  await page.getByTestId("detection-link").first().click();
  await expect(page.getByTestId("proof")).toBeVisible();
  await expect(page.getByTestId("technical-report")).toContainText("certificate_json");
  await page.getByTestId("copy-report").click();
  await expect(page.getByText("Copied")).toBeVisible();

  await page.getByTestId("replay-link").click();
  await expect(page.getByTestId("replay-status")).toContainText("ready");
  await page.getByTestId("replay-start").click();
  await expect(page.getByTestId("replay-status")).toContainText(/playing|paused|finished/);
  await page.getByTestId("replay-pause").click();
  await expect(page.getByTestId("replay-status")).toContainText(/paused|finished/);
  const range = await page.getByText(/Range /).textContent();
  const match = range?.match(/Range (\d+) to (\d+)/);
  expect(match).toBeTruthy();
  const target = match?.[2] ?? "0";
  await page.getByTestId("replay-seek").fill(target);
  const sought = page.waitForResponse(
    (response) => response.url().includes("/seek") && response.request().method() === "POST",
    { timeout: 90_000 },
  );
  await page.getByRole("button", { name: "Seek" }).click();
  await sought;
  await expect(page.getByTestId("replay-status")).toContainText("paused", { timeout: 60_000 });
  await expect(page.getByTestId("replay-status")).toContainText(`position ${target}`, {
    timeout: 60_000,
  });
});
