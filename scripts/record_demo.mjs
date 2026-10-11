/**
 * Record the synthetic Compose dashboard. Run after `make demo`.
 * Uses the installed Playwright browser. Does not draw a UI.
 */
import { createRequire } from "node:module";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(path.join(root, "apps/web/package.json"));
const { chromium } = require("playwright");

const base = process.env.DEMO_URL ?? "http://127.0.0.1:3000";
const detection = process.env.DEMO_DETECTION;
const relationship = process.env.DEMO_RELATIONSHIP;
const outDir = path.join(root, "docs/demo");

if (!detection || !relationship) {
  console.error("Set DEMO_DETECTION and DEMO_RELATIONSHIP from make demo.");
  process.exit(1);
}

await mkdir(outDir, { recursive: true });
const videoDir = path.join(outDir, ".video");
await mkdir(videoDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1280, height: 800 },
  recordVideo: { dir: videoDir, size: { width: 1280, height: 800 } },
});
const page = await context.newPage();

await page.goto(`${base}/`);
await page.getByTestId("synthetic-banner").waitFor();
await page.waitForTimeout(3500);
await page.screenshot({ path: path.join(outDir, "overview.png") });
await page.getByText("Recent detections").scrollIntoViewIfNeeded();
await page.waitForTimeout(2500);

await page.goto(`${base}/relationships/${encodeURIComponent(relationship)}`);
await page.getByText("Formal constraints").waitFor();
await page.waitForTimeout(3500);
await page.screenshot({ path: path.join(outDir, "relationship.png") });

await page.goto(`${base}/detections/${encodeURIComponent(detection)}`);
await page.getByText("Current net edge").waitFor();
await page.waitForTimeout(3000);
await page.screenshot({ path: path.join(outDir, "detection-fees.png") });
await page.getByTestId("proof").scrollIntoViewIfNeeded();
await page.waitForTimeout(3000);
await page.screenshot({ path: path.join(outDir, "detection.png") });

await page.getByTestId("replay-link").click();
await page.getByTestId("replay-status").waitFor();
await page.getByTestId("replay-start").click();
await page.waitForFunction(
  () => {
    const node = document.querySelector("[data-testid='replay-status']");
    return node !== null && /playing|paused|finished/.test(node.textContent ?? "");
  },
  null,
  { timeout: 60_000 },
);
const afterStart = await page.getByTestId("replay-status").innerText();
await page.getByRole("button", { name: "Step" }).click();
await page.waitForFunction(
  (previous) => {
    const node = document.querySelector("[data-testid='replay-status']");
    return node !== null && (node.textContent ?? "") !== previous;
  },
  afterStart,
  { timeout: 30_000 },
);
await page.waitForTimeout(2500);
await page.screenshot({ path: path.join(outDir, "replay.png") });

const video = page.video();
await context.close();
await browser.close();
if (video) {
  const saved = await video.path();
  console.log(saved);
}
