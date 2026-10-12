/**
 * Record the synthetic Compose dashboard with Playwright's Chromium.
 * Run after `make demo`. Does not draw a UI and does not call an image model.
 *
 * Required:
 *   DEMO_DETECTION, DEMO_RELATIONSHIP, DEMO_REPLAY_MS, DEMO_REPLAY_CURSOR, DEMO_CERT_HASH
 * Optional:
 *   DEMO_URL (default http://127.0.0.1:3000)
 *   DEMO_REPLAY_CURSOR (printed into the log; the seek uses DEMO_REPLAY_MS)
 */
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { copyFile, mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

const execFileAsync = promisify(execFile);
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(path.join(root, "apps/web/package.json"));
const { chromium } = require("playwright");

const base = process.env.DEMO_URL ?? "http://127.0.0.1:3000";
const detection = process.env.DEMO_DETECTION;
const relationship = process.env.DEMO_RELATIONSHIP;
const replayMs = process.env.DEMO_REPLAY_MS;
const certHash = process.env.DEMO_CERT_HASH;
const replayCursor = process.env.DEMO_REPLAY_CURSOR ?? "";
const outDir = path.join(root, "docs/demo");
const videoDir = path.join(outDir, ".video");

if (!detection || !relationship || !replayMs || !certHash || !replayCursor) {
  console.error(
    "Set DEMO_DETECTION, DEMO_RELATIONSHIP, DEMO_REPLAY_MS, DEMO_REPLAY_CURSOR, and DEMO_CERT_HASH from make demo.",
  );
  process.exit(1);
}

function digestOf(text) {
  return `sha256:${createHash("sha256").update(text).digest("hex")}`;
}

await rm(videoDir, { recursive: true, force: true });
await mkdir(videoDir, { recursive: true });
await mkdir(outDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1280, height: 800 },
  recordVideo: { dir: videoDir, size: { width: 1280, height: 800 } },
});
const page = await context.newPage();
const video = page.video();

try {
  await page.goto(`${base}/`);
  await page.getByTestId("synthetic-banner").waitFor();
  await page.waitForTimeout(800);
  await page.screenshot({ path: path.join(outDir, "overview.png") });

  await page.goto(`${base}/relationships/${encodeURIComponent(relationship)}`);
  await page.getByText("Formal constraints").waitFor();
  await page.waitForTimeout(800);
  await page.screenshot({ path: path.join(outDir, "relationship.png") });

  await page.goto(`${base}/detections/${encodeURIComponent(detection)}`);
  await page.getByRole("heading", { name: "Last qualifying quote" }).waitFor();
  await page.getByTestId("quote-not-executable").waitFor();
  await page.getByTestId("synthetic-detection").waitFor();
  await page.getByRole("heading", { name: "Last qualifying quote" }).scrollIntoViewIfNeeded();
  await page.waitForTimeout(1200);
  await page.screenshot({ path: path.join(outDir, "detection-fees.png") });
  await page.getByTestId("proof").scrollIntoViewIfNeeded();
  await page.waitForTimeout(1400);
  await page.screenshot({ path: path.join(outDir, "detection.png") });

  const shownProof = await page.getByTestId("certificate-hash").innerText();
  if (!shownProof.includes(certHash)) {
    throw new Error(`qualifying certificate label ${shownProof} != ${certHash}`);
  }
  const closing = await page.getByTestId("closing-condition").innerText();
  if (!closing.includes("INSUFFICIENT_LIQUIDITY") || closing.includes(certHash)) {
    throw new Error("closing condition is not distinct from the qualifying proof");
  }
  const report = await page.getByTestId("technical-report").innerText();
  const hashLine = report.match(/^certificate_hash: (sha256:[0-9a-f]+)/m);
  const marker = "certificate_json:\n";
  const jsonAt = report.indexOf(marker);
  if (!hashLine || jsonAt < 0) {
    throw new Error("technical report is missing the qualifying certificate");
  }
  const certificateJson = report.slice(jsonAt + marker.length).trim();
  const recomputed = digestOf(certificateJson);
  if (hashLine[1] !== certHash || recomputed !== certHash) {
    throw new Error(`displayed certificate ${hashLine[1]} recomputed ${recomputed} stored ${certHash}`);
  }
  if (!report.includes("FEE_ADJUSTED_CANDIDATE")) {
    throw new Error("technical report is missing the qualifying classification");
  }

  await page.getByTestId("replay-link").click();
  await page.getByTestId("synthetic-replay").waitFor();
  await page.getByTestId("replay-status").waitFor();
  const replayLabel = await page.getByTestId("synthetic-replay").innerText();
  if (!replayLabel.includes("not a live Kalshi feed")) {
    throw new Error("replay page does not say this is not a live Kalshi feed");
  }
  const started = page.waitForResponse(
    (response) => response.url().includes("/start") && response.request().method() === "POST",
  );
  await page.getByTestId("replay-start").click();
  await started;
  await page.getByTestId("replay-pause").click();
  await page.getByTestId("replay-seek-cursor").fill(replayCursor);
  const sought = page.waitForResponse(
    (response) => response.url().includes("/seek") && response.request().method() === "POST",
    { timeout: 180_000 },
  );
  await page.getByRole("button", { name: "Seek cursor" }).click();
  await sought;
  await page.getByTestId("replay-detection").waitFor({ timeout: 30_000 });
  const shown = await page.getByTestId("replay-detection").innerText();
  if (!shown.includes(detection)) {
    throw new Error(`replay detection panel does not show ${detection}`);
  }
  if (!shown.includes("FEE_ADJUSTED_CANDIDATE")) {
    throw new Error("replay detection is not the qualifying classification");
  }
  const shownHash = await page.getByTestId("replay-certificate").innerText();
  if (!shownHash.includes(certHash)) {
    throw new Error(`replay certificate ${shownHash} != ${certHash}`);
  }
  const books = await page.getByTestId("replay-books").innerText();
  if (!books.includes("SYNCPI")) {
    throw new Error(`replay books are not the CPI legs: ${books.slice(0, 240)}`);
  }
  const constraint = await page.getByTestId("replay-constraint").innerText();
  if (!constraint.includes("SYNCPI")) {
    throw new Error("replay is missing the nested-threshold constraint");
  }
  const status = await page.getByTestId("replay-status").innerText();
  if (!status.includes(`position ${replayMs}`)) {
    throw new Error(`replay status ${status} is not at ${replayMs}`);
  }
  if (!status.includes(`cursor ${replayCursor}/`)) {
    throw new Error(`replay status ${status} is not at cursor ${replayCursor}`);
  }
  await page.waitForTimeout(2200);
  await page.screenshot({ path: path.join(outDir, "replay.png") });
  console.log(`replay ${status}`);
  if (replayCursor) console.log(`expected cursor ${replayCursor}`);
} finally {
  await context.close();
  await browser.close();
}

if (!video) {
  throw new Error("Playwright did not record a video");
}
const saved = await video.path();
const webm = path.join(outDir, "demo.webm");
await copyFile(saved, webm);
await rm(videoDir, { recursive: true, force: true });
console.log(webm);

const gif = path.join(outDir, "demo.gif");
try {
  await execFileAsync("ffmpeg", [
    "-y",
    "-i",
    webm,
    "-vf",
    "fps=6,scale=720:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=96:stats_mode=diff[p];[s1][p]paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle",
    gif,
  ]);
  console.log(gif);
} catch (error) {
  await rm(gif, { force: true });
  console.error("ffmpeg did not write demo.gif; removed a stale GIF so it cannot sit beside a new WebM");
  console.error(error instanceof Error ? error.message : error);
}

try {
  const { stdout } = await execFileAsync("ffprobe", [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    path.join(outDir, "demo.webm"),
  ]);
  console.log(`duration_s ${stdout.trim()}`);
} catch (error) {
  console.error("ffprobe did not report a duration");
  console.error(error instanceof Error ? error.message : error);
}
