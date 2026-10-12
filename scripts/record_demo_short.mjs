/**
 * Record a 20–30 second cut of the synthetic Compose dashboard.
 * Playwright captures the UI. ffmpeg concatenates that footage and, when the
 * journal seek runs long, keeps the replay open and the cursor-981 result
 * instead of the wait. Does not draw a UI, does not call an image model, and
 * does not replace docs/demo/demo.webm or docs/demo/demo.gif.
 *
 * If ffmpeg cannot write the GIF, the script deletes a stale demo-short.gif
 * so it cannot sit beside a newer WebM.
 *
 * Required:
 *   DEMO_DETECTION, DEMO_RELATIONSHIP, DEMO_REPLAY_MS, DEMO_REPLAY_CURSOR, DEMO_CERT_HASH
 * Optional:
 *   DEMO_URL (default http://127.0.0.1:3000)
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
const videoDir = path.join(outDir, ".video-short");
const webm = path.join(outDir, "demo-short.webm");
const gif = path.join(outDir, "demo-short.gif");

if (!detection || !relationship || !replayMs || !certHash || !replayCursor) {
  console.error(
    "Set DEMO_DETECTION, DEMO_RELATIONSHIP, DEMO_REPLAY_MS, DEMO_REPLAY_CURSOR, and DEMO_CERT_HASH from make demo.",
  );
  process.exit(1);
}

function digestOf(text) {
  return `sha256:${createHash("sha256").update(text).digest("hex")}`;
}

async function durationOf(file) {
  const { stdout } = await execFileAsync("ffprobe", [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    file,
  ]);
  const value = Number(stdout.trim());
  if (!Number.isFinite(value)) {
    throw new Error(`ffprobe did not report a duration for ${file}`);
  }
  return value;
}

await rm(videoDir, { recursive: true, force: true });
await mkdir(videoDir, { recursive: true });
await mkdir(outDir, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1280, height: 800 },
  recordVideo: { dir: videoDir, size: { width: 1280, height: 800 } },
});

const intro = await context.newPage();
const introVideo = intro.video();
try {
  await intro.goto(`${base}/`);
  await intro.getByTestId("synthetic-banner").waitFor();
  await intro.waitForTimeout(1800);

  await intro.goto(`${base}/relationships/${encodeURIComponent(relationship)}`);
  const constraints = intro.getByText("Formal constraints");
  await constraints.waitFor();
  await constraints.scrollIntoViewIfNeeded();
  await intro.waitForTimeout(2200);

  await intro.goto(`${base}/detections/${encodeURIComponent(detection)}`);
  const quote = intro.getByRole("heading", { name: "Last qualifying quote" });
  await quote.waitFor();
  await intro.getByTestId("quote-not-executable").waitFor();
  await intro.getByTestId("synthetic-detection").waitFor();
  await quote.scrollIntoViewIfNeeded();
  await intro.waitForTimeout(2400);

  const proof = intro.getByTestId("proof");
  await proof.scrollIntoViewIfNeeded();
  await intro.waitForTimeout(2200);
  const shownProof = await intro.getByTestId("certificate-hash").innerText();
  if (!shownProof.includes(certHash)) {
    throw new Error(`qualifying certificate label ${shownProof} != ${certHash}`);
  }
  const report = await intro.getByTestId("technical-report").innerText();
  const hashLine = report.match(/^certificate_hash: (sha256:[0-9a-f]+)/m);
  const marker = "certificate_json:\n";
  const jsonAt = report.indexOf(marker);
  if (!hashLine || jsonAt < 0) {
    throw new Error("technical report is missing the qualifying certificate");
  }
  const certificateJson = report.slice(jsonAt + marker.length).trim();
  if (digestOf(certificateJson) !== certHash || hashLine[1] !== certHash) {
    throw new Error(`displayed certificate does not match ${certHash}`);
  }
} finally {
  await intro.close();
}
if (!introVideo) {
  throw new Error("Playwright did not record the intro");
}
const introWebm = await introVideo.path();

const replayPage = await context.newPage();
const replayVideo = replayPage.video();
try {
  await replayPage.goto(`${base}/detections/${encodeURIComponent(detection)}`);
  await replayPage.getByTestId("replay-link").click();
  await replayPage.getByTestId("synthetic-replay").waitFor();
  const started = replayPage.waitForResponse(
    (response) => response.url().includes("/start") && response.request().method() === "POST",
  );
  await replayPage.getByTestId("replay-start").click();
  await started;
  await replayPage.getByTestId("replay-pause").click();
  await replayPage.getByTestId("replay-seek-cursor").fill(replayCursor);
  const sought = replayPage.waitForResponse(
    (response) => response.url().includes("/seek") && response.request().method() === "POST",
    { timeout: 180_000 },
  );
  await replayPage.getByRole("button", { name: "Seek cursor" }).click();
  await sought;
  await replayPage.getByTestId("replay-detection").waitFor({ timeout: 30_000 });
  const shown = await replayPage.getByTestId("replay-detection").innerText();
  if (!shown.includes(detection) || !shown.includes("FEE_ADJUSTED_CANDIDATE")) {
    throw new Error(`replay detection panel does not show the qualifying detection: ${shown.slice(0, 240)}`);
  }
  const shownHash = await replayPage.getByTestId("replay-certificate").innerText();
  if (!shownHash.includes(certHash)) {
    throw new Error(`replay certificate ${shownHash} != ${certHash}`);
  }
  const status = await replayPage.getByTestId("replay-status").innerText();
  if (!status.includes(`cursor ${replayCursor}/`) || !status.includes(`position ${replayMs}`)) {
    throw new Error(`replay status ${status} is not cursor ${replayCursor} at ${replayMs}`);
  }
  await replayPage.getByTestId("replay-detection").scrollIntoViewIfNeeded();
  await replayPage.waitForTimeout(6500);
  console.log(`replay ${status}`);
} finally {
  await replayPage.close();
  await context.close();
  await browser.close();
}
if (!replayVideo) {
  throw new Error("Playwright did not record the replay");
}
const replayWebm = await replayVideo.path();
const introS = await durationOf(introWebm);
const replayS = await durationOf(replayWebm);
const head = 3.2;
const tail = 6.2;
const budget = 28.5 - introS;
let replayFilter = "[1:v]setpts=PTS-STARTPTS[rv]";
if (introS + replayS > 29.5 && replayS > head + tail && budget > head + 4) {
  const useTail = Math.min(tail, budget - head);
  const tailStart = Math.max(0, replayS - useTail);
  replayFilter =
    `[1:v]trim=start=0:end=${head.toFixed(3)},setpts=PTS-STARTPTS[rh];` +
    `[1:v]trim=start=${tailStart.toFixed(3)}:end=${replayS.toFixed(3)},setpts=PTS-STARTPTS[rt];` +
    `[rh][rt]concat=n=2:v=1:a=0[rv]`;
}
const staged = path.join(videoDir, "cut.webm");
await execFileAsync("ffmpeg", [
  "-y",
  "-i",
  introWebm,
  "-i",
  replayWebm,
  "-filter_complex",
  `[0:v]setpts=PTS-STARTPTS[iv];${replayFilter};[iv][rv]concat=n=2:v=1:a=0[out]`,
  "-map",
  "[out]",
  "-an",
  "-c:v",
  "libvpx-vp9",
  "-b:v",
  "900k",
  staged,
]);
await copyFile(staged, webm);
await rm(videoDir, { recursive: true, force: true });
console.log(webm);

try {
  await execFileAsync("ffmpeg", [
    "-y",
    "-i",
    webm,
    "-vf",
    "fps=8,scale=720:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=128:stats_mode=diff[p];[s1][p]paletteuse=dither=bayer:bayer_scale=3:diff_mode=rectangle",
    gif,
  ]);
  console.log(gif);
} catch (error) {
  await rm(gif, { force: true });
  console.error("ffmpeg did not write demo-short.gif; removed a stale GIF so it cannot sit beside the WebM");
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
}

const seconds = await durationOf(webm);
console.log(`duration_s ${seconds.toFixed(6)}`);
if (seconds < 20 || seconds > 30) {
  console.error(`demo-short.webm duration ${seconds} is outside 20–30 seconds`);
  process.exitCode = 1;
}
