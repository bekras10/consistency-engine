# Demonstration

This is a synthetic simulation. The prices, the CPI contracts, and the detection
come from the bundled inconsistent dataset. They are not live-venue quotes and
they are not a measurement of a real market. The replay page says the cursor
plays the archived journal and is not a live Kalshi feed.

The sequence is the Compose deployment, project `consistency-deploy`, frontend
`http://127.0.0.1:3000`. Replay of the completed journal is what moves the book.
A live tick of that session is already finished, so the viewing uses playback
rather than waiting for the generator.

## Initialize

From the repository root, with Docker running:

```bash
make demo
```

`scripts/demo.sh` starts `docker compose -p consistency-deploy up -d` only when
`http://127.0.0.1:3000/` is not already serving the page. It waits until the HTML
contains `Synthetic simulation`, then reads one implication classified
`FEE_ADJUSTED_CANDIDATE` from Postgres. It does not hardcode the detection id.
It checks that SHA-256 of the stored certificate JSON equals the stored
certificate hash, that the closing classification is a different value, and that
the qualifying journal prefix is strictly before the later entries that share
the qualifying timestamp. It then fetches `/detections/<id>` and exits if the
page is missing the stored detection id, certificate hash, quote, closing
condition, or the synthetic-data label.

Set `DEMO_BUILD=1` to rebuild images. Set `WEB_PORT` if the frontend is not on
3000. The script does not call Kalshi and does not publish Postgres.

## Click path

1. Open `http://127.0.0.1:3000/`. The banner says **Synthetic simulation**.
   Source is `synthetic:inconsistent` and the session is completed. The activity
   chart and the recent-detections table are the seeded journal, not a live tick.
2. Open Relationships and the printed relationship
   (`/relationships/rel-32e08488281acae6` on this volume). Type
   `nested_thresholds`, verification `verified`. Constituents are the four
   synthetic August 2026 CPI thresholds. Formal constraints, stored on the
   relationship document:
   - `P(SYNCPI-26AUG-GE0.4) <= P(SYNCPI-26AUG-GE0.3)`
   - `P(SYNCPI-26AUG-GE0.3) <= P(SYNCPI-26AUG-GE0.2)`
   - `P(SYNCPI-26AUG-GE0.2) <= P(SYNCPI-26AUG-GE0.1)`
3. Open the printed detection
   (`/detections/det-f26eee89fc522826ab5d90f1`). The retained classification is
   `FEE_ADJUSTED_CANDIDATE`, status `EXPIRED`. The page labels the figures
   **Last qualifying quote** and says they are not a currently executable
   opportunity. The basket is NO on `SYNCPI-26AUG-GE0.3` and YES on
   `SYNCPI-26AUG-GE0.2`, which is the injected break of `P(GE0.3) <= P(GE0.2)`.
4. Read the qualifying quote: gross edge `8.4180`, fees `0.958000`, net edge
   `7.460000`, maximum net edge `26.310000`, depth quantity `153`. The bid/ask
   chart is the latest persisted book, not a reconstruction of the first
   observation.
5. Scroll to **Scenario payoffs**. Payoff per unit is `1`, one row is marked
   worst case, and the qualifying certificate hash is printed. **Fees in the
   qualifying certificate** is the fee schedule used for that proof.
   **Closing condition** is separate: classification `INSUFFICIENT_LIQUIDITY`,
   reason `NO_ASKS`. That object is not the qualifying proof.
6. Choose **Replay**. Press **Start**, then **Pause**. Type cursor `981` and
   press **Seek cursor**. The status line becomes `paused · cursor 981/12277 ·
   position 1784124010025 · speed 1`. The panel shows detection
   `det-f26eee89fc522826ab5d90f1` as `FEE_ADJUSTED_CANDIDATE`, the nested
   CPI constraints, the qualifying certificate hash, and the `SYNCPI-26AUG-GE0.3`
   and `SYNCPI-26AUG-GE0.2` books. Seeking the timestamp `1784124010025` instead
   would continue through cursor `994`, which is after the close, so the
   detection would no longer be active.

## Stored figures compared with the page

Queried from the `consistency-deploy` database on 2026-10-11, detection
`det-f26eee89fc522826ab5d90f1`, then checked against
`http://127.0.0.1:3000/detections/det-f26eee89fc522826ab5d90f1`. `make demo`
printed `qualifying certificate hash matches the stored JSON` and
`page matches the stored detection row and certificate payoff`.

| Field | Stored | On the page |
|---|---|---|
| Qualifying classification | `FEE_ADJUSTED_CANDIDATE` | same string, labeled last qualifying quote |
| Status | `EXPIRED` | same string |
| Gross edge | `8.4180` | `8.4180` |
| Fees | `0.958000` | `0.958000` |
| Net edge | `7.460000` | `7.460000` |
| Maximum net edge | `26.310000` | `26.310000` |
| Depth quantity | `153` | `153` |
| Record `worst_case_payoff` | `153` | `153`; scenario payoff per unit `1` |
| Certificate `min_payoff_per_unit` | `1` | `1` in the scenario table |
| Qualifying certificate hash | `sha256:0240383b5da7dea8aeb28ce478bd6e9655c5aa31468692fe1b938e0ebfb107fd` | same string under Scenario payoffs |
| Closing classification | `INSUFFICIENT_LIQUIDITY` | same string under Closing condition |
| Close reason | `NO_ASKS` | same string |
| Closing certificate hash | `sha256:1f837ec74d44b4012131a54da84259ea3443b646334fa0b336fc967364dc1140` | same string, labeled as not the qualifying proof |

Fee arithmetic on the stored qualifying quote: `8.4180 − 0.958000 = 7.460000`.

Worst-case payoff on the detection record is `153`. The qualifying certificate
payoff is enumerated, `min_payoff_per_unit` `"1"`, and the worst state is all
zeros for `SYNCPI-26AUG-GE0.1`, `GE0.2`, `GE0.3`, and `GE0.4`. `153 × 1 = 153`.

SHA-256 of the exact stored `certificate_json` (14291 bytes) is
`sha256:0240383b5da7dea8aeb28ce478bd6e9655c5aa31468692fe1b938e0ebfb107fd`,
which equals the stored `certificate_hash`. SHA-256 of the stored
`closing_certificate_json` (8281 bytes) is
`sha256:1f837ec74d44b4012131a54da84259ea3443b646334fa0b336fc967364dc1140`,
which equals `closing_certificate_hash`. The qualifying certificate's
classification is `FEE_ADJUSTED_CANDIDATE`. The closing certificate's
classification is `INSUFFICIENT_LIQUIDITY`.

The qualifying observation is journal ordinal `980`, pipeline timestamp
`1784124010025`. Replay cursor `981` is the prefix through that ordinal.
Nineteen journal rows share that timestamp (ordinals 975–993). The detection
is active with this certificate only through cursor `981`. Cursor `982` is the
close. The database was produced by replaying the bundled recording with the
fixed writer (`docker compose -p consistency-deploy down -v`, then a rebuild).
The certificate JSON was not pasted in by hand.

At cursor `981` the replay panel shows the same hash. Playback rebuilds books
with the recording source label `synthetic:inconsistent`, so the certificate
matches the stored proof. The label on the page still says this is not a live
Kalshi feed.

## Recording

Captured from the running Compose UI with Playwright's Chromium
(`scripts/record_demo.mjs`, headless, viewport 1280×800) and ffmpeg 8.1.1.
`ffprobe` reports `demo.webm` duration `44.760000` seconds and `demo.gif`
duration `44.840000` seconds. No image was drawn or generated by an image model.
The take is longer than 30 seconds because Start, Pause, and Seek cursor apply
the 981-entry prefix before the qualifying books are on screen.

| File | What it is |
|---|---|
| `docs/demo/overview.png` | Dashboard, synthetic banner, detection activity |
| `docs/demo/relationship.png` | Verified nested-threshold constraints |
| `docs/demo/detection-fees.png` | Last qualifying quote: net `7.460000`, fees `0.958000`, depth `153` |
| `docs/demo/detection.png` | Scenario payoffs and qualifying certificate hash |
| `docs/demo/replay.png` | Cursor `981/12277`, CPI constraints, the same certificate |
| `docs/demo/demo.webm` | 44.76 s, 1280×800, 4,290,463 bytes |
| `docs/demo/demo.gif` | Same take, 720 px wide, 6 fps, 2,249,847 bytes |

To record again after `make demo` prints the ids:

```bash
DEMO_DETECTION=<printed detection id> \
DEMO_RELATIONSHIP=<printed relationship id> \
DEMO_REPLAY_MS=<printed timestamp> \
DEMO_REPLAY_CURSOR=<printed cursor> \
DEMO_CERT_HASH=<printed hash> \
node scripts/record_demo.mjs
```

The script writes `docs/demo/demo.webm`, refreshes the screenshots, and tries
ffmpeg for `docs/demo/demo.gif`. `docs/demo/.video/` is scratch and is
gitignored. If Playwright or ffmpeg is missing, do not substitute a drawing.
