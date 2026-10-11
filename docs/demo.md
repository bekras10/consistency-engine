# Demonstration

This is a synthetic simulation. The prices, the CPI contracts, and the detection
come from the bundled inconsistent dataset. They are not live-venue quotes and
they are not a measurement of a real market.

The sequence is the Compose deployment, project `consistency-deploy`, frontend
`http://127.0.0.1:3000`. Replay of the completed journal is what moves the book.
A live tick of that session is already finished, so a 20–30 second viewing uses
playback rather than waiting for the generator.

## Initialize

From the repository root, with Docker running:

```bash
make demo
```

`scripts/demo.sh` starts `docker compose -p consistency-deploy up -d` only when
`http://127.0.0.1:3000/` is not already serving the page. It waits until the HTML
contains `Synthetic simulation`, then reads one implication classified
`FEE_ADJUSTED_CANDIDATE` from Postgres. It does not hardcode the detection id.
It then fetches `/detections/<id>` and exits if the page is missing the stored
detection id, certificate hash, gross edge, fees, net edge, minimum payoff per
unit, or classification.

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
   Evidence checks are `pass` for the shared synthetic underlying, measurement,
   location, window, and settlement source.
3. Open the printed detection
   (`/detections/det-f26eee89fc522826ab5d90f1`). The row is
   `FEE_ADJUSTED_CANDIDATE`, status `EXPIRED`. The basket is NO on
   `SYNCPI-26AUG-GE0.3` and YES on `SYNCPI-26AUG-GE0.2`, which is the injected
   break of `P(GE0.3) <= P(GE0.2)`.
4. Read the quote: gross edge, fees, net edge, and depth quantity. The bid/ask
   chart is executable depth from the persisted book (YES bid in green, YES ask
   in red). The page states that these levels are the latest book, not a
   reconstruction of the first observation.
5. Scroll to **Scenario payoffs**. Payoff per unit is 1, one row is marked worst
   case, and the certificate hash is printed. **Fees in the certificate** is the
   empty schedule list. That is the stored closing certificate, not the headline
   quote. See the comparison below.
6. Choose **Replay**. Press **Start**, then **Step**. The status line leaves
   `ready`. On the recording it became `paused · cursor 28/12277 · position
   1784124000024 · speed 1`, and the book shown was `SYNCHAMP-26-OWLS` at YES
   `0.25 / 0.28`. That cursor change is the historical replay of the same
   journal.

## Stored figures compared with the page

Queried from the `consistency-deploy` database on 2026-10-11, detection
`det-f26eee89fc522826ab5d90f1`, then checked against
`http://127.0.0.1:3000/detections/det-f26eee89fc522826ab5d90f1`. `make demo`
printed `page matches the stored detection row and certificate payoff`.

| Field | Stored | On the page |
|---|---|---|
| Classification | `FEE_ADJUSTED_CANDIDATE` | same string |
| Gross edge | `8.4180` | `8.4180` |
| Fees | `0.958000` | `0.958000` |
| Net edge | `7.460000` | `7.460000` |
| Maximum net edge | `26.310000` | `26.310000` |
| Depth quantity | `153` | `153` |
| Record `worst_case_payoff` | `153` | depth quantity `153`; scenario payoff per unit `1` |
| Certificate `min_payoff_per_unit` | `1` | `min_payoff_per_unit` is in the page HTML |
| Certificate hash | `sha256:0240383b5da7dea8aeb28ce478bd6e9655c5aa31468692fe1b938e0ebfb107fd` | same string under Scenario payoffs |

Fee arithmetic on the stored quote: `8.4180 − 0.958000 = 7.460000`.

Worst-case payoff on the detection record is `153`. The certificate payoff is
enumerated, `min_payoff_per_unit` `"1"`, `max_payoff_per_unit` `"2"`, constant
`"1"`. The worst state is all zeros for `SYNCPI-26AUG-GE0.1`, `GE0.2`, `GE0.3`,
and `GE0.4`. `153 × 1 = 153`.

The closing certificate is not the same object as the headline quote. Its
classification is `INSUFFICIENT_LIQUIDITY`. `fees` is JSON null, `fee_schedules`
is `[]`, and `validation.first_failing_gate` is `markets_open_with_asks`. The
page shows that empty list under **Fees in the certificate** and still shows the
row's `0.958000` under **Fees**. The proof is the stored certificate. It is not
hardcoded in the frontend.

## Recording

Captured from the running Compose UI with Playwright's Chromium
(`scripts/record_demo.mjs`, headless, viewport 1280×800) and ffmpeg 8.1.1.
Duration of `demo.webm` is 21.76 seconds. No image was drawn or generated by an
image model.

| File | What it is |
|---|---|
| `docs/demo/overview.png` | Dashboard, synthetic banner, detection activity |
| `docs/demo/relationship.png` | Verified nested-threshold constraints |
| `docs/demo/detection-fees.png` | Headline fees, net edge, depth 153 |
| `docs/demo/detection.png` | Scenario payoffs and certificate hash |
| `docs/demo/replay.png` | Cursor 28 after Start and Step |
| `docs/demo/demo.webm` | 21.76 s, 1280×800 |
| `docs/demo/demo.gif` | Same take, 720×450, 6 fps, about 1.4 MB |

To record again after `make demo` prints the ids:

```bash
DEMO_DETECTION=<printed detection id> \
DEMO_RELATIONSHIP=<printed relationship id> \
node scripts/record_demo.mjs
ffmpeg -y -i docs/demo/.video/*.webm \
  -vf "fps=6,scale=720:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=96:stats_mode=diff[p];[s1][p]paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle" \
  docs/demo/demo.gif
```

`docs/demo/.video/` is scratch and is gitignored. If Playwright or ffmpeg is
missing, do not substitute a drawing. Follow the click path above and capture
the window with `screencapture` or another screen recorder.
