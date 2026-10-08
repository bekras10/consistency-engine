# Data contracts

This document records how external payloads map onto the exchange-agnostic domain models, what
was verified against official documentation, and what remains an assumption.

## Documentation access log (2026-10-08)

No Kalshi API endpoint (REST or WebSocket, production or demo) was contacted at any point.

| Page | Status |
|---|---|
| https://docs.kalshi.com/getting_started/fee_rounding | Automated fetch **hung and was abandoned**. Content later supplied verbatim by the owner, saved at [`docs/sources/kalshi-fee-rounding-2026-10-08.md`](sources/kalshi-fee-rounding-2026-10-08.md). |
| https://kalshi.com/docs/kalshi-fee-schedule.pdf | **Not fetched** (one attempt was permitted; it was skipped deliberately after the earlier fetch hung for ~15 min). Owner transcription saved at [`docs/sources/kalshi-fee-schedule-2026-07-07.md`](sources/kalshi-fee-schedule-2026-07-07.md). Exception table therefore **PARTIAL**. |
| https://kalshi.com/fee-schedule | Not fetched. Must be checked for revisions after 2026-07-07 before any authorized live integration. |
| getting_started/fixed_point_migration | Not fetched — field names below are **unverified**. |
| getting_started/orderbook_responses | Not fetched — **unverified**. |
| websockets/orderbook-updates | Not fetched — **unverified**. |
| quick_start_websockets, rate_limits, get-markets, get-event, get-series | Not fetched — **unverified**; relevant only to the milestone-4 connector. |

All pages marked unverified must be read and this document corrected before the authorized
connector is enabled. Every assumed field name is centralised in
`consistency_core.normalization.FieldMap`, so corrections are a one-place change.

## Numeric conventions

| Quantity | Representation | Validation |
|---|---|---|
| Price | `Decimal` dollars, string on the wire (e.g. `"0.3800"`) | strictly in (0, 1); at most 4 dp; on the market's tick grid |
| Quantity | `Decimal` contracts, string on the wire (e.g. `"12.50"`) | >= 0 (book levels), at most 2 dp |
| Delta | signed `Decimal` string (e.g. `"-54.00"`) | non-zero, at most 2 dp |
| Fees | `Decimal`, rounded up to $0.000001 then balance-aligned | see fee section |
| Timestamps | integer epoch **milliseconds** for stream/book timing; aware UTC `datetime` for metadata | |

Floats are refused by `consistency_core.money.dec` and by every Pydantic money field. JSON output
uses fixed-point strings (`format(d, "f")`), never JSON numbers and never exponent notation.

## Assumed Kalshi field names (UNVERIFIED best knowledge)

### REST order book

```json
{"orderbook_fp": {"yes_dollars": [["0.4500", "100.00"]], "no_dollars": [["0.5300", "20.00"]]}}
```

Each side lists **bids** as `[price_dollars, quantity_fp]`. A legacy shape with integer cents
(`{"orderbook": {"yes": [[45, 100]], "no": [[53, 20]]}}`) is also accepted.

### WebSocket order-book channel

```json
{"type": "orderbook_snapshot", "sid": 2, "seq": 3,
 "msg": {"market_ticker": "X", "yes_dollars_fp": [["0.0800", "300.00"]], "no_dollars_fp": []}}
{"type": "orderbook_delta", "sid": 2, "seq": 4,
 "msg": {"market_ticker": "X", "price_dollars": "0.9600", "delta_fp": "-54.00", "side": "yes"}}
```

`seq` is treated as scoped to the subscription `sid`, **not** to a market: a gap on a `sid`
desynchronises every market carried on it.

### Market metadata / tick grid

`price_ranges: [{"start": "0.0000", "end": "0.1000", "step": "0.0010"}, ...]` (preferred),
`tick_size_dollars: "0.01"`, or legacy `tick_size: 1` (integer cents).

## Binary book semantics (implemented)

YES ask = 1 − NO bid; NO ask = 1 − YES bid, with the opposing bid's quantity. Ask curves come
only from displayed opposing bids (no invented liquidity). A book whose best YES bid + best NO
bid >= 1 is rejected as crossed/locked (those orders would have matched).

## Fees — verification split

Detailed in [mathematical-model.md](mathematical-model.md#5-fee-model).

**Sourced from official documentation (owner-supplied copies):**

- Rounding layer: `trade_fee = ceil_6dp(model_fee)`; `aligned_change = floor_precision(revenue −
  trade_fee)` with signed revenue (negative for buyers); `rounding_fee` = remainder; per-order
  accumulator persisting across taker and maker fills; rebates in whole precision increments;
  net fee never negative. Member precision DIRECT = $0.0001, NON_DIRECT = $0.01.
- Fee formulas: taker `M_taker · 0.07 · C · P · (1 − P)`, maker `M_maker · 0.0175 · C · P ·
  (1 − P)`; defaults `M_taker = 1`, `M_maker = 0`; the nine listed series rows; effective
  2026-07-07. Stored as versioned config `fixtures/fees/kalshi-2026-07-07.yaml` with
  `verification_status: VERIFIED_AGAINST_PUBLISHED_SCHEDULE` (formulas + listed rows only).

**Interpretation (not confirmed by a captured worked example):** the exact rebate cap rule
(see mathematical-model.md §5.3).

**Requires exchange/account confirmation (yields FEE_UNVERIFIED until resolved):** member
classification, FCM/intermediary fees, the complete exception table (stored table is PARTIAL —
any unlisted real series is FEE_UNVERIFIED), KXMVE combo classification (and the uncorrelated-NFL
exclusion wording), and schedule revisions after 2026-07-07.

The synthetic exchange uses a **fictional** schedule (`fixtures/fees/synthetic-fictional-v1.yaml`)
routed through the same rounding layer.
