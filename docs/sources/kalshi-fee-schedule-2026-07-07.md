<!--
Transcribed by the repository owner from official Kalshi sources on 2026-10-08:
- Fee schedule PDF (effective 2026-07-07): https://kalshi.com/docs/kalshi-fee-schedule.pdf
- Current schedule landing page:          https://kalshi.com/fee-schedule
- Rounding docs:                          https://docs.kalshi.com/getting_started/fee_rounding
The PDF itself is intentionally NOT committed (redistribution permission unknown).
No automated fetch of the PDF was attempted by this repository's build (see PROGRESS.md).
-->

# Kalshi fee schedule (event contracts) — owner transcription, effective 2026-07-07

## 1. Base-fee formulas (event contracts), before rounding

- Taker: `M_taker * 0.07 * C * P * (1 - P)`
- Maker: `M_maker * 0.0175 * C * P * (1 - P)`
- `C` = contract quantity (fractional allowed), `P` = execution price in dollars,
  `M` = series-specific multiplier. Default `M_taker = 1`, default `M_maker = 0`.
- There is no separate settlement fee in the published schedule.
- Perpetual-futures fees are out of scope and use a separate tiered structure; they are not part
  of the binary event-contract fee engine.

## 2. Series exceptions (Non-Standard Fees table) — rows verified by the owner

| Series | Maker M | Taker M |
|---|---|---|
| KXCPI | 1 | 1 |
| KXFED | 1 | 1 |
| KXMLBGAME | 1 | 1 |
| KXNFLGAME | 1 | 1 |
| KXBALLONDOR | 1 | 1 |
| KXBTCY | 0 | 0 |
| KXETHY | 0 | 0 |
| KXDOED | 0 | 0 |
| KXMVE (specified combos) | 2 | 1 |

**Completeness: PARTIAL.** This is not the complete table. The PDF also contains an exclusion
for uncorrelated NFL combos under KXMVE whose exact wording was not transcribed.

## 3. Rounding

Per the fee-rounding documentation (see `kalshi-fee-rounding-2026-10-08.md`):
`trade_fee = ceil_6dp(model_fee)`; balance alignment to $0.0001 (direct members) or $0.01
(non-direct members); per-order rounding accumulator with rebates in whole precision increments.

## 4. Official worked examples supplied by the owner

- Single fill, NON_DIRECT, 1 contract @ $0.055 (signed revenue -$0.055000): model fee
  $0.00363825 (= 0.07 x 1 x 0.055 x 0.945), trade_fee $0.003639, aligned_change -$0.060000,
  rounding_fee $0.001361, net fee before rebate $0.005000.
- Accumulator, NON_DIRECT, three fills each contributing $0.004 rounding: accumulator
  $0.004 / $0.008 / $0.012; rebates 0 / 0 / $0.010; remaining accumulator $0.002.
- Reference taker fees (M = 1): P=$0.50, C=1 -> raw $0.0175, NON_DIRECT net $0.02;
  P=$0.50, C=100 -> $1.75; P=$0.10, C=100 -> $0.63.
