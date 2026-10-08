<!--
Retrieved manually by the repository owner from the public Kalshi documentation on 2026-10-08:
https://docs.kalshi.com/getting_started/fee_rounding
Saved verbatim as supplied. Not fetched by any automated process in this repository.
The "FCM-cleared fill" and "Accumulator across fills" worked examples were collapsed on the page
and were NOT captured in this copy.
-->

Fee Rounding
How trade fees and balances are rounded.

Overview
User balances have a target precision before and after every fill:
- Direct member balances are aligned to $0.0001 (0.01c)
- Non-direct member balances are aligned to $0.01 (1c)
When a trade produces a balance change that is more precise than the user's target balance precision, the exchange charges a rounding fee to bring the balance back to that target. The fee accumulator applies across all fills of an order so that the total fee converges to what a single equivalent fill would cost.
Fees are six-decimal dollar amounts ($0.000001 granularity) - the finest precision a fill's revenue (price x quantity) can occupy. Every fill produces three fee components:
| Component | Description |
| Trade fee | Fee from the fee model, rounded up to the nearest $0.000001 |
| Rounding fee | Adjustment that restores the user's target balance precision |
| Rebate | Refund from accumulated rounding overpayment, aligned to the user's target balance precision |
Net fee = trade fee + rounding fee - rebate (always >= $0.00)

Rounding Mechanics
Given a fill's signed revenue (negative for buyers) and model fee:
1. Compute trade_fee = ceil_6dp(model_fee)
2. Compute aligned_change = floor_precision(revenue - trade_fee)
3. Compute rounding_fee = (revenue - trade_fee) - aligned_change
4. Add the rounding fee to the order's accumulator
5. Rebate accumulated rounding in increments of the user's target balance precision, capped so the fill's net fee cannot be negative
Before any rebate, the user's balance changes by aligned_change, which is always on the user's precision grid.

Fee Accumulator
The fee accumulator carries rounding overpayment across an order's fills. Rebates use the user's target balance precision: $0.0001 for direct members and $0.01 for non-direct members.
The fee accumulator is maintained per order across all fills regardless of whether the fills are taker or maker. If an order initially takes (matching resting orders) and then becomes a resting maker order, the accumulated rounding carries over to subsequent maker fills.

Worked Examples
(The 'FCM-cleared fill' and 'Accumulator across fills' examples were collapsed on the page and were NOT captured.)
