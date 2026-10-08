# AGENTS.md — conventions for humans and coding agents

## Non-negotiable rules

1. **No Kalshi network access.** Do not call `api.elections.kalshi.com`, `demo-api.kalshi.co`,
   `trading-api.kalshi.com`, or any Kalshi WebSocket. Reading public documentation pages is fine.
   The Kalshi adapter is refused at startup unless `ENABLE_KALSHI_API=true` **and**
   `KALSHI_AUTHORIZATION_CONFIRMED=true`. CI must never need network access to Kalshi.
2. **No live trading.** No order placement, cancellation, or portfolio modification code.
   `ENABLE_LIVE_TRADING=true` is rejected by `Settings`.
3. **Money is `decimal.Decimal`.** Never use `float` for prices, quantities, fees, costs, or edges.
   - Construct with `consistency_core.money.dec()` (rejects `float` and `bool`).
   - Prices are dollars with up to 4 decimal places (e.g. `0.3333`); quantities up to 2 dp.
   - Rounding only at documented points (fee rounding layer, tick validation). Use
     `ceil_to` / `floor_to` with explicit increments. No implicit `round()`.
   - JSON serialises Decimals as fixed-point strings (`format(d, "f")`), never as numbers.
4. **Determinism.** Engine functions take explicit timestamps (`as_of_ms`); no wall clock inside
   core math. Iterate over sorted collections, never over sets of strings. Seeded `random.Random`
   only (no global `random`, no `hash()` of strings).
5. **Never call a candidate a profit.** Output vocabulary: "candidate", "worst-case payoff",
   "fee-adjusted", "theoretical". Independent books do not imply atomic execution.

## Layout

| Path | Contents |
|---|---|
| `packages/core` (`consistency_core`) | money, tick grids, domain models, order books, relationships, scenarios, fees, pricing, classification, certificates |
| `packages/simulation` (`consistency_simulation`) | seeded synthetic exchange, market families, injected scenarios, event-stream faults, dataset builder |
| `packages/connectors` (`consistency_connectors`) | settings/guards, `MarketDataSource` ABC, synthetic/replay/Kalshi-skeleton sources, local book manager, bounded queues |
| `apps/api` | FastAPI (health only in milestone 1) |
| `apps/worker` | worker entry point (pipeline in milestone 2) |
| `fixtures/` | golden fixtures, fee schedules, relationship reviews, bundled datasets |

Dependency direction: `core` ← `simulation` ← `connectors` ← `apps`. Core imports nothing else.

## Commands

```bash
uv sync                      # install (Python 3.12+)
make test                    # full pytest suite
make test-golden             # golden fixtures A-J
make lint                    # ruff check + ruff format --check
make typecheck               # mypy --strict
make seed                    # regenerate bundled datasets (deterministic)
uv run python scripts/generate_datasets.py --bundled --check   # verify fixtures are reproducible
```

## Testing conventions

- Golden fixtures live in `fixtures/golden/*.yaml`; tests in `tests/golden/`. Expected numbers
  are hand-derived and the arithmetic is written in the test docstring.
- Hypothesis is derandomized by default (`tests/conftest.py`); `HYPOTHESIS_PROFILE=thorough`
  for a longer randomized run.
- Do not weaken a test to make it pass. If a spec number is wrong, document why in PROGRESS.md.
