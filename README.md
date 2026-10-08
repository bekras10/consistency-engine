# Consistency Engine

Consistency Engine identifies logically related binary prediction-market contracts and tests
whether their *executable* order-book prices violate formal probability constraints, after
depth walking, fees, and fee rounding, under an explicitly enumerated set of admissible
settlement scenarios.

> Independent open-source engineering project. Not an official Kalshi product and not endorsed
> by Kalshi. The default mode is a fully synthetic exchange; results from it say nothing
> empirical about real markets.

**Status:** milestone 1 (core math, synthetic exchange, ingestion core, relationship engine,
pricing engine). See [PROGRESS.md](PROGRESS.md), [docs/mathematical-model.md](docs/mathematical-model.md),
[docs/data-contracts.md](docs/data-contracts.md), and [docs/compliance.md](docs/compliance.md).

```bash
uv sync
make test lint typecheck
```
