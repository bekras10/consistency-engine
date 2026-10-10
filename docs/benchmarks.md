# Benchmarks

Synthetic data only. This run did not contact Kalshi. GitHub Actions does not run
`make benchmark`; the numbers below are from the local run that wrote
`docs/benchmark-results.json`.

## Hardware and command

| Item | Value |
|---|---|
| CPU | Apple M3 |
| Memory | 17179869184 bytes (16 GiB) |
| OS | Darwin 26.6.2 (arm64) |
| Python | 3.12.11 |
| PostgreSQL | 16.15 (Debian 16.15-1.pgdg13+2), Docker, host port 5433 |
| Database | `consistency_bench` on that server (created by the harness; the demo database is not used) |
| Log level | WARNING, so per-detection log lines are not part of the measured I/O |

```
DATABASE_URL=postgresql+asyncpg://consistency:consistency@127.0.0.1:5433/consistency make benchmark
```

`make benchmark` runs `uv run --frozen python scripts/benchmark.py` with the defaults
below. The harness refuses port 5432 and any non-PostgreSQL URL. Exit 0 means every
workload below finished and the replay comparison matched. A missed speed target does
not change the exit code.

## Load parameters

| Parameter | Value |
|---|---|
| Seed | 606 |
| Families | the six synthetic families |
| Markets per instance | 27 (one calibration instance, 1000 ms, 0.0172 s to generate) |
| Tick | 250 ms |
| Message target | 4000 (duration is chosen from the calibration rate; one tick is the minimum) |
| Focused workload | 250 markets, used for the latency target and for replay |
| Paced rate | 500 messages/s, start-to-start, on the focused market count, message target capped at 2000 |
| Per-workload budget | 180 s |
| Worker queues | inbound 10000, outbox 1024 (the process defaults) |

Market counts are `ceil(target / 27) * 27`, so the rows are 54, 270, 1026, and 5022 markets.

## What is measured

The harness generates a session with `SyntheticExchange`, discovers relationships, then
runs the real `WorkerService` into PostgreSQL: ingestion, detection, journal, and outbox.
`messages_per_s` is pipeline listener entries divided by that wall time, including
session open and database waits. Processing latency is `perf_counter` around
`on_message`, before the outbox wait. Detection latency is `perf_counter` around
`pricing.evaluate`, one sample per call. Database rows per second are committed
`orderbook_updates` + `orderbook_snapshots` + `notification_outbox` + `session_checkpoints`
divided by the same wall time. RSS is the process resident size at the end of the
workload. CPU is user+system time of the process divided by wall time, as percent of
one core.

These books produced evaluations and no detection lifecycle events, so the detection
outbox stayed empty (`outbox_high_water` 0, `detections` 0). The backpressure section
is a separate probe of that queue.

On the 5022-market run the processing-latency percentiles use the last 10000
`on_message` samples. The listener keeps that window. Detection latency uses every
`evaluate` call.

## Results

| Markets | Messages | Msg/s | Processing p50/p95/p99 (ms) | Detection p50/p95/p99 (ms) | DB rows/s | RSS MiB | CPU % | Inbound high water |
|---|---:|---:|---|---|---:|---:|---:|---:|
| 54 | 3973 | 1310.33 | 0.7560 / 1.1782 / 1.2947 | 0.3338 / 0.4797 / 0.5371 | 1328.80 | 91.6 | 87.27 | 3972 |
| 270 | 4285 | 1093.72 | 0.6167 / 1.1711 / 1.3354 | 0.3371 / 0.4849 / 0.5816 | 1163.40 | 110.5 | 79.80 | 4284 |
| 1026 | 5161 | 658.20 | 0.3632 / 1.1749 / 1.3181 | 0.3365 / 0.4873 / 0.5753 | 789.43 | 195.5 | 71.00 | 5160 |
| 5022 | 10713 | 313.48 | 0.7003 / 1.1837 / 1.3213 | 0.3335 / 0.4884 / 0.5782 | 460.61 | 866.4 | 71.69 | 10000 |

Wall time was 3.0321 s, 3.9178 s, 7.8411 s, and 34.1739 s. Relationship discovery was
0.0057 s, 0.0156 s, 0.0531 s, and 0.4861 s, measured before the worker and not included
in msg/s. The 5022-market run finished; it was not skipped.

### Paced workload

270 markets, 2410 messages, configured start-to-start interval of 1/500 s.
Achieved **378.40 messages/s** in 6.3689 s. Inbound high water was **43** (cap 10000).
Outbox high water was 0. CPU was 41.45% of one core. RSS at the end of this workload
was 824.6 MiB because the process still held the 5022-market run.

A standalone `asyncio.sleep` loop of 2409 iterations at a 2 ms deadline on this machine
achieved 500.19 iterations/s in 4.8161 s. The worker is slower because detection and
database writes share that thread with the pacer. The queue did not absorb the whole
recording the way the unpaced runs did.

### Reconnection recovery

One instance (27 markets), 12 s of simulated time, one synthetic gap at 4 s. The
recording does not deliver the skipped sequence. It emits fresh snapshots on a new
subscription id. The harness times that snapshot burst.

Two bursts, 10 snapshots. p50 **7.9292 ms**, max **11.2319 ms**. The worker then
finished the session at 1141.65 messages/s.

### Replay

The 270-market journal (4285 entries) was applied twice with `replay_catalog`.
The second pass took 2.3577 s, **1817.44 entries/s**. `compare_outcomes` reported
`identical: true`, 0 event mismatches, 0 classification mismatches.

### Backpressure

`Outbox` maxsize 4, a sink that sleeps 10 ms, 40 batches. High water **4**, writes
**40**, bounded **true**, elapsed **0.4387 s**. The queue did not grow past its cap.

On the unpaced runs the inbound queue held the whole finite recording whenever that
was under 10000 (3972, 4284, 5160). At 5022 markets the stream had 10713 messages and
the inbound high water was exactly **10000**, the cap. `asyncio.Queue` blocks at that
size. It did not grow without a bound.

## Profile

Taken before any attempt to change the hot path, on a 270-market session
(2410 messages, 5.137 s under cProfile):

```
DATABASE_URL=postgresql+asyncpg://consistency:consistency@127.0.0.1:5433/consistency \
  uv run --frozen python scripts/benchmark.py \
  --markets 250 --message-target 2000 --paced-rate 0 --profile \
  --output /tmp/ce-bench-profile.json
```

cProfile counts each resume of an async function, so `ncalls` on `run` is not a logical
call count. Cumulative time still shows where the wall time went:

| Function | Cumulative seconds |
|---|---:|
| `PipelineListener.on_message` | 3.054 |
| `DetectionEngine._sample` | 2.603 |
| `pricing.evaluate` | 2.185 |
| `DatabaseSessionStore.open_session` | 1.948 |
| `sha256_of` | 1.049 |
| `to_jsonable` (tottime 0.506) | 0.768 |
| `kqueue.control` (waiting) | 1.311 |

No pricing, Decimal, or detection code was changed after this profile. Certificate
canonical JSON is inside `evaluate`. Altering it would risk the golden certificate
hashes. The other large piece is one-time catalog upsert at session open, plus time
spent waiting on PostgreSQL. Those were left as they are.

## Targets

| Goal | Result |
|---|---|
| At least 1000 synthetic book updates/s where attainable | Hit at 54 markets (1310.33) and 270 markets (1093.72). Missed at 1026 (658.20) and 5022 (313.48) |
| p95 detection latency under 100 ms on the focused workload | Hit. 270 markets, detection p95 0.4849 ms. Processing p95 1.1711 ms |
| No incorrect positive classifications on golden fixtures | Golden expectations were not edited. This phase did not change detection code, so the golden suite was not re-run here. The 270-market replay comparison was identical |
| No unbounded queue growth under backpressure | Hit. Inbound stopped at 10000. The outbox probe stopped at 4 |
| Deterministic replay still correct | Hit. Second pass matched the first |

## Remaining bottlenecks

- End-to-end rate falls as the catalog grows: 1310 msg/s at 54 markets, 313 msg/s at 5022, while per-call detection p95 stays near 0.5 ms. Session open writes every market, and each message carries a larger book manager.
- `evaluate` spends much of its time in canonical JSON and SHA-256 for certificates.
- At 5022 markets the process RSS was 866.4 MiB.
- A requested 500 msg/s pacing produced 378.40 msg/s once detection and database work shared the thread.
