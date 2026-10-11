# Benchmarks

Synthetic data only. This run did not contact Kalshi. GitHub Actions runs a tiny
`--smoke` benchmark (exit status and JSON shape only, not a speed target). It does
not run the measurement below. Raw rows for this run are in
`docs/benchmark-results.json`. The previous single-process run is kept under
[Prior run](#prior-run-phase-13) and in `docs/benchmark-results-m3.json`. Those
figures are not mixed into the tables in this section.

## Hardware and command

| Item | Value |
|---|---|
| CPU | Apple M3 (`probe: sysctl`) |
| Memory | 17179869184 bytes (16 GiB) |
| OS | Darwin 26.6.2 (arm64) |
| Python | 3.12.11 |
| PostgreSQL | 16.15 (Debian 16.15-1.pgdg13+2), Docker, host port 5433 |
| Database | `consistency_bench` on that server |
| Log level | WARNING |

```
DATABASE_URL=postgresql+asyncpg://consistency:consistency@127.0.0.1:5433/consistency make benchmark
```

The command exited 0. The shell reported the process finished in 482.5 s.

## Database guard

Checked before any connection, `CREATE DATABASE`, or Alembic upgrade:

* An omitted port is treated as 5432.
* Port 5432 is refused unless `BENCHMARK_ALLOW_PORT_5432=1` (the default is off; `true` does not enable it).
* Any host other than `localhost`, `127.0.0.1`, or `::1` is refused unless `BENCHMARK_ALLOW_REMOTE=1`.
* A refused URL is not migrated and is not dropped.

The CI smoke sets `BENCHMARK_ALLOW_PORT_5432=1` because the Actions Postgres service publishes port 5432. This local run used port 5433 and did not set either override.

## How this run is measured

| Parameter | Value |
|---|---|
| Sweep seed | 606 |
| Inconsistency seed | 505 (the bundled inconsistent session: eight injected scenarios, 126000 ms) |
| Repetitions | 3, each in its own process |
| Observation window | 0.5 s, the same length for cold start and steady state |
| Message target | 4000 (paced workload capped at 2000) |
| Focused workload | 250 markets, used for replay |
| Paced rate | 500 messages/s, start-to-start |
| Per-workload budget | 180 s |
| Tick | 250 ms |

Market counts are `ceil(target / 27) * 27`, so the sweep rows are 54, 270, 1026, and 5022 markets.

Scope, kept separate on purpose:

* **Session open** is catalog upsert before the first listener entry. It is not part of either throughput number.
* **Cold start** is the first 0.5 s after that first entry. It includes ingestion, detection, the PostgreSQL journal, and the notification outbox. It is not included in the steady-state number.
* **Steady state** is every later complete 0.5 s window, same scope, excluding session open and the cold window. The headline `messages_per_s` is the median of those windows across all three repetitions. Variability is min, median, max, mean, and sample standard deviation of the same windows.
* **Full run** is listener entries divided by the whole `worker.run` wall time, including session open. That is the closest figure to the prior run's single rate, and it is not the steady-state figure.

Latency percentiles below are from the middle repetition (run order), not a pool of all three. Processing latency uses the listener's last 10000 `on_message` samples when the run is longer than that. Detection latency is `perf_counter` around `pricing.evaluate`, one sample per call.

A missed speed goal does not change the exit code. A mandatory workload that times out, is skipped, or errors does.

## Sweep results

Steady-state messages/s are the headline. Full-run messages/s include session open.

| Markets | Reps | Steady msg/s min / median / max | Steady mean (stdev) | Cold median msg/s | Full-run median msg/s | Session open median s | Detection p50 / p95 / p99 ms | RSS MiB min / median / max |
|---|---:|---|---|---:|---:|---:|---|---|
| 54 | 3 | 660 / 773 / 900 | 784.54 (77.99) | 846 | 732.46 | 0.4549 | 0.5752 / 0.8351 / 1.164 | 94.5 / 94.5 / 94.6 |
| 270 | 3 | 12 / 794 / 1094 | 751.87 (257.49) | 998 | 588.80 | 2.1366 | 0.5849 / 0.8383 / 1.0014 | 107.5 / 113.7 / 113.8 |
| 1026 | 3 | 0 / 666 / 1008 | 582.00 (307.62) | 998 | 286.15 | 9.3838 | 0.6758 / 1.1106 / 1.4523 | 175.0 / 180.4 / 199.0 |
| 5022 | 3 | 0 / 0 / 1034 | 309.76 (369.29) | 1618 | 152.79 | 38.1022 | 0.5646 / 0.8226 / 0.9403 | 757.1 / 780.7 / 866.2 |

Middle-repetition wall time was 5.3848 s, 7.2775 s, 18.0357 s, and 81.6591 s. The three full-run rates at 5022 markets were 154.99, 131.19, and 152.79 msg/s. Relationship discovery is inside session generation, before `worker.run`, and is not part of these rates.

At 5022 markets, 192 steady windows were observed. The median window completed no listener entries. The mean of those windows is 309.76 msg/s and the fastest window is 1034 msg/s. Session open alone was 32.23–52.46 s. The cold window, which starts only after that, was 1488–1692 msg/s and is not averaged into the steady median. Inbound high water on the middle repetition was 10000, the queue cap.

These books still produced evaluations and no detection lifecycle events (`detections` 0, outbox high water 0). The inconsistency workload below is the one that opens detections.

### Paced workload

270 markets, 2410 messages, configured start-to-start interval of 1/500 s, 3 isolated repetitions.

Steady-state median **470** msg/s (min 302, max 700, mean 489.60, stdev 88.33). Full-run median **359.62** msg/s. Cold median 580 msg/s. Session open median 1.6789 s. Inbound high water on the middle repetition was **104** (cap 10000). CPU on that repetition was 54.62% of one core. RSS across reps was 109.0–109.2 MiB.

### Inconsistency workload

One synthetic instance, seed 505, `inject_standard_scenarios`, 126000 ms of simulated time, 12276 messages, 27 markets. Three isolated repetitions. Scope for the throughput row is the same cold/steady split as the sweep. Certificate, persistence, and end-to-end figures are from the middle repetition.

Lifecycle counts committed to `notification_outbox` were **OPENED 12, UPDATED 120, EXPIRED 7, RESOLVED 5** on that repetition (144 outbox rows, 12 detection rows). The engine's in-memory counts matched. Monetary columns (`max_net_edge`, `max_deviation`, `max_capacity`) and certificate hashes matched the engine events. No float appeared in `record_json` or outbox payloads. Golden fixtures were not edited.

| Measurement | Scope | Result |
|---|---|---|
| Steady throughput | ingestion, detection, journal, outbox; cold window excluded | median 452 msg/s (min 0, max 908, mean 448.78, stdev 240.34, n=157 windows) |
| Cold throughput | first 0.5 s after the first listener entry | median 540 msg/s across reps |
| Full run | includes session open | median 446.53 msg/s |
| Certificate serialization | CPU time in `Evaluation.certificate_json` | n=420, p50 0.6041 ms, p95 1.0206 ms, p99 1.1826 ms |
| Detection persistence | `DetectionWriter.write` through commit, including the outbox insert in that transaction | n=144, p50 18.3789 ms, p95 791.8847 ms, p99 1672.2758 ms |
| Outbox writes | rows committed with detection events | 144 |
| End-to-end | book update received until that detection transaction commits | n=144, p50 2281.125 ms, p95 6614.0593 ms, p99 6811.6081 ms |

The end-to-end figure includes outbox queue wait, not only `evaluate`. Outbox high water on the middle repetition was 49 (cap 1024). Inbound high water was 10000.

### Reconnection recovery

One instance, 12 s of simulated time, one synthetic gap on the econ family, plus one dropped in-order delta so the book manager actually observes a skipped sequence. The recording's later snapshots resynchronize those markets. Four intervals are timed separately. There is no single "recovery ms".

Three episodes (one per repetition):

| Segment | What it includes | min / median / max (ms) |
|---|---|---|
| Gap detection | `BookManager.process` on the message that flags the sequence gap | 0.0343 / 0.0374 / 0.0399 |
| Recovery request | `source.request_recovery` for that gap | 0.0433 / 0.0490 / 0.0786 |
| Snapshot arrival | from the request returning until the first desynced market is SYNCHRONIZED | 266.9633 / 286.3013 / 331.4410 |
| Full resynchronization | from that first restoring snapshot until every market desynced by the gap is SYNCHRONIZED | 9.0515 / 9.2444 / 14.3359 |

`gaps_detected` was 1 on the middle repetition. Snapshot arrival is wall time spent processing the messages that sit between the request and the restoring snapshots in this unpaced recording. It is not the synthetic exchange's 250 ms `recovery_delay`.

### Replay

The 270-market journal (4285 entries) was applied twice with `replay_catalog` on the first repetition. The second pass took 4.4957 s, **953.13 entries/s**. `compare_outcomes` reported `identical: true`, 0 event mismatches, 0 classification mismatches.

### Backpressure

`Outbox` maxsize 4, a sink that sleeps 10 ms, 40 batches. High water **4**, writes **40**, bounded **true**, elapsed **0.4388 s**.

On the unpaced runs the inbound queue held the whole finite recording whenever that was under 10000. At 5022 markets and on the inconsistency workload the inbound high water was **10000**, the cap.

## Targets

Compared with this run, not with the prior run:

| Goal | Result |
|---|---|
| At least 1000 synthetic book updates/s where attainable | Not hit on the steady-state median (773, 794, 666, and 0 msg/s) or on the full-run median (732.46, 588.80, 286.15, 152.79). The cold window at 1026 and 5022 markets was about 998 and 1618 msg/s and is not the steady-state number |
| p95 detection latency under 100 ms on the focused workload | Hit. 270 markets, detection p95 0.8383 ms |
| No incorrect positive classifications on golden fixtures | Golden expectations were not edited. The 270-market replay comparison was identical. The inconsistency workload's committed money matched the engine |
| No unbounded queue growth under backpressure | Hit. Inbound stopped at 10000. The outbox probe stopped at 4 |
| Deterministic replay still correct | Hit. Second pass matched the first |

## Remaining bottlenecks

* Session open grows with the catalog: median 0.45 s at 54 markets, 2.14 s at 270, 9.38 s at 1026, and 38.10 s at 5022. That time is excluded from steady state and still dominates the 5022-market wall clock.
* At 5022 markets the steady-state median window is 0 msg/s while the mean is 309.76. Listener entries arrive in bursts separated by waits, so a 0.5 s window is often empty.
* On the inconsistency workload, certificate JSON is about 0.6 ms at p50, but committing the detection plus its outbox row is 18 ms at p50 and 792 ms at p95. End-to-end from the book update to that commit is about 2.3 s at p50 because the outbox writer falls behind.
* Full-run rates in this isolated-process remeasurement are lower than the prior single-process run (732 vs 1310 msg/s at 54 markets). This file does not treat that gap as a profiled cause.

## Prior run (Phase 13)

Preserved from the measurement recorded with `docs/benchmark-results-m3.json`. One process, no cold/steady split, no isolated repetitions. `messages_per_s` there is listener entries divided by worker wall time, including session open. Do not read those cells as the steady-state column above.

| Markets | Messages | Msg/s | Processing p50/p95/p99 (ms) | Detection p50/p95/p99 (ms) | DB rows/s | RSS MiB | CPU % | Inbound high water |
|---|---:|---:|---|---|---:|---:|---:|---:|
| 54 | 3973 | 1310.33 | 0.7560 / 1.1782 / 1.2947 | 0.3338 / 0.4797 / 0.5371 | 1328.80 | 91.6 | 87.27 | 3972 |
| 270 | 4285 | 1093.72 | 0.6167 / 1.1711 / 1.3354 | 0.3371 / 0.4849 / 0.5816 | 1163.40 | 110.5 | 79.80 | 4284 |
| 1026 | 5161 | 658.20 | 0.3632 / 1.1749 / 1.3181 | 0.3365 / 0.4873 / 0.5753 | 789.43 | 195.5 | 71.00 | 5160 |
| 5022 | 10713 | 313.48 | 0.7003 / 1.1837 / 1.3213 | 0.3335 / 0.4884 / 0.5782 | 460.61 | 866.4 | 71.69 | 10000 |

That run's paced workload achieved 378.40 msg/s. Its recovery figure was one snapshot-burst time (p50 7.9292 ms, max 11.2319 ms) and is not the four segments above. Replay of the 270-market journal was 1817.44 entries/s and identical. The outbox probe elapsed 0.4387 s and stayed at its cap of 4.

The cProfile table from that run (270 markets, 2410 messages, 5.137 s) was taken before any hot-path edit and was not repeated here:

| Function | Cumulative seconds |
|---|---:|
| `PipelineListener.on_message` | 3.054 |
| `DetectionEngine._sample` | 2.603 |
| `pricing.evaluate` | 2.185 |
| `DatabaseSessionStore.open_session` | 1.948 |
| `sha256_of` | 1.049 |
| `to_jsonable` (tottime 0.506) | 0.768 |
| `kqueue.control` (waiting) | 1.311 |

No pricing, Decimal, or detection code was changed for that profile or for this remeasurement. Golden expected values were not changed.
