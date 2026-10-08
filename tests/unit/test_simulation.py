"""Phase 3: synthetic exchange determinism, coherence, faults, datasets, playback."""

from __future__ import annotations

import asyncio
from collections import Counter
from decimal import Decimal
from fractions import Fraction

import pytest

from consistency_core.models.common import Side
from consistency_core.money import ONE
from consistency_simulation.datasets import BUNDLED, PRESETS, build, load, read_text, render
from consistency_simulation.exchange import SessionConfig, SyntheticExchange
from consistency_simulation.stream import playback, validate_speed
from tests.conftest import FIXTURES

SHORT = SessionConfig(name="unit", seed=7, duration_ms=15_000, description="unit")


def best_ask(ex: SyntheticExchange, market_id: str, side: Side) -> Decimal | None:
    for rt in ex.runtimes:
        if market_id in rt.books:
            opp = rt.books[market_id].levels(side.opposite)
            return ONE - opp[0][0] if opp else None
    raise KeyError(market_id)


def test_same_seed_identical_output() -> None:
    assert render(build(SHORT)) == render(build(SHORT))


def test_different_seed_differs() -> None:
    other = SessionConfig(name="unit", seed=8, duration_ms=15_000, description="unit")
    assert render(build(SHORT))["messages.jsonl"] != render(build(other))["messages.jsonl"]


@pytest.mark.parametrize("name", BUNDLED)
def test_bundled_fixtures_reproducible(name: str) -> None:
    directory = FIXTURES / "datasets" / name
    for fname, text in render(build(name)).items():
        assert read_text(directory, fname) == text, f"{name}/{fname} drifted; run `make seed`"


def test_bundled_dataset_metadata() -> None:
    for name in BUNDLED:
        ds = load(FIXTURES / "datasets" / name)
        md = ds.metadata
        assert md["synthetic"] is True
        assert md["generator_version"].startswith("synthetic-exchange/")
        assert md["seed"] == PRESETS[name].seed
        assert md["expected_relationships"]
        assert len(ds.messages) == md["counts"]["messages"]
    inc = load(FIXTURES / "datasets" / "inconsistent").metadata
    kinds = {f["kind"] for f in inc["expected_findings"]}
    assert len(kinds) == 8  # every spec-3.3 scenario present
    assert all(f["expected_classification"] for f in inc["expected_findings"])


def test_fair_probabilities_coherent_every_tick() -> None:
    """The baseline is built from valid distributions: exact identities hold at every tick."""

    def check(_t: int, ex: SyntheticExchange) -> None:
        fams = {rt.key: rt.family for rt in ex.runtimes}
        e = fams["election"]
        assert sum(m.fair() for m in e.markets[:5]) == 1
        assert sum(m.fair() for m in e.markets[5:]) < 1
        w = fams["weather"]
        assert sum(m.fair() for m in w.markets) == 1
        s = fams["sports"]
        assert sum(m.fair() for m in s.markets[2:5]) == 1
        assert sum(m.fair() for m in s.markets[:2]) < 1  # draw unlisted
        c = [m.fair() for m in fams["econ"].markets[:4]]  # GE0.4, GE0.3, GE0.2, GE0.1
        assert c == sorted(c)
        champ, finals = (m.fair() for m in fams["implication"].markets)
        assert champ <= finals
        q = fams["equivalent"].markets
        assert q[0].fair() == q[1].fair()
        for rt in ex.runtimes:
            for m in rt.family.markets:
                assert Fraction(0) < m.fair() < Fraction(1)

    SyntheticExchange(SHORT).run(observer=check)


def test_books_uncrossed_and_baseline_has_no_book_arbitrage() -> None:
    """Without injections, no verified canonical portfolio is cheaper than its guarantee."""

    def check(_t: int, ex: SyntheticExchange) -> None:
        for rt in ex.runtimes:
            for mid, b in rt.books.items():
                if b.yes and b.no:
                    assert max(b.yes) + max(b.no) < ONE, mid
        fams = {rt.key: rt.family for rt in ex.runtimes}

        def ask(mid: str, side: Side) -> Decimal:
            a = best_ask(ex, mid, side)
            return a if a is not None else Decimal(2)  # no liquidity -> cannot buy

        pres = fams["election"].market_ids()[:5]
        assert sum(ask(m, Side.YES) for m in pres) >= 1
        bins = fams["weather"].market_ids()
        assert sum(ask(m, Side.YES) for m in bins) >= 1
        chain = fams["econ"].market_ids()[:4]
        for i in range(4):
            for j in range(i + 1, 4):
                assert ask(chain[i], Side.NO) + ask(chain[j], Side.YES) >= 1
        champ, finals = fams["implication"].market_ids()
        assert ask(champ, Side.NO) + ask(finals, Side.YES) >= 1
        a, b = fams["equivalent"].market_ids()[:2]
        assert ask(a, Side.YES) + ask(b, Side.NO) >= 1
        assert ask(a, Side.NO) + ask(b, Side.YES) >= 1
        h, aw = fams["sports"].market_ids()[:2]
        assert ask(h, Side.NO) + ask(aw, Side.NO) >= 1

    SyntheticExchange(SHORT).run(observer=check)


def test_smoke_contains_duplicate_and_gap_with_recovery() -> None:
    ds = load(FIXTURES / "datasets" / "smoke")
    keyed = Counter(
        (m.event.sid, m.event.seq)
        for m in ds.messages
        if m.event.type in ("orderbook_delta", "orderbook_snapshot")
    )
    assert sum(1 for c in keyed.values() if c > 1) == 1  # exactly one duplicated message
    tags = Counter(m.synthetic_tag for m in ds.messages if m.synthetic_tag)
    assert tags["after_gap"] == 1
    assert tags["recovery_snapshot"] >= 1
    seqs: dict[str, list[int]] = {}
    for m in ds.messages:
        if m.event.type in ("orderbook_delta", "orderbook_snapshot"):
            seqs.setdefault(m.event.sid, []).append(m.event.seq)
    gaps = [sid for sid, s in seqs.items() if sorted(set(s)) != list(range(1, max(s) + 1))]
    assert len(gaps) == 1  # exactly one subscription with a missing sequence number


def test_corruption_preset_has_every_fault_kind() -> None:
    res = build(SessionConfig(**{**PRESETS["corruption"].__dict__, "duration_ms": 170_000}))
    for k in (
        "fault_gap",
        "fault_duplicate",
        "fault_reorder",
        "fault_malformed",
        "fault_disconnect",
        "recoveries",
    ):
        assert res.counts.get(k, 0) >= 1, k
    assert any(m.event.type == "raw_wire" for m in res.messages)


def test_normal_preset_lifecycle_and_status() -> None:
    cfg = SessionConfig(**{**PRESETS["normal"].__dict__, "duration_ms": 510_000})
    res = build(cfg)
    assert res.counts["markets_created"] == 1
    assert res.counts["markets_removed"] == 1
    assert res.counts["status_changes"] == 2


def test_received_timestamps_monotone_in_delivery_order() -> None:
    ds = load(FIXTURES / "datasets" / "smoke")
    rts = [m.received_ts_ms for m in ds.messages]
    assert rts == sorted(rts)
    assert [m.position for m in ds.messages] == list(range(len(ds.messages)))


class TestPlayback:
    def _msgs(self):  # type: ignore[no-untyped-def]
        return load(FIXTURES / "datasets" / "smoke").messages[:200]

    @pytest.mark.parametrize("speed", [0.5, 1.0, 2.0, 5.0, 10.0])
    def test_speeds_scale_waits_not_content(self, speed: float) -> None:
        msgs = self._msgs()
        waits: list[float] = []

        async def fake_sleep(s: float) -> None:
            waits.append(s)

        async def collect() -> list[int]:
            return [m.position async for m in playback(msgs, speed=speed, sleep=fake_sleep)]

        got = asyncio.run(collect())
        assert got == [m.position for m in msgs]
        span_ms = msgs[-1].received_ts_ms - msgs[0].received_ts_ms
        assert sum(waits) == pytest.approx(span_ms / 1000 / speed)

    def test_unpaced(self) -> None:
        async def collect() -> int:
            return len([m async for m in playback(self._msgs(), speed=None)])

        assert asyncio.run(collect()) == 200

    def test_invalid_speed(self) -> None:
        with pytest.raises(ValueError, match="playback speed"):
            validate_speed(3.0)
