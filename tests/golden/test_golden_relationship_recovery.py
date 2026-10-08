"""Golden fixtures G (boundary mismatch) and H (recovery). Data: ``fixtures/golden/{G,H}.yaml``."""

from __future__ import annotations

import pytest

from consistency_connectors.ingestion import BookManager
from consistency_core.events import OrderBookDeltaEvent, OrderBookSnapshotEvent, StreamMessage
from consistency_core.models import Side, SyncStatus
from consistency_core.models.relationship import EvidenceOutcome, VerificationStatus
from consistency_core.models.settlement import IntervalTerms, ThresholdTerms
from consistency_core.money import dec, dec_str
from consistency_core.pricing.certificate import EvaluationConfig
from consistency_core.pricing.evaluator import evaluate
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.numeric import raw_interval
from tests.factories import T0, T0_MS
from tests.golden.support import catalog, fee_calculator, find_relationship, load, portfolio

pytestmark = pytest.mark.golden


def test_G_boundary_mismatch_rejected() -> None:
    """G: exact raw-value preimages
        GE0.3, half-up to 0.1:  report >= 0.3  <=>  x >= 0.25  -> [1/4, +inf)
        GT0.27, half-up to 0.01: report > 0.27 <=> report >= 0.28 <=> x >= 0.275 -> [11/40, +inf)
    [1/4, inf) is not a subset of [11/40, inf): x = 0.25 reports 0.3 (YES) under the first and
    0.25 (NO) under the second, so "GE0.3 => GT0.27" is REJECTED with that witness. The converse
    holds on raw values, but the two use different methodologies/rounding, so it is only a
    CANDIDATE_REVIEW."""
    fx = load("G")
    cat = catalog(fx)
    for m in cat.markets:
        t = m.settlement.terms
        assert isinstance(t, ThresholdTerms | IntervalTerms)
        got = raw_interval(t, m.settlement.rounding).describe()
        assert got == fx["expected"]["false_implication"]["raw_preimages"][m.market_id]
    rels = discover(cat, as_of=T0)
    for key in ("false_implication", "converse"):
        exp = fx["expected"][key]
        r = find_relationship(rels, exp)
        assert r.verification_status is VerificationStatus(exp["status"])
        if "witness" in exp:
            fail = [e for e in r.evidence if e.outcome is EvidenceOutcome.FAIL]
            assert len(fail) == 1
            assert exp["witness"] in fail[0].detail


def _message(spec: dict[str, object], pos: int, t: int) -> StreamMessage:
    kind = spec["type"]
    base = {
        "market_id": spec["market"],
        "sid": spec["sid"],
        "seq": spec["seq"],
        "connection_id": "golden-h",
        "exchange_ts_ms": t,
    }
    if kind == "snapshot":
        event: OrderBookSnapshotEvent | OrderBookDeltaEvent = OrderBookSnapshotEvent(
            **base,  # type: ignore[arg-type]
            yes_bids=tuple((dec(p), dec(q)) for p, q in spec.get("yes_bids", [])),  # type: ignore[attr-defined]
            no_bids=tuple((dec(p), dec(q)) for p, q in spec.get("no_bids", [])),  # type: ignore[attr-defined]
        )
    else:
        event = OrderBookDeltaEvent(
            **base,  # type: ignore[arg-type]
            side=Side(spec["side"]),
            price=dec(spec["price"]),
            delta=dec(spec["delta"]),
        )
    return StreamMessage(position=pos, emitted_ts_ms=t, received_ts_ms=t + 5, event=event)


def test_H_recovery_no_verified_opportunity_while_unsynchronized() -> None:
    """H: after the lost seq 4, both books on sid 1 are UNSYNCHRONIZED; the seq-6 delta (+500)
    is ignored (YES bids on GE3 stay at 50); every evaluation is UNSYNCHRONIZED_DATA until both
    legs have fresh snapshots on sid 2, after which the implication is again a candidate with
    the new depth (40)."""
    fx = load("H")
    cat = catalog(fx)
    rel = find_relationship(discover(cat, as_of=T0), fx["relationship"])
    pf = portfolio(fx["portfolio"])
    mgr = BookManager(cat.markets, source="golden-h")
    fees = fee_calculator()
    t = T0_MS
    for i, step in enumerate(fx["steps"]):
        t += 100
        msg = _message(step["msg"], i, t)
        mgr.process(msg)
        for mid, status in step["expect_sync"].items():
            assert mgr.sync_status(mid) is SyncStatus(status), (i, mid)
        ev = evaluate(
            rel,
            pf,
            markets=cat.markets_by_id(),
            books={m: mgr.book(m) for m in rel.members},
            now_ms=msg.received_ts_ms,
            fees=fees,
            config=EvaluationConfig(),
            observed_duration_ms=5000,
        )
        assert ev.classification.value == step["expect_classification"], (i, ev.reason_codes)
        for mid, bids in step.get("expect_yes_bids", {}).items():
            got = [[dec_str(lv.price), dec_str(lv.quantity)] for lv in mgr.book(mid).yes_bids]
            assert [[dec(p), dec(q)] for p, q in got] == [[dec(p), dec(q)] for p, q in bids]
        if "expect_max_supported" in step:
            assert ev.certificate.capacity is not None
            assert ev.certificate.capacity.max_supported_quantity == dec(
                step["expect_max_supported"]
            )
    assert mgr.stats.gaps_detected == 1
    assert mgr.stats.ignored_unsynchronized == 1
