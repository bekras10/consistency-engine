"""Complete families and a reviewed settlement path. Titles do not verify."""

from __future__ import annotations

from datetime import UTC, datetime

from consistency_connectors.kalshi.families import (
    apply_settlement_overlay,
    relationship_proposals,
    select_complete_families,
)
from consistency_connectors.kalshi.mapping import catalog_from_payloads
from consistency_connectors.kalshi.universe import diff_membership
from consistency_core.models.relationship import RelationshipType, VerificationStatus
from consistency_core.relationships.discovery import discover
from consistency_core.relationships.review import revalidate

NOW = datetime(2026, 10, 11, 18, 0, tzinfo=UTC)
LATER = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)


def _market(ticker: str, event: str, strike: str) -> dict[str, object]:
    return {
        "ticker": ticker,
        "event_ticker": event,
        "market_type": "binary",
        "status": "active",
        "notional_value_dollars": "1.0000",
        "settlement_bounds_type": "default",
        "open_time": "2026-10-01T00:00:00Z",
        "close_time": "2026-10-31T00:00:00Z",
        "expiration_time": "2026-10-31T00:00:00Z",
        "rules_primary": f"Resolves using the reviewed measurement above {strike}.",
        "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
        "strike_type": "greater",
        "floor_strike": strike,
        "yes_sub_title": f"Above {strike}",
        "title": f"Reading above {strike}",
    }


def _event(event_id: str) -> dict[str, object]:
    return {
        "event_ticker": event_id,
        "series_ticker": "KXTEST",
        "title": "Reading",
        "mutually_exclusive": False,
        "settlement_sources": [{"name": "Agency bulletin"}],
    }


def _series() -> dict[str, object]:
    return {"ticker": "KXTEST", "title": "Reading", "category": "climate"}


def _catalog(markets: list[dict[str, object]], events: list[dict[str, object]]):
    return catalog_from_payloads(
        markets, events, [_series()], retrieved_at=NOW, environment="production", limit=20
    )


def test_selection_keeps_whole_events_instead_of_the_first_n() -> None:
    markets = [
        _market("A-1", "EVT-A", "10"),
        _market("A-2", "EVT-A", "20"),
        _market("A-3", "EVT-A", "30"),
        _market("B-1", "EVT-B", "1"),
        _market("B-2", "EVT-B", "2"),
    ]
    catalog, skipped = catalog_from_payloads(
        markets,
        [_event("EVT-A"), _event("EVT-B")],
        [_series()],
        retrieved_at=NOW,
        environment="production",
        limit=2,
    )
    # Budget 2 cannot take EVT-A (3 markets). EVT-B fits as a complete family.
    assert [market.market_id for market in catalog.markets] == ["B-1", "B-2"]
    assert any("EVT-A" in item for item in skipped)
    assert all(market.event_id == "EVT-B" for market in catalog.markets)


def test_direct_family_budget_skips_a_partial_event() -> None:
    catalog, _skipped = _catalog(
        [
            _market("A-1", "EVT-A", "10"),
            _market("A-2", "EVT-A", "20"),
            _market("B-1", "EVT-B", "1"),
        ],
        [_event("EVT-A"), _event("EVT-B")],
    )
    chosen, notes = select_complete_families(catalog.markets, limit=2)
    assert [market.market_id for market in chosen] == ["A-1", "A-2"]
    assert any(note.startswith("EVT-B") for note in notes)
    larger, large_notes = select_complete_families(
        _catalog(
            [
                _market("A-1", "EVT-A", "10"),
                _market("A-2", "EVT-A", "20"),
                _market("A-3", "EVT-A", "30"),
                _market("B-1", "EVT-B", "1"),
                _market("B-2", "EVT-B", "2"),
            ],
            [_event("EVT-A"), _event("EVT-B")],
        )[0].markets,
        limit=4,
    )
    assert {market.event_id for market in larger} == {"EVT-A"}
    assert any(note.startswith("EVT-B") for note in large_notes)


def test_titles_stay_unverified_until_reviewed_rules_and_a_rule_change_invalidates() -> None:
    raw = [
        _market("KX-30", "EVT-A", "30"),
        _market("KX-40", "EVT-A", "40"),
        _market("KX-50", "EVT-A", "50"),
    ]
    catalog, _skipped = _catalog(raw, [_event("EVT-A")])
    proposals = relationship_proposals(catalog, as_of=NOW)
    assert all(item["grants_verified"] is False for item in proposals)
    assert all(item["verification"] != "verified" for item in proposals)
    before = discover(catalog, as_of=NOW)
    assert not any(item.is_verified for item in before)

    overlay = {
        market_id: {
            "underlying_id": "station-1",
            "measurement": "daily high",
            "location": "station-1",
            "observation_window": "2026-10-31 local day",
            "settlement_source": "Agency bulletin",
            "methodology": "agency daily high",
            "unit": "celsius",
            "early_close_policy": "no_early_close",
            "exceptional_resolution": [],
            "rounding": {"mode": "none"},
            "title": "this title must not be treated as a rule",
        }
        for market_id in ("KX-30", "KX-40", "KX-50")
    }
    reviewed = apply_settlement_overlay(catalog, overlay)
    verified = [item for item in discover(reviewed, as_of=NOW) if item.is_verified]
    assert len(verified) == 1
    assert verified[0].relationship_type is RelationshipType.NESTED_THRESHOLDS
    assert verified[0].verification_status is VerificationStatus.VERIFIED
    assert tuple(verified[0].members) == ("KX-50", "KX-40", "KX-30")
    bundle = relationship_proposals(reviewed, as_of=NOW)
    nested = next(item for item in bundle if item["relationship_type"] == "nested_thresholds")
    members = nested["members"]
    assert isinstance(members, list)
    assert {row["rules_hash"] for row in members} == set(verified[0].rules_hashes.values())

    changed_markets = []
    for market in reviewed.markets:
        if market.market_id == "KX-50":
            changed_markets.append(
                market.model_copy(update={"settlement_rules": market.settlement_rules + " amended"})
            )
        else:
            changed_markets.append(market)
    amended = reviewed.model_copy(update={"markets": tuple(changed_markets)})
    demoted = revalidate(verified, amended, as_of=LATER)
    assert len(demoted) == 1
    assert demoted[0].is_verified is False
    assert demoted[0].verification_status is VerificationStatus.CANDIDATE_REVIEW
    assert "rules changed" in (demoted[0].invalidation_reason or "")


def test_membership_rotation_records_enter_and_exit() -> None:
    changes = diff_membership(
        {"M-A": "hash-a", "M-B": "hash-b"},
        {"M-B": "hash-b2", "M-C": "hash-c"},
        subject_kind="market",
        at_ms=5000,
    )
    actions = {(item.subject_id, item.action, item.fingerprint) for item in changes}
    assert ("M-A", "exit", "hash-a") in actions
    assert ("M-B", "exit", "hash-b") in actions
    assert ("M-B", "enter", "hash-b2") in actions
    assert ("M-C", "enter", "hash-c") in actions
    assert ("M-A", "enter", "hash-a") not in actions
