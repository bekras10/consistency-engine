"""Deterministic relationship discovery (spec 8.1).

Output depends only on catalog *content*: markets are processed in sorted order and results are
sorted by ``relationship_id``, so shuffling the catalog cannot change anything.

Rules
-----
Categorical (``OutcomeTerms``), per event:
    ``event.mutually_exclusive`` -> MUTUALLY_EXCLUSIVE; additionally EXHAUSTIVE_PARTITION when
    ``outcome_set_complete`` is true *and* the listed outcome ids equal the declared universe.
Numeric (``ThresholdTerms`` / ``IntervalTerms``), per ``underlying_id``:
    every contract is mapped to its exact raw-value preimage (rounding included). For each
    ordered pair (x, y):

    * raw(x) == raw(y)                         -> EQUIVALENT
    * raw(x) strict-subset raw(y)              -> IMPLICATION x => y
    * reported(x) subset reported(y) but raw(x) not subset raw(y)
                                               -> REJECTED IMPLICATION with a counterexample
                                                  (the "naive reading" trap; Test G)

    Same-convention up-rays (or down-rays) with >= 3 distinct members are aggregated into one
    NESTED_THRESHOLDS chain (narrowest first). Interval markets of one event with pairwise
    disjoint preimages form DISJOINT_INTERVALS; exhaustive only if :func:`covers` proves the
    union contains the declared value domain.
Propositions (``PropositionTerms``): same underlying and same proposition id -> EQUIVALENT
    candidate, never auto-verified (needs manual review). Different propositions are only
    related through manual review.
Title similarity: identical normalized title token sets where at least one side lacks an
    ``underlying_id`` -> CANDIDATE_REVIEW at most.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Generator, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from fractions import Fraction

from consistency_core.models.market import Catalog, Event, Market
from consistency_core.models.relationship import EvidenceCheck, Relationship, RelationshipType
from consistency_core.models.settlement import (
    IntervalTerms,
    OutcomeTerms,
    PropositionTerms,
    ThresholdTerms,
)
from consistency_core.relationships.numeric import (
    Interval,
    apply_rounding,
    covers,
    frac,
    raw_interval,
    reported_interval,
)
from consistency_core.relationships.verification import (
    FAIL,
    IDENTITY_FIELDS,
    NUMERIC_IDENTITY_FIELDS,
    PASS,
    UNKNOWN,
    build_relationship,
    check,
    convention_checks,
    exceptional_check,
    field_checks,
    status_from,
)

DISCOVERED_BY = "deterministic-discovery/1"


@dataclass(frozen=True)
class DiscoveryConfig:
    discovered_by: str = DISCOVERED_BY
    min_chain_length: int = 3


def discover(
    catalog: Catalog, *, as_of: datetime, config: DiscoveryConfig | None = None
) -> list[Relationship]:
    cfg = config or DiscoveryConfig()
    markets = sorted(catalog.markets, key=lambda m: m.market_id)
    events = {e.event_id: e for e in catalog.events}
    out: dict[str, Relationship] = {}

    def add(rel: Relationship) -> None:
        out.setdefault(rel.relationship_id, rel)

    for rel in _categorical(markets, events, as_of, cfg):
        add(rel)
    for rel in _numeric(markets, as_of, cfg):
        add(rel)
    for rel in _propositions(markets, as_of, cfg):
        add(rel)
    for rel in _title_similarity(markets, as_of, cfg):
        add(rel)
    return [out[k] for k in sorted(out)]


# ------------------------------------------------------------------------------ categorical
def _categorical(
    markets: Sequence[Market], events: dict[str, Event], as_of: datetime, cfg: DiscoveryConfig
) -> Iterable[Relationship]:
    by_event: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        if isinstance(m.settlement.terms, OutcomeTerms):
            by_event[m.event_id].append(m)
    for event_id in sorted(by_event):
        group = by_event[event_id]
        if len(group) < 2:
            continue
        ev = events.get(event_id)
        evidence = field_checks(group, IDENTITY_FIELDS)
        evidence.append(exceptional_check(group))
        outcome_ids = [_outcome(m) for m in group]
        if len(set(outcome_ids)) != len(outcome_ids):
            evidence.append(check("distinct_outcomes", FAIL, "two markets share an outcome id"))
        else:
            evidence.append(check("distinct_outcomes", PASS, f"outcomes {outcome_ids}"))
        me = None if ev is None else ev.mutually_exclusive
        if me is False:
            continue
        evidence.append(
            check("event_mutually_exclusive", PASS, f"event {event_id} declares exclusivity")
            if me
            else check("event_mutually_exclusive", UNKNOWN, f"event {event_id} unknown")
        )
        exhaustive = (
            ev is not None
            and ev.outcome_set_complete is True
            and ev.outcome_universe is not None
            and set(outcome_ids) == set(ev.outcome_universe)
        )
        if exhaustive:
            assert ev is not None
            evidence.append(
                check(
                    "outcome_set_complete",
                    PASS,
                    f"listed outcomes equal declared universe {sorted(ev.outcome_universe or ())}",
                )
            )
            rtype = RelationshipType.EXHAUSTIVE_PARTITION
            why = "categorical event with a complete listed outcome set: exactly one YES"
        else:
            rtype = RelationshipType.MUTUALLY_EXCLUSIVE
            completeness = None if ev is None else ev.outcome_set_complete
            why = "categorical event, at most one YES; exhaustiveness " + (
                "refuted (unlisted outcomes)" if completeness is False else "not proven"
            )
        yield build_relationship(
            rtype,
            group,
            evidence=evidence,
            reasoning=why,
            as_of=as_of,
            discovered_by=cfg.discovered_by,
            exhaustive=exhaustive,
        )


def _outcome(m: Market) -> str:
    assert isinstance(m.settlement.terms, OutcomeTerms)
    return m.settlement.terms.outcome_id


# ---------------------------------------------------------------------------------- numeric
@dataclass(frozen=True)
class _Num:
    market: Market
    raw: Interval
    reported: Interval

    @property
    def mid(self) -> str:
        return self.market.market_id

    @property
    def convention(self) -> tuple[str, ...]:
        s = self.market.settlement
        return (
            repr(s.rounding),
            repr(s.methodology),
            *(repr(getattr(s, f)) for f in NUMERIC_IDENTITY_FIELDS),
        )


def _numeric(
    markets: Sequence[Market], as_of: datetime, cfg: DiscoveryConfig
) -> Generator[Relationship, None, None]:
    groups: dict[str, list[_Num]] = defaultdict(list)
    for m in markets:
        t = m.settlement.terms
        if isinstance(t, ThresholdTerms | IntervalTerms) and m.settlement.underlying_id:
            groups[m.settlement.underlying_id].append(
                _Num(m, raw_interval(t, m.settlement.rounding), reported_interval(t))
            )
    for underlying in sorted(groups):
        nums = groups[underlying]
        chained = yield from _chains(nums, as_of, cfg)
        yield from _pairs(nums, chained, as_of, cfg)
        yield from _disjoint(nums, as_of, cfg)


def _equal(a: Interval, b: Interval) -> bool:
    return a.is_subset_of(b) and b.is_subset_of(a)


def _pair_evidence(x: _Num, y: _Num) -> list[EvidenceCheck]:
    pair = [x.market, y.market]
    ev = field_checks(pair, NUMERIC_IDENTITY_FIELDS)
    ev.append(exceptional_check(pair))
    ev.extend(convention_checks(pair))
    return ev


def _witness_detail(x: _Num, y: _Num, w: Fraction) -> str:
    rx = apply_rounding(w, x.market.settlement.rounding)
    ry = apply_rounding(w, y.market.settlement.rounding)
    return (
        f"counterexample raw x={_fmt(w)}: {x.mid} reports {_fmt(rx)} -> YES, "
        f"{y.mid} reports {_fmt(ry)} -> NO"
    )


def _fmt(f: Fraction) -> str:
    if f.denominator == 1:
        return str(f.numerator)
    d = Decimal(f.numerator) / Decimal(f.denominator)
    return format(d.normalize(), "f") if Fraction(d) == f else str(f)


def _pairs(
    nums: Sequence[_Num],
    chained: set[frozenset[str]],
    as_of: datetime,
    cfg: DiscoveryConfig,
) -> Iterable[Relationship]:
    for i, a in enumerate(nums):
        for b in nums[i + 1 :]:
            base = _pair_evidence(a, b)
            if _equal(a.raw, b.raw):
                ev = [*base, check("raw_preimage_equal", PASS, f"both {a.raw.describe()}")]
                yield build_relationship(
                    RelationshipType.EQUIVALENT,
                    sorted([a.market, b.market], key=lambda m: m.market_id),
                    evidence=ev,
                    reasoning="identical exact YES sets on the raw observation",
                    as_of=as_of,
                    discovered_by=cfg.discovered_by,
                )
                continue
            for x, y in ((a, b), (b, a)):
                exact = x.raw.is_subset_of(y.raw)
                naive = x.reported.is_subset_of(y.reported)
                if exact:
                    if frozenset((x.mid, y.mid)) in chained:
                        continue
                    ev = [
                        *base,
                        check(
                            "raw_preimage_subset",
                            PASS,
                            f"{x.raw.describe()} subset of {y.raw.describe()}",
                        ),
                    ]
                    yield build_relationship(
                        RelationshipType.IMPLICATION,
                        [x.market, y.market],
                        evidence=ev,
                        reasoning=f"every raw value settling {x.mid} YES settles {y.mid} YES",
                        as_of=as_of,
                        discovered_by=cfg.discovered_by,
                    )
                elif naive:
                    w = x.raw.witness_outside(y.raw)
                    assert w is not None
                    ev = [*base, check("raw_preimage_subset", FAIL, _witness_detail(x, y, w))]
                    yield build_relationship(
                        RelationshipType.IMPLICATION,
                        [x.market, y.market],
                        evidence=ev,
                        reasoning=(
                            "naive reading of the thresholds suggests an implication, but the "
                            "settlement rounding conventions break it"
                        ),
                        as_of=as_of,
                        discovered_by=cfg.discovered_by,
                    )


def _chains(
    nums: Sequence[_Num], as_of: datetime, cfg: DiscoveryConfig
) -> Generator[Relationship, None, set[frozenset[str]]]:
    """Yields NESTED_THRESHOLDS relationships; returns the member pairs they cover."""
    covered: set[frozenset[str]] = set()
    by_conv: dict[tuple[str, ...], list[_Num]] = defaultdict(list)
    for n in nums:
        by_conv[n.convention].append(n)
    for conv in sorted(by_conv):
        group = by_conv[conv]
        for direction in ("up", "down"):
            rays = [
                n
                for n in group
                if (n.raw.upper is None and n.raw.lower is not None and direction == "up")
                or (n.raw.lower is None and n.raw.upper is not None and direction == "down")
            ]
            # one representative per distinct preimage (smallest id); equal ones stay pairwise
            reps: list[_Num] = []
            for n in sorted(rays, key=lambda n: n.mid):
                if not any(_equal(n.raw, r.raw) for r in reps):
                    reps.append(n)
            if len(reps) < cfg.min_chain_length:
                continue
            # narrowest first: up-rays by descending lower bound, down-rays by ascending upper
            if direction == "up":
                reps.sort(key=lambda n: (-_bound(n.raw.lower), n.raw.lower_closed, n.mid))
            else:
                reps.sort(key=lambda n: (_bound(n.raw.upper), n.raw.upper_closed, n.mid))
            ordered = [n.market for n in reps]
            evidence = field_checks(ordered, NUMERIC_IDENTITY_FIELDS)
            evidence.append(exceptional_check(ordered))
            evidence.extend(convention_checks(ordered))
            evidence.append(
                check(
                    "raw_preimage_chain",
                    PASS,
                    " subset ".join(n.raw.describe() for n in reps),
                )
            )
            rel = build_relationship(
                RelationshipType.NESTED_THRESHOLDS,
                ordered,
                evidence=evidence,
                reasoning="same observation and convention; thresholds strictly nested",
                as_of=as_of,
                discovered_by=cfg.discovered_by,
            )
            yield rel
            ids = [n.mid for n in reps]
            covered.update(frozenset((a, b)) for i, a in enumerate(ids) for b in ids[i + 1 :])
    return covered


def _bound(v: Fraction | None) -> Fraction:
    assert v is not None
    return v


def _disjoint(
    nums: Sequence[_Num], as_of: datetime, cfg: DiscoveryConfig
) -> Iterable[Relationship]:
    by_event: dict[str, list[_Num]] = defaultdict(list)
    for n in nums:
        if isinstance(n.market.settlement.terms, IntervalTerms):
            by_event[n.market.event_id].append(n)
    for event_id in sorted(by_event):
        group = by_event[event_id]
        if len(group) < 2 or len({n.convention for n in group}) != 1:
            continue
        if not all(a.raw.disjoint_from(b.raw) for i, a in enumerate(group) for b in group[i + 1 :]):
            continue
        group = sorted(
            group,
            key=lambda n: (n.raw.lower is not None, n.raw.lower or Fraction(0), n.mid),
        )
        ms = [n.market for n in group]
        evidence = field_checks(ms, NUMERIC_IDENTITY_FIELDS)
        evidence.append(exceptional_check(ms))
        evidence.append(
            check(
                "pairwise_disjoint",
                PASS,
                ", ".join(f"{n.mid}={n.raw.describe()}" for n in group),
            )
        )
        domains = {(m.settlement.value_domain_lower, m.settlement.value_domain_upper) for m in ms}
        exhaustive = False
        if len(domains) == 1:
            (lo, hi), *_ = domains
            dom = Interval(
                None if lo is None else frac(lo),
                lo is not None,
                None if hi is None else frac(hi),
                hi is not None,
            )
            ok, gap = covers([n.raw for n in group], dom)
            exhaustive = ok
            note = (
                f"union covers value domain {dom.describe()}"
                if ok
                else f"not exhaustive: raw value {gap} is in no interval"
            )
        else:
            note = "members disagree on the value domain; exhaustiveness not evaluated"
        why = "pairwise disjoint YES sets on one observation; " + note
        yield build_relationship(
            RelationshipType.DISJOINT_INTERVALS,
            ms,
            evidence=evidence,
            reasoning=why,
            as_of=as_of,
            discovered_by=cfg.discovered_by,
            exhaustive=exhaustive,
        )


# ----------------------------------------------------------------------------- propositions
def _propositions(
    markets: Sequence[Market], as_of: datetime, cfg: DiscoveryConfig
) -> Iterable[Relationship]:
    by_prop: dict[tuple[str, str], list[Market]] = defaultdict(list)
    for m in markets:
        t = m.settlement.terms
        if isinstance(t, PropositionTerms) and m.settlement.underlying_id:
            by_prop[(m.settlement.underlying_id, t.proposition_id)].append(m)
    for key in sorted(by_prop):
        group = by_prop[key]
        for i, a in enumerate(group):
            for b in group[i + 1 :]:
                pair = [a, b]
                ev = field_checks(pair, IDENTITY_FIELDS)
                ev.append(exceptional_check(pair))
                ev.append(
                    check(
                        "proposition_semantics",
                        UNKNOWN,
                        f"same proposition id {key[1]!r}; free-text propositions require "
                        "manual review",
                    )
                )
                yield build_relationship(
                    RelationshipType.EQUIVALENT,
                    pair,
                    evidence=ev,
                    reasoning="shared proposition identifier",
                    as_of=as_of,
                    discovered_by=cfg.discovered_by,
                )


# ------------------------------------------------------------------------- title similarity
_TOKEN = re.compile(r"[a-z0-9]+(?:\.[0-9]+)?")


def title_tokens(title: str) -> frozenset[str]:
    return frozenset(_TOKEN.findall(title.lower()))


def _title_similarity(
    markets: Sequence[Market], as_of: datetime, cfg: DiscoveryConfig
) -> Iterable[Relationship]:
    buckets: dict[frozenset[str], list[Market]] = defaultdict(list)
    for m in markets:
        buckets[title_tokens(m.title)].append(m)
    for key in sorted(buckets, key=lambda k: sorted(k)):
        group = buckets[key]
        for i, a in enumerate(group):
            for b in group[i + 1 :]:
                if a.settlement.underlying_id and b.settlement.underlying_id:
                    continue  # both identified: handled (or excluded) by structured rules
                pair = [a, b]
                ev = field_checks(pair, IDENTITY_FIELDS)
                ev.append(
                    check("title_similarity", UNKNOWN, "identical title tokens only (heuristic)")
                )
                status = status_from(ev)
                yield build_relationship(
                    RelationshipType.EQUIVALENT,
                    pair,
                    evidence=ev,
                    reasoning="title match only; never sufficient for verification",
                    as_of=as_of,
                    discovered_by=cfg.discovered_by,
                    status=status,
                )
