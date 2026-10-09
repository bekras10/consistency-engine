"""Canonical, deterministic JSON for audit artefacts.

Decimals become fixed-point strings (never JSON numbers), keys are sorted, separators are
compact, so identical inputs produce byte-identical output and identical hashes.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel

from consistency_core.money import dec_str


def to_jsonable(value: Any) -> Any:
    t = type(value)
    # Exact-type fast paths (subclasses such as StrEnum / IntEnum take the general path below).
    if t is str or t is int or t is bool or value is None:
        return value
    if t is dict:
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if t is list or t is tuple:
        return [to_jsonable(v) for v in value]
    if isinstance(value, BaseModel):
        return to_jsonable(value.model_dump(mode="json"))
    if isinstance(value, Decimal):
        return dec_str(value)
    if isinstance(value, float):
        raise TypeError("binary float found in an audit artefact")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [to_jsonable(v) for v in value]
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(to_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_of(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode()).hexdigest()
