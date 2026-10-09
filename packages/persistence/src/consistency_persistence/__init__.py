"""PostgreSQL persistence (spec 11) shared by the worker and, later, the API."""

from consistency_persistence.schema import SPEC_TABLES

__all__ = ["SPEC_TABLES"]
