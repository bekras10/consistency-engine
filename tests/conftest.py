from __future__ import annotations

import os
from pathlib import Path

from hypothesis import HealthCheck, settings

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "fixtures"

# Derandomized so property-test runs are reproducible in CI and locally.
settings.register_profile(
    "default",
    max_examples=200,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.register_profile("thorough", max_examples=2000, derandomize=False, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))
