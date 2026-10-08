# Consistency Engine — developer entry points.
# Targets that belong to later milestones fail loudly (exit 2) instead of faking success.

UV ?= uv
PY := $(UV) run --frozen python
PYTEST := $(UV) run --frozen pytest

.PHONY: help setup dev test test-unit test-property test-golden lint format typecheck build \
        seed datasets replay benchmark api clean

help:
	@echo "setup      install Python workspace (uv sync)"
	@echo "test       ruff-independent full Python test suite"
	@echo "test-unit / test-property / test-golden   subsets"
	@echo "lint       ruff check + ruff format --check"
	@echo "format     ruff format + ruff check --fix"
	@echo "typecheck  mypy (strict)"
	@echo "datasets   regenerate all synthetic datasets"
	@echo "seed       regenerate bundled fixtures (smoke + inconsistent)"
	@echo "api        run the FastAPI app (health endpoints only in milestone 1)"
	@echo "dev / build / replay / benchmark: later milestones"

setup:
	$(UV) sync
	@# macOS can mark editable-install .pth files hidden; Python >= 3.12.10 then ignores them.
	@-chflags nohidden .venv/lib/python3.12/site-packages/*.pth 2>/dev/null || true

test:
	$(PYTEST)

test-unit:
	$(PYTEST) tests/unit

test-property:
	$(PYTEST) tests/property

test-golden:
	$(PYTEST) tests/golden -m golden

lint:
	$(UV) run --frozen ruff check .
	$(UV) run --frozen ruff format --check .

format:
	$(UV) run --frozen ruff format .
	$(UV) run --frozen ruff check --fix .

typecheck:
	$(UV) run --frozen mypy

seed:
	$(PY) scripts/generate_datasets.py --bundled

datasets:
	$(PY) scripts/generate_datasets.py --all

api:
	$(UV) run --frozen uvicorn consistency_api.main:app --reload --port 8000

dev:
	@echo "make dev: not yet implemented — milestone 2 (worker + detection pipeline) and milestone 3 (frontend)." >&2
	@echo "Available now: 'docker compose up db' and 'make api'." >&2
	@exit 2

build:
	@echo "make build: not yet implemented — production images arrive with the deployment milestone." >&2
	@exit 2

replay:
	@echo "make replay: not yet implemented — replay processor/UI is a milestone-2+ deliverable." >&2
	@echo "The ReplayDataSource exists and is exercised by tests/replay." >&2
	@exit 2

benchmark:
	@echo "make benchmark: not yet implemented — performance milestone." >&2
	@exit 2

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .hypothesis fixtures/datasets/generated
