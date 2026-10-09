# Consistency Engine — developer entry points.
# Targets that belong to later milestones fail loudly (exit 2) instead of faking success.

UV ?= uv
PY := $(UV) run --frozen python
PYTEST := $(UV) run --frozen pytest
# Editable installs can be invisible to `python` on macOS (hidden .pth). pytest sets
# pythonpath itself; scripts and the worker need the same roots.
export PYTHONPATH := packages/core/src:packages/simulation/src:packages/connectors/src:packages/pipeline/src:packages/persistence/src:apps/api/src:apps/worker/src$(if $(PYTHONPATH),:$(PYTHONPATH),)

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
	@echo "seed       load synthetic markets, relationships, reviews, fees into Postgres"
	@echo "replay     replay the bundled inconsistent session and print the comparison"
	@echo "api        run the FastAPI app (health endpoints; readiness checks Postgres)"
	@echo "dev        start Postgres, migrate, and run the worker (no frontend)"
	@echo "build / benchmark: later milestones"

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

test-replay:
	$(PYTEST) tests/replay

test-integration:
	$(PYTEST) tests/integration

lint:
	$(UV) run --frozen ruff check .
	$(UV) run --frozen ruff format --check .

format:
	$(UV) run --frozen ruff format .
	$(UV) run --frozen ruff check --fix .

typecheck:
	$(UV) run --frozen mypy

seed:
	$(PY) scripts/seed_reference.py

datasets:
	$(PY) scripts/generate_datasets.py --all

api:
	$(UV) run --frozen uvicorn consistency_api.main:app --reload --port 8000

dev:
	docker compose up -d db
	@echo "Frontend (Phase 10) is not started. This target runs the database and the worker only."
	DATABASE_URL="$${DATABASE_URL:-postgresql+asyncpg://consistency:consistency@127.0.0.1:$${POSTGRES_PORT:-5432}/consistency}" \
	  $(UV) run --frozen alembic upgrade head
	DATABASE_URL="$${DATABASE_URL:-postgresql+asyncpg://consistency:consistency@127.0.0.1:$${POSTGRES_PORT:-5432}/consistency}" \
	  $(UV) run --frozen python -m consistency_worker

build:
	@echo "make build: not yet implemented — production images arrive with the deployment milestone." >&2
	@exit 2

replay:
	$(PY) scripts/replay_session.py

benchmark:
	@echo "make benchmark: not yet implemented — performance milestone." >&2
	@exit 2

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .hypothesis fixtures/datasets/generated
