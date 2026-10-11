#!/bin/sh
# Apply migrations, then replace this shell with the API so SIGTERM reaches uvicorn.
set -eu
alembic upgrade head
exec uvicorn consistency_api.main:app --host 0.0.0.0 --port "${API_PORT:-8000}"
