# Dashboard

Next.js app for Phase 10. It reads persisted rows through the internal gateway
(`scripts/dashboard_gateway.py`). It does not implement the Phase 11 `/api/v1` catalog or
an SSE stream. The UI polls.

```bash
npm install
npm run dev
```

`make dev` from the repository root starts Postgres, migrations, the synthetic worker, the
gateway, and this app. `make build` runs `npm run build`.
