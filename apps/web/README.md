# Dashboard

Next.js app. It reads persisted rows through the internal gateway
(`scripts/dashboard_gateway.py`). The public `/api/v1` catalog and `GET /api/v1/stream`
run on the API process. This UI polls and does not subscribe to that stream.

```bash
npm install
npm run dev
```

`make dev` from the repository root starts Postgres, migrations, the synthetic worker, the
gateway, and this app. `make build` runs `npm run build`.
