# HookRail

Crash-safe, signed webhook delivery infrastructure in Go. HookRail accepts CloudEvents, durably acknowledges them, and delivers them with bounded concurrency, exponential backoff, dead-lettering, and replay.

## Quick start

```bash
docker compose up --build
```

- API: <http://localhost:8082>
- Delivery dashboard: <http://localhost:8083>

Create a destination with `POST /v1/destinations`, then publish a structured CloudEvent with `POST /v1/events`. Receivers verify `HookRail-Signature: t=<unix>,v1=<hmac-sha256>` over `timestamp.payload`.

## Delivery guarantees

- Events are fsync'd before ingestion returns success.
- Event IDs are idempotent.
- Delivery is at least once; receivers must deduplicate by event ID.
- A successful `2xx` response is terminal.
- Exhausted events move to `dead_letter` and can be manually replayed with a fresh signature.

The v0.1 reference adapter uses an append-only local WAL so crash recovery is demonstrable without infrastructure. The store boundary is designed for a PostgreSQL leasing adapter; v0.1 does not claim multi-node delivery.

See [architecture](docs/architecture.md), [failure semantics](docs/failure-semantics.md), and [benchmarks](docs/benchmarks.md).

Apache-2.0 licensed.
