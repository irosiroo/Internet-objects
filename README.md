# Internet Objects 0.9

0.9 adds federation primitives and a durable outbox contract while retaining zero-dependency JSON persistence for local execution.

## Federation
- `GET /.well-known/internet-objects`
- `POST /api/federation/register`
- `POST /api/federation/ingest`
- `GET /api/peers`

Federation events are accepted as protocol envelopes and stored in the event ledger. Remote state is not blindly trusted or merged into local objects.

## Durable outbox
Webhook delivery is now queued in `state.outbox` and processed with exponential retry (up to 5 attempts). Each delivery has an idempotent delivery ID exposed as `X-Internet-Object-Delivery`.

## PostgreSQL / Neon target
`migrations.sql` defines the durable schema. The current runtime intentionally keeps JSON as the local adapter so 0.9 remains runnable without external dependencies. The next storage adapter should implement the same repository contract against PostgreSQL/Neon before multi-instance production deployment.

## Security boundary
Federation ingest records remote events but does not execute remote commands or mutate local graph state. A future trust layer should use signed federation envelopes and issuer keys before accepting state transitions from other instances.

## 0.10 — PostgreSQL/Neon adapter

Set `DATABASE_URL` to enable the Neon/PostgreSQL adapter. The local JSON store remains available for zero-dependency development. When `DATABASE_URL` is configured, schema initialization and periodic synchronization persist graphs and objects into PostgreSQL. `FEDERATION_SECRET` signs federation envelopes; signed ingest is verified when present.

## 0.11 — repository + durable PostgreSQL mode
- `repository.js` provides JSON and PostgreSQL/Neon adapters.
- PostgreSQL is the source of truth when `DATABASE_URL` is configured; startup hydrates state from PostgreSQL.
- `/api/ready` verifies storage readiness.
- PostgreSQL outbox workers use `FOR UPDATE SKIP LOCKED`, allowing multiple instances to process webhook jobs without claiming the same row.
- Added `io_receipts` and `io_idempotency` tables.
- `Idempotency-Key` is supported for compile requests in PostgreSQL mode.
- Federation ingest now requires a signature rather than accepting unsigned envelopes.
- Local JSON mode remains deterministic and dependency-light.


## 0.12

0.12 makes the PostgreSQL resolution path transaction-safe: object resolution, child creation, invite creation, event insertion, receipt creation, outbox enqueue, and idempotency persistence are committed as one database transaction. PostgreSQL object rows are locked with `FOR UPDATE`; outbox workers claim jobs with `FOR UPDATE SKIP LOCKED`. JSON mode remains available for local development.


## 0.13 — PostgreSQL-first federation

- Ed25519 federation identity with `FEDERATION_PRIVATE_KEY` / `FEDERATION_PUBLIC_KEY`.
- Public key discovery at `/.well-known/internet-objects/keys`.
- Peer registration verifies and stores the remote manifest/key.
- Federation ingest requires an enrolled issuer and Ed25519 signature.
- Federation events are queued in the durable outbox and delivered to peers.
- PostgreSQL stores peer identity and federation outbox state.
- Local JSON mode remains available for development.

For production, provide a persistent Ed25519 key pair through environment variables; otherwise the process generates an ephemeral identity for local development.

## 0.14 — Vercel runtime + federation hardening

- Vercel-compatible Node function entrypoint at `api/index.js`.
- `vercel.json` routes application requests to the Node function and includes the HTML runtime assets.
- Standalone local `node server.js` mode is preserved for development.
- Runtime version is 0.14.0.
- Federation peer registration now trusts the fetched peer manifest key and rejects a supplied key mismatch.
- Federation replay protection uses an issuer/event-id uniqueness boundary in PostgreSQL and a JSON-mode equivalent.
- Federation events are accepted only inside the configured timestamp window (`FEDERATION_MAX_SKEW_MS`, default 5 minutes).
- Capability/invite bearer tokens are excluded from federated event payloads.
- Only state-transition federation events are propagated (`activate`, `resolve`, `propagate`, `complete`).
- Recommended production deployment remains PostgreSQL/Neon as the source of truth with persistent Ed25519 keys.
