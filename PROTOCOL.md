# Internet Objects Protocol 0.8

An Internet Object is a public stateful resource with an actor, a pending/resolved state, optional lineage, and a transition to another object.

## Discovery
`GET /api/discover` exposes currently pending objects and their protocol entry points.

## Protocol manifest
`GET /api/protocol` describes capabilities and endpoint semantics.

## Event model
Events are append-only records: `compile`, `activate`, `visit`, `invite_created`, `invite_open`, `resolve`, `propagate`, `complete`, `webhook_delivery`.

## Production storage
The prototype keeps JSON persistence for zero-dependency local operation. For production, set `DATABASE_URL` and replace the persistence adapter with Postgres/Neon using the same state/event repository interface. Do not use process-local JSON as the source of truth across multiple instances.

## Abuse controls
Every HTTP request is rate-limited per client key. `RATE_LIMIT` and `RATE_WINDOW_MS` tune the guard. Production should add durable quotas, bot filtering, signed actor capabilities, replay protection, and per-graph limits.


## Capability security (0.8)

Invite URLs are bearer capabilities bound to an object and actor. Each capability carries an HMAC signature, expires after `INVITE_TTL_MS` (default 24 hours), and is limited to `INVITE_MAX_USES` (default 3). Discovery is opt-out by default at graph creation and only exposes pending objects marked discoverable. Configure `CAPABILITY_SECRET` in any non-local deployment.

## Production boundary

The JSON repository remains a local prototype persistence adapter. A production deployment must move state, events, invite replay/usage counters, and webhook outbox records to durable shared storage before horizontal scaling.
