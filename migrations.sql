-- Internet Objects 0.9 durable storage target (PostgreSQL/Neon)
create table if not exists io_objects(id text primary key, graph_id text, actor_id text, state text not null, payload jsonb not null, created_at timestamptz not null default now(), updated_at timestamptz not null default now());
create table if not exists io_graphs(id text primary key, status text not null, payload jsonb not null, created_at timestamptz not null default now(), updated_at timestamptz not null default now());
create table if not exists io_events(id text primary key, type text not null, payload jsonb not null, created_at timestamptz not null default now());
create table if not exists io_invites(token text primary key, object_id text not null, actor_id text, signature text not null, expires_at timestamptz, uses integer not null default 0, max_uses integer not null default 3, created_at timestamptz not null default now());
create table if not exists io_outbox(id text primary key, kind text not null, graph_id text, event_type text, payload jsonb not null, status text not null default 'pending', attempts integer not null default 0, next_attempt_at timestamptz not null default now(), completed_at timestamptz, error text);
create index if not exists io_outbox_pending_idx on io_outbox(status,next_attempt_at);
create table if not exists io_peers(id text primary key, url text unique not null, payload jsonb not null, created_at timestamptz not null default now(), last_seen_at timestamptz);

create table if not exists io_receipts(id text primary key, graph_id text, status text not null, payload jsonb not null, created_at timestamptz not null default now());
create table if not exists io_idempotency(scope text not null, key text not null, response jsonb not null, created_at timestamptz not null default now(), primary key(scope,key));

-- 0.14 federation replay protection
create table if not exists io_federation_events (
  issuer text not null,
  event_id text not null,
  received_at timestamptz not null default now(),
  primary key (issuer, event_id)
);
