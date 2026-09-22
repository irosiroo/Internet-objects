import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export function createJsonRepository({dataDir, initial}) {
  const file = path.join(dataDir, 'store.json');
  fs.mkdirSync(dataDir, {recursive:true});
  let state = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file,'utf8')) : structuredClone(initial);
  for (const k of Object.keys(initial)) if (state[k] === undefined) state[k] = structuredClone(initial[k]);
  const save = () => fs.writeFileSync(file, JSON.stringify(state, null, 2));
  return { kind:'json', state, save, async init(){}, async ready(){return true}, async close(){} };
}

export async function createPostgresRepository({databaseUrl, dataDir, initial}) {
  const {Pool} = await import('@neondatabase/serverless');
  const pool = new Pool({connectionString: databaseUrl});
  const q = async (text, params=[]) => (await pool.query(text, params)).rows;
  const schema = [
    `create table if not exists io_graphs(id text primary key,status text not null,payload jsonb not null,created_at timestamptz not null default now(),updated_at timestamptz not null default now())`,
    `create table if not exists io_objects(id text primary key,graph_id text,actor_id text,state text not null,payload jsonb not null,created_at timestamptz not null default now(),updated_at timestamptz not null default now())`,
    `create table if not exists io_events(id text primary key,type text not null,payload jsonb not null,created_at timestamptz not null default now())`,
    `create table if not exists io_invites(token text primary key,object_id text not null,actor_id text,signature text not null,expires_at timestamptz,uses integer not null default 0,max_uses integer not null default 3,created_at timestamptz not null default now())`,
    `create table if not exists io_outbox(id text primary key,kind text not null,graph_id text,event_type text,payload jsonb not null,status text not null default 'pending',attempts integer not null default 0,next_attempt_at timestamptz not null default now(),completed_at timestamptz,error text)`,
    `create index if not exists io_outbox_pending_idx on io_outbox(status,next_attempt_at)`,
    `create table if not exists io_peers(id text primary key,url text unique not null,issuer text unique,payload jsonb not null,created_at timestamptz not null default now(),last_seen_at timestamptz)`,
    `create table if not exists io_receipts(id text primary key,graph_id text,status text not null,payload jsonb not null,created_at timestamptz not null default now())`,
    `create table if not exists io_idempotency(scope text not null,key text not null,response jsonb not null,created_at timestamptz not null default now(),primary key(scope,key))`
  ];
  async function init(){ for(const s of schema) await q(s); await q('alter table io_peers add column if not exists issuer text unique'); }
  async function load(){
    const state=structuredClone(initial);
    for(const [k,sql] of Object.entries({
      graphs:'select payload from io_graphs', objects:'select payload from io_objects', events:'select payload from io_events order by created_at asc limit 10000', invites:'select token,object_id,actor_id,signature,extract(epoch from expires_at)*1000 expires_at,uses,max_uses,extract(epoch from created_at)*1000 created_at from io_invites', outbox:'select id,kind,graph_id,event_type,payload,status,attempts,extract(epoch from next_attempt_at)*1000 next_attempt_at,extract(epoch from completed_at)*1000 completed_at,error from io_outbox', peers:'select id,url,issuer,payload,extract(epoch from created_at)*1000 created_at,extract(epoch from last_seen_at)*1000 last_seen_at from io_peers', receipts:'select payload from io_receipts'
    })){
      const rows=await q(sql);
      if(k==='graphs'||k==='objects'||k==='receipts') for(const r of rows){const x=r.payload;state[k][x.id]=x}
      else if(k==='events') state[k]=rows.map(r=>r.payload)
      else if(k==='invites') for(const r of rows) state[k][r.token]={token:r.token,objectId:r.object_id,actorId:r.actor_id,signature:r.signature,expiresAt:Number(r.expires_at),uses:r.uses,maxUses:r.max_uses,createdAt:Number(r.created_at)}
      else if(k==='outbox') for(const r of rows) state[k][r.id]={...r,payload:r.payload,nextAttemptAt:Number(r.next_attempt_at),completedAt:r.completed_at?Number(r.completed_at):null}
      else if(k==='peers') for(const r of rows) state[k][r.id]={...r,payload:r.payload,issuer:r.issuer||null,createdAt:Number(r.created_at),lastSeenAt:r.last_seen_at?Number(r.last_seen_at):null}
    }
    return state;
  }
  async function transaction(fn){
    const client=await pool.connect();
    try{await client.query('begin'); const tx={query:(text,params)=>client.query(text,params),insertEvent:async e=>client.query('insert into io_events(id,type,payload,created_at) values($1,$2,$3,now()) on conflict do nothing',[e.id,e.type,JSON.stringify(e)])}; const out=await fn(tx); await client.query('commit'); return out}catch(e){try{await client.query('rollback')}catch{} throw e}finally{client.release()}
  }
  async function upsertObject(tx,o){await tx.query(`insert into io_objects(id,graph_id,actor_id,state,payload,created_at,updated_at) values($1,$2,$3,$4,$5,$6,now()) on conflict(id) do update set graph_id=excluded.graph_id,actor_id=excluded.actor_id,state=excluded.state,payload=excluded.payload,updated_at=now()`,[o.id,o.graphId||null,o.actorId||null,o.state,JSON.stringify(o),new Date(o.createdAt)])}
  async function upsertGraph(tx,g){await tx.query(`insert into io_graphs(id,status,payload,created_at,updated_at) values($1,$2,$3,$4,now()) on conflict(id) do update set status=excluded.status,payload=excluded.payload,updated_at=now()`,[g.id,g.status,JSON.stringify(g),new Date(g.createdAt)])}
  async function putInvite(tx,i){await tx.query(`insert into io_invites(token,object_id,actor_id,signature,expires_at,uses,max_uses,created_at) values($1,$2,$3,$4,$5,$6,$7,$8) on conflict(token) do update set uses=excluded.uses`,[i.token,i.objectId,i.actorId,i.signature,new Date(i.expiresAt),i.uses,i.maxUses,new Date(i.createdAt)])}
  async function putOutbox(tx,o){await tx.query(`insert into io_outbox(id,kind,graph_id,event_type,payload,status,attempts,next_attempt_at) values($1,$2,$3,$4,$5,$6,$7,$8) on conflict(id) do nothing`,[o.id,o.kind,o.graphId,o.eventType,JSON.stringify(o.payload),o.status,o.attempts,new Date(o.nextAttemptAt)])}
  async function putPeer(tx,p){await tx.query(`insert into io_peers(id,url,issuer,payload,created_at,last_seen_at) values($1,$2,$3,$4,$5,$6) on conflict(id) do update set url=excluded.url,issuer=excluded.issuer,payload=excluded.payload,last_seen_at=excluded.last_seen_at`,[p.id,p.url,p.issuer||null,JSON.stringify(p),new Date(p.createdAt),p.lastSeenAt?new Date(p.lastSeenAt):null])}
  async function putReceipt(tx,r){await tx.query(`insert into io_receipts(id,graph_id,status,payload,created_at) values($1,$2,$3,$4,$5) on conflict(id) do nothing`,[r.id,r.graphId,r.status,JSON.stringify(r),new Date(r.issuedAt)])}
  async function idempotency(scope,key,response){const rows=await q('select response from io_idempotency where scope=$1 and key=$2',[scope,key]);if(rows[0])return rows[0].response;return null}
  async function putIdempotency(tx,scope,key,response){const r=await tx.query('insert into io_idempotency(scope,key,response) values($1,$2,$3) on conflict(scope,key) do nothing returning response',[scope,key,JSON.stringify(response)]);return r.rows[0]?.response||null}
  async function atomicResolve({object,graph,child,invite,events,completedReceipt,idempotencyKey,idempotencyResponse,federationDeliveries=[]}){
    return transaction(async tx=>{
      const locked=await tx.query('select payload,state from io_objects where id=$1 for update',[object.id]);
      if(!locked.rows[0]) throw Object.assign(new Error('object not found'),{code:'not_found'});
      if(locked.rows[0].state==='resolved') return {duplicate:true,payload:locked.rows[0].payload};
      if(idempotencyKey){const prior=await tx.query('select response from io_idempotency where scope=$1 and key=$2',[idempotencyKey.scope,idempotencyKey.key]);if(prior.rows[0])return {duplicate:true,payload:prior.rows[0].response};}
      await tx.query('update io_objects set state=$2,payload=$3,updated_at=now() where id=$1',[object.id,object.state,JSON.stringify(object)]);
      if(child) await upsertObject(tx,child);
      if(graph) await upsertGraph(tx,graph);
      if(invite) await putInvite(tx,invite);
      for(const e of events||[]) await tx.insertEvent(e);
      if(completedReceipt) await putReceipt(tx,completedReceipt);
      for(const q of (events||[]).filter(e=>e.delivery)){const d=q.delivery;await putOutbox(tx,d);}
      for(const d of (federationDeliveries||[])) await putOutbox(tx,d);
      if(idempotencyKey) await putIdempotency(tx,idempotencyKey.scope,idempotencyKey.key,idempotencyResponse);
      return {duplicate:false,payload:idempotencyResponse};
    });
  }
  async function claimOutbox(){const client=await pool.connect();try{await client.query('begin');const r=await client.query(`select * from io_outbox where status='pending' and next_attempt_at<=now() order by next_attempt_at asc for update skip locked limit 1`);if(!r.rows[0]){await client.query('commit');return null}const x=r.rows[0];await client.query(`update io_outbox set status='processing',attempts=attempts+1 where id=$1`,[x.id]);await client.query('commit');return {...x,payload:x.payload,attempts:x.attempts+1,nextAttemptAt:new Date(x.next_attempt_at).getTime()};}catch(e){try{await client.query('rollback')}catch{}throw e}finally{client.release()}}
  async function finishOutbox(x,{ok,error}){if(ok)await q(`update io_outbox set status='delivered',completed_at=now(),error=null where id=$1`,[x.id]);else if(x.attempts>=5)await q(`update io_outbox set status='failed',completed_at=now(),error=$2 where id=$1`,[x.id,error||'delivery failed']);else await q(`update io_outbox set status='pending',next_attempt_at=now()+($2 * interval '1 second'),error=$3 where id=$1`,[x.id,Math.min(300,2**(x.attempts-1)),error||'delivery failed']);}
  return {kind:'postgres',pool,init,load,ready:async()=>{await q('select 1');return true},close:()=>pool.end(),transaction,upsertObject,upsertGraph,putInvite,putOutbox,putPeer,putReceipt,idempotency,putIdempotency,atomicResolve,claimOutbox,finishOutbox};
}
