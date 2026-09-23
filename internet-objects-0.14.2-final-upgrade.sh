#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-$HOME/Internet-objects}"
cd "$ROOT"

STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP="../Internet-objects.backup-0.14.2-${STAMP}"
cp -a . "$BACKUP"
echo "BACKUP=$BACKUP"

python3 - <<'PY'
from pathlib import Path
import json,re

p=Path('package.json')
d=json.loads(p.read_text())
d['version']='0.14.2'
p.write_text(json.dumps(d,indent=2,ensure_ascii=False)+'\n')

p=Path('server.js')
s=p.read_text()
s=s.replace("const VERSION='0.14';", "const VERSION='0.14.2';", 1)
s=s.replace("function baseUrl(){return process.env.PUBLIC_BASE_URL||`http://localhost:${process.env.PORT||3000}`;}", "function baseUrl(){return String(process.env.PUBLIC_BASE_URL||`http://localhost:${process.env.PORT||3000}`).replace(/\\/+$/,'');}", 1)

old="""function parseCapability(raw){const [token,sig]=String(raw||'').split('.');const inv=state.invites[token];if(!inv||!sig||!crypto.timingSafeEqual(Buffer.from(sig),Buffer.from(inv.signature||'')))return null;if(inv.expiresAt&&Date.now()>inv.expiresAt)return null;if(inv.uses>=INVITE_MAX_USES)return null;return inv;}"""
new="""function parseCapability(raw){const [token,sig]=String(raw||'').split('.');if(!token||!sig)return null;const expected=capabilitySignature(token);if(Buffer.byteLength(sig)!==Buffer.byteLength(expected)||!crypto.timingSafeEqual(Buffer.from(sig),Buffer.from(expected)))return null;const inv=state.invites[token];if(!inv||inv.expiresAt&&Date.now()>inv.expiresAt||inv.uses>=Number(inv.maxUses||INVITE_MAX_USES))return null;return inv;}\nasync function consumeCapability(raw){\n const [token,sig]=String(raw||'').split('.');\n if(!token||!sig)return null;\n const expected=capabilitySignature(token);\n if(Buffer.byteLength(sig)!==Buffer.byteLength(expected)||!crypto.timingSafeEqual(Buffer.from(sig),Buffer.from(expected)))return null;\n if(repo.kind!=='postgres'){\n  const inv=parseCapability(raw);if(!inv)return null;\n  const o=state.objects[inv.objectId];if(!o)return {gone:true};\n  inv.uses++;inv.usedAt=Date.now();save();\n  return {invite:inv,object:o};\n }\n const e={id:id(),type:'invite_open',at:Date.now()};\n const result=await repo.consumeInvite({token,signature:expected,now:Date.now(),event:e});\n if(!result)return null;\n if(result.gone)return {gone:true};\n state.invites[token]={...result.invite,usedAt:result.invite.usedAt};\n state.objects[result.object.id]=result.object;\n state.events.push(e);\n return result;\n}"""
if old not in s: raise SystemExit('parseCapability anchor missing')
s=s.replace(old,new,1)

old="""function activate(gid){const g=state.graphs[gid];if(!g)return null;if(g.rootObjectId&&state.objects[g.rootObjectId])return state.objects[g.rootObjectId];const o=graphObject(gid,g,0);o.inviteToken=inviteFor(o.id,o.actorId);g.rootObjectId=o.id;g.status='active';g.startedAt=Date.now();event('activate',{graphId:gid,objectId:o.id,actorId:o.actorId});queueDelivery(g,'object.activated',{graphId:gid,objectId:o.id,actorId:o.actorId,publicUrl:`${baseUrl()}${inviteUrl(o.inviteToken)}`});return o;}"""
new="""async function activate(gid){\n const g=state.graphs[gid];\n if(!g)return null;\n if(g.rootObjectId&&state.objects[g.rootObjectId])return state.objects[g.rootObjectId];\n if(repo.kind==='postgres'){\n  const o=graphObject(gid,g,0);\n  delete state.objects[o.id];\n  const token=inviteToken();\n  const inv={token,objectId:o.id,actorId:o.actorId,createdAt:Date.now(),expiresAt:Date.now()+INVITE_TTL_MS,usedAt:null,uses:0,maxUses:INVITE_MAX_USES,signature:capabilitySignature(token)};\n  o.inviteToken=token;\n  const updatedGraph={...g,rootObjectId:o.id,status:'active',startedAt:Date.now()};\n  const activateEvent={id:id(),type:'activate',at:Date.now(),graphId:gid,objectId:o.id,actorId:o.actorId};\n  const delivery=updatedGraph.webhookUrl?{id:id(),kind:'webhook',graphId:gid,eventType:'object.activated',payload:{graphId:gid,objectId:o.id,actorId:o.actorId,publicUrl:`${baseUrl()}${inviteUrl(token)}`},attempts:0,status:'pending',nextAttemptAt:Date.now()}:null;\n  const result=await repo.atomicActivate({graph:updatedGraph,object:o,invite:inv,event:activateEvent,delivery});\n  if(result.existing){\n   state.graphs[gid]=result.graph;\n   state.objects[result.object.id]=result.object;\n   state.invites[result.invite.token]=result.invite;\n   return result.object;\n  }\n  state.graphs[gid]=updatedGraph;state.objects[o.id]=o;state.invites[token]=inv;state.events.push(activateEvent);if(delivery)state.outbox[delivery.id]=delivery;\n  return o;\n }\n const o=graphObject(gid,g,0);o.inviteToken=inviteFor(o.id,o.actorId);g.rootObjectId=o.id;g.status='active';g.startedAt=Date.now();event('activate',{graphId:gid,objectId:o.id,actorId:o.actorId});queueDelivery(g,'object.activated',{graphId:gid,objectId:o.id,actorId:o.actorId,publicUrl:`${baseUrl()}${inviteUrl(o.inviteToken)}`});return o;\n}"""
if old not in s: raise SystemExit('activate anchor missing')
s=s.replace(old,new,1)

old="""if(req.method==='GET'&&u.pathname.startsWith('/r/')){const raw=u.pathname.split('/')[2],inv=parseCapability(raw);if(!inv)return res.writeHead(404),res.end('Invite not found');const o=state.objects[inv.objectId];if(!o)return res.writeHead(410),res.end('State no longer exists');inv.uses++;inv.usedAt=Date.now();save();event('invite_open',{token:inv.token,objectId:o.id,actorId:inv.actorId,uses:inv.uses});return res.writeHead(302,{location:`/o/${o.id}?actor=${encodeURIComponent(inv.actorId)}&invite=${encodeURIComponent(inv.token)}`}),res.end()}"""
new="""if(req.method==='GET'&&u.pathname.startsWith('/r/')){const raw=u.pathname.split('/')[2],result=await consumeCapability(raw);if(!result)return res.writeHead(404),res.end('Invite not found');if(result.gone)return res.writeHead(410),res.end('State no longer exists');const inv=result.invite,o=result.object;return res.writeHead(302,{location:`/o/${o.id}?actor=${encodeURIComponent(inv.actorId)}&invite=${encodeURIComponent(inv.token)}`}),res.end()}"""
if old not in s: raise SystemExit('redirect anchor missing')
s=s.replace(old,new,1)

old="if(req.method==='POST'&&u.pathname==='/api/graphs/'"
# no-op: retain route replacement below
old="""m=u.pathname.match(/^\\/api\\/graphs\\/([^/]+)\\/activate$/);if(req.method==='POST'&&m){const o=activate(m[1]);return o?json(res,201,{object:publicObject(o),publicUrl:`${baseUrl()}${o.inviteToken?inviteUrl(o.inviteToken):`/o/${o.id}`}`}):json(res,404,{error:'graph not found'});}"""
new="""m=u.pathname.match(/^\\/api\\/graphs\\/([^/]+)\\/activate$/);if(req.method==='POST'&&m){const o=await activate(m[1]);return o?json(res,201,{object:publicObject(o),publicUrl:`${baseUrl()}${o.inviteToken?inviteUrl(o.inviteToken):`/o/${o.id}`}`}):json(res,404,{error:'graph not found'});}"""
if old not in s: raise SystemExit('activate route anchor missing')
s=s.replace(old,new,1)
p.write_text(s)

p=Path('repository.js')
s=p.read_text()

json_old="""  return { kind:'json', state, save, async init(){}, async ready(){return true}, async close(){} };"""
json_new="""  async function consumeInvite({token,signature,now,event}){\n    const inv=state.invites[token];\n    if(!inv||inv.signature!==signature)return null;\n    if(inv.expiresAt&&now>inv.expiresAt)return null;\n    if(inv.uses>=Number(inv.maxUses||3))return null;\n    const object=state.objects[inv.objectId];\n    if(!object)return {gone:true};\n    inv.uses++;inv.usedAt=now;\n    state.events.push({...event,token,objectId:object.id,actorId:inv.actorId,uses:inv.uses});\n    save();\n    return {invite:inv,object};\n  }\n  return { kind:'json', state, save, consumeInvite, async init(){}, async ready(){return true}, async close(){} };"""
if json_old not in s: raise SystemExit('json repo anchor missing')
s=s.replace(json_old,json_new,1)

anchor="""  async function atomicResolve({object,graph,child,invite,events,completedReceipt,idempotencyKey,idempotencyResponse,federationDeliveries=[]}){\n"""
methods="""  async function consumeInvite({token,signature,now,event}){\n    return transaction(async tx=>{\n      const r=await tx.query('select token,object_id,actor_id,signature,extract(epoch from expires_at)*1000 expires_at,uses,max_uses,extract(epoch from created_at)*1000 created_at from io_invites where token=$1 for update',[token]);\n      if(!r.rows[0])return null;\n      const x=r.rows[0];\n      if(x.signature!==signature)return null;\n      if(x.expires_at&&now>Number(x.expires_at))return null;\n      if(x.uses>=Number(x.max_uses||3))return null;\n      const o=await tx.query('select payload from io_objects where id=$1',[x.object_id]);\n      if(!o.rows[0])return {gone:true};\n      const uses=Number(x.uses)+1;\n      await tx.query('update io_invites set uses=$2 where token=$1',[token,uses]);\n      const e={...event,token,objectId:x.object_id,actorId:x.actor_id,uses};\n      await tx.insertEvent(e);\n      return {invite:{token:x.token,objectId:x.object_id,actorId:x.actor_id,signature:x.signature,expiresAt:Number(x.expires_at),uses,maxUses:Number(x.max_uses),createdAt:Number(x.created_at),usedAt:now},object:o.rows[0].payload};\n    });\n  }\n  async function atomicActivate({graph,object,invite,event,delivery}){\n    return transaction(async tx=>{\n      const locked=await tx.query('select payload from io_graphs where id=$1 for update',[graph.id]);\n      if(!locked.rows[0])throw Object.assign(new Error('graph not found'),{code:'not_found'});\n      const current=locked.rows[0].payload;\n      if(current.rootObjectId){\n        const ro=await tx.query('select payload from io_objects where id=$1',[current.rootObjectId]);\n        const ri=await tx.query('select token,object_id,actor_id,signature,extract(epoch from expires_at)*1000 expires_at,uses,max_uses,extract(epoch from created_at)*1000 created_at from io_invites where object_id=$1 order by created_at asc limit 1',[current.rootObjectId]);\n        if(ro.rows[0]&&ri.rows[0])return {existing:true,graph:current,object:ro.rows[0].payload,invite:{token:ri.rows[0].token,objectId:ri.rows[0].object_id,actorId:ri.rows[0].actor_id,signature:ri.rows[0].signature,expiresAt:Number(ri.rows[0].expires_at),uses:Number(ri.rows[0].uses),maxUses:Number(ri.rows[0].max_uses),createdAt:Number(ri.rows[0].created_at)}};\n      }\n      await upsertObject(tx,object);\n      await upsertGraph(tx,graph);\n      await putInvite(tx,invite);\n      await tx.insertEvent(event);\n      if(delivery)await putOutbox(tx,delivery);\n      return {existing:false};\n    });\n  }\n"""
if anchor not in s: raise SystemExit('atomicResolve anchor missing')
s=s.replace(anchor,methods+anchor,1)

old="""return {kind:'postgres',pool,init,load,ready:async()=>{await q('select 1');return true},close:()=>pool.end(),transaction,upsertObject,upsertGraph,putInvite,putOutbox,putPeer,putReceipt,idempotency,putIdempotency,atomicResolve,claimOutbox,finishOutbox};"""
new="""return {kind:'postgres',pool,init,load,ready:async()=>{await q('select 1');return true},close:()=>pool.end(),transaction,upsertObject,upsertGraph,putInvite,putOutbox,putPeer,putReceipt,idempotency,putIdempotency,consumeInvite,atomicActivate,atomicResolve,claimOutbox,finishOutbox};"""
if old not in s: raise SystemExit('return anchor missing')
s=s.replace(old,new,1)
p.write_text(s)
PY

node --check server.js
node --check repository.js
node -e "const p=require('./package.json'); if(p.version!=='0.14.2') process.exit(1); console.log('package 0.14.2 OK')"

echo
printf '%s\n' '=== 0.14.2 verification ==='
grep -n "VERSION='0.14.2'\|async function consumeCapability\|async function activate\|atomicActivate\|consumeInvite" server.js repository.js
printf '%s\n' '=== git diff --stat ==='
git diff --stat
printf '%s\n' '=== git diff --check ==='
git diff --check
printf '%s\n' 'READY: 0.14.2 patch applied and syntax-checked.'

# JSON fallback E2E smoke test. This validates the complete lifecycle without requiring Neon.
TEST_DIR="$(mktemp -d)"
PORT_TEST=4187
PID=""
cleanup(){ if [ -n "$PID" ]; then kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi; rm -rf "$TEST_DIR"; }
trap cleanup EXIT
DATA_DIR="$TEST_DIR" PORT="$PORT_TEST" PUBLIC_BASE_URL="http://127.0.0.1:$PORT_TEST" node server.js >/tmp/internet-objects-0.14.2-test.log 2>&1 &
PID=$!
for i in $(seq 1 50); do curl -fsS "http://127.0.0.1:$PORT_TEST/api/ready" >/dev/null 2>&1 && break; sleep 0.1; done
curl -fsS "http://127.0.0.1:$PORT_TEST/api/ready" >/dev/null
node --input-type=module <<'NODE'
const base='http://127.0.0.1:4187';
const post=async(path,body,headers={})=>{const r=await fetch(base+path,{method:'POST',headers:{'content-type':'application/json',...headers},body:JSON.stringify(body)});const t=await r.text();if(!r.ok)throw new Error(`${path} ${r.status}: ${t}`);return JSON.parse(t)};
const get=async(path,opts={})=>{const r=await fetch(base+path,opts);const t=await r.text();return {r,t,json:()=>JSON.parse(t)}};
const c=await post('/api/compile',{prompt:'Review a document with 3 reviewers sequentially'});
if(c.graphId==null)throw new Error('compile failed');
const a=await post(`/api/graphs/${c.graphId}/activate`,{});
if(!a.object?.id||!a.publicUrl.includes('/r/'))throw new Error('activate failed');
let r=await get(a.publicUrl,{redirect:'manual'});
if(r.r.status!==302)throw new Error(`invite redirect ${r.r.status}`);
const rootId=a.object.id;
let current=rootId;
for(let i=0;i<3;i++){
  const o=(await get(`/api/objects/${current}`)).json().object;
  const resolved=await post(`/api/objects/${current}`,{answer:'Complete',actorId:o.actorId},{'idempotency-key':`e2e-${current}`});
  if(i<2){if(!resolved.nextObject?.id)throw new Error(`missing next object at ${i}`);current=resolved.nextObject.id;}
  else if(!resolved.completed||!resolved.receipt?.id)throw new Error('graph did not complete');
}
const stats=(await get('/api/stats')).json();
if(stats.completedGraphs<1||stats.resolutions<3||stats.propagations<2)throw new Error(`bad stats ${JSON.stringify(stats)}`);
console.log(JSON.stringify({ok:true,graphId:c.graphId,rootId,completedGraphs:stats.completedGraphs,resolutions:stats.resolutions,propagations:stats.propagations}));
NODE
printf '%s\n' 'READY: 0.14.2 local JSON lifecycle E2E passed.'
