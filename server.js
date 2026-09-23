import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { createJsonRepository, createPostgresRepository } from './repository.js';

const DATABASE_URL=process.env.DATABASE_URL||'';
const DATA_DIR = process.env.DATA_DIR || (process.env.VERCEL === '1' ? '/tmp/internet-objects-data' : path.join(process.cwd(), 'data'));
const initial={objects:{},graphs:{},events:[],invites:{},receipts:{},deliveries:{},outbox:{},peers:{},federationEvents:{}};
// Vercel's /var/task filesystem is read-only; /tmp is the only writable fallback.
fs.mkdirSync(DATA_DIR,{recursive:true});
const RATE_WINDOW=Number(process.env.RATE_WINDOW_MS||60000);
const RATE_LIMIT=Number(process.env.RATE_LIMIT||120);
const VERSION='0.14';
let repo = DATABASE_URL ? await createPostgresRepository({databaseUrl:DATABASE_URL,dataDir:DATA_DIR,initial}) : createJsonRepository({dataDir:DATA_DIR,initial});
await repo.init();
const state = repo.kind==='postgres' ? await repo.load() : repo.state;
const save=()=>repo.kind==='json' ? repo.save() : undefined;
async function mirror(){if(repo.kind!=='postgres')return;await repo.transaction(async tx=>{for(const g of Object.values(state.graphs))await repo.upsertGraph(tx,g);for(const o of Object.values(state.objects))await repo.upsertObject(tx,o);for(const i of Object.values(state.invites))await repo.putInvite(tx,i);for(const q of Object.values(state.outbox))if(q.status==='pending')await repo.putOutbox(tx,q);for(const r of Object.values(state.receipts))await repo.putReceipt(tx,r);});}
const INVITE_TTL_MS=Number(process.env.INVITE_TTL_MS||86400000);
const INVITE_MAX_USES=Number(process.env.INVITE_MAX_USES||3);
const CAPABILITY_SECRET=process.env.CAPABILITY_SECRET||process.env.WEBHOOK_SECRET||'development-capability-secret';
const FEDERATION_MAX_SKEW_MS=Number(process.env.FEDERATION_MAX_SKEW_MS||300000);
const federationKeys=(()=>{try{if(process.env.FEDERATION_PRIVATE_KEY&&process.env.FEDERATION_PUBLIC_KEY)return {privateKey:crypto.createPrivateKey(process.env.FEDERATION_PRIVATE_KEY),publicKey:crypto.createPublicKey(process.env.FEDERATION_PUBLIC_KEY)};const k=crypto.generateKeyPairSync('ed25519');return {privateKey:k.privateKey,publicKey:k.publicKey,ephemeral:true};}catch(e){throw new Error(`federation key init failed: ${e.message}`)}})();
const federationIssuer=()=>baseUrl();
function federationPublicKeyPem(){return federationKeys.publicKey.export({type:'spki',format:'pem'}).toString();}
function signFederationEnvelope(envelope){const body=JSON.stringify(envelope);return crypto.sign(null,Buffer.from(body),federationKeys.privateKey).toString('base64url');}
function verifyFederationEnvelope(envelope,signature,publicKey){try{return crypto.verify(null,Buffer.from(JSON.stringify(envelope)),crypto.createPublicKey(publicKey),Buffer.from(signature,'base64url'));}catch{return false;}}

const rateBuckets=new Map();
function clientKey(req){return req.headers['x-forwarded-for']?.split(',')[0]?.trim()||req.socket.remoteAddress||'unknown';}
function rateOk(req){const k=clientKey(req),now=Date.now();let b=rateBuckets.get(k);if(!b||now-b.start>RATE_WINDOW)b={start:now,count:0};b.count++;rateBuckets.set(k,b);return b.count<=RATE_LIMIT;}
function discovery(){return Object.values(state.objects).filter(o=>o.state==='pending'&&o.inviteToken&&o.discoverable!==false).slice(-100).map(o=>({id:o.id,graphId:o.graphId,actorId:o.actorId,title:o.title,description:o.description,createdAt:o.createdAt,protocol:`${baseUrl()}/protocol`,entry:`${baseUrl()}${inviteUrl(o.inviteToken)}`}));}
function protocolManifest(){return {protocol:'internet-objects',version:VERSION,capabilities:['state','actor','lineage','invite','capability','events','webhooks','receipts','discovery','federation','outbox'],security:{inviteTtlMs:INVITE_TTL_MS,maxInviteUses:INVITE_MAX_USES},endpoints:{discovery:'/api/discover',events:'/api/events',receipts:'/api/receipts',health:'/api/health',protocol:'/api/protocol',federation:'/api/federation/ingest'},semantics:{pending:'awaiting actor action',resolved:'actor action recorded',propagated:'next actor state created',completed:'graph terminal state reached'},federation:{manifest:'/.well-known/internet-objects',ingest:'/api/federation/ingest',events:'/api/federation/events',keys:'/.well-known/internet-objects/keys'}};}
function id(){return crypto.randomBytes(7).toString('base64url');}
function inviteToken(){return crypto.randomBytes(18).toString('base64url');}
function capabilitySignature(token){return crypto.createHmac('sha256',CAPABILITY_SECRET).update(token).digest('base64url');}
function inviteFor(objectId, actorId){const token=inviteToken();state.invites[token]={token,objectId,actorId,createdAt:Date.now(),expiresAt:Date.now()+INVITE_TTL_MS,usedAt:null,uses:0,maxUses:INVITE_MAX_USES,signature:capabilitySignature(token)};save();event('invite_created',{token,objectId,actorId});return token;}
function inviteUrl(token){const inv=state.invites[token];const sig=inv?.signature||capabilitySignature(token);return `/r/${token}.${sig}`;}
function parseCapability(raw){const [token,sig]=String(raw||'').split('.');const inv=state.invites[token];if(!inv||!sig||!crypto.timingSafeEqual(Buffer.from(sig),Buffer.from(inv.signature||'')))return null;if(inv.expiresAt&&Date.now()>inv.expiresAt)return null;if(inv.uses>=INVITE_MAX_USES)return null;return inv;}
function baseUrl(){return process.env.PUBLIC_BASE_URL||`http://localhost:${process.env.PORT||3000}`;}
function signPayload(secret, body){return crypto.createHmac('sha256',secret).update(body).digest('hex');}
function receiptFor(graph){const rid=id();const r={id:rid,graphId:graph.id||null,status:'completed',issuedAt:Date.now(),objects:Object.values(state.objects).filter(o=>o.graphId===graph.id).map(o=>({id:o.id,actorId:o.actorId,answer:o.answer,state:o.state}))};state.receipts[rid]=r;save();return r;}
function json(res,status,data){res.writeHead(status,{'content-type':'application/json; charset=utf-8','cache-control':'no-store','access-control-allow-origin':'*'});res.end(JSON.stringify(data));}
async function body(req){let s='';for await(const c of req)s+=c;return JSON.parse(s||'{}')}
function event(type,payload={}){const e={id:id(),type,at:Date.now(),...payload};state.events.push(e);if(state.events.length>10000)state.events=state.events.slice(-10000);save();if(repo.kind==='postgres') repo.transaction(tx=>tx.insertEvent(e)).catch(console.error);return e}
function compileDeterministic(prompt){
 const lower=prompt.toLowerCase();
 const n=Number((prompt.match(/\b(\d+)\s*(people|persons|users|participants|humans|reviewers|approvers)\b/i)||[])[1])||2;
 const count=Math.max(2,Math.min(20,n));
 const kind=/review|approve|approval|verify|verification/.test(lower)?'review':/vote|choose|pick|select|decision|decide/.test(lower)?'decision':'task';
 const sequential=/then|after|next|in order|sequential|one by one|each/.test(lower);
 const action=kind==='review'?'review':kind==='decision'?'decide':'complete';
 const actors=Array.from({length:count},(_,i)=>({id:`actor_${i+1}`,role:i===0?'creator':`participant_${i}`,action,required:true}));
 return {version:VERSION,intent:prompt,kind,actors,transitions:actors.slice(0,-1).map((a,i)=>({from:a.id,to:actors[i+1].id,when:'resolved'})),resolution:{mode:sequential?'sequential':'all_required'}};
}
async function compileWithAI(prompt){
 const key=process.env.AI_API_KEY||process.env.OPENAI_API_KEY;if(!key)return null;
 const base=process.env.AI_BASE_URL||'https://api.openai.com/v1',model=process.env.AI_MODEL;if(!model) return null;
 const r=await fetch(`${base}/chat/completions`,{method:'POST',headers:{'content-type':'application/json','authorization':`Bearer ${key}`},body:JSON.stringify({model,temperature:0,response_format:{type:'json_object'},messages:[{role:'system',content:'Compile the process into JSON: {kind,actors:[{id,role,action,required}],transitions:[{from,to,when}],resolution:{mode}}. Max 20 actors. First actor creator. mode sequential or all_required. No prose.'},{role:'user',content:prompt}]})});
 if(!r.ok)throw Error(`AI ${r.status}`);const d=await r.json();return {version:VERSION+'-ai',intent:prompt,...JSON.parse(d.choices?.[0]?.message?.content||'{}')};
}
function graphObject(gid,g,actorIndex,parent=null){const actor=g.actors[actorIndex];const oid=id();const o={id:oid,graphId:gid,actorId:actor.id,actorIndex,discoverable:g.discoverable!==false,title:g.intent.slice(0,100),description:`${actor.role}: ${actor.action}. Resolve this state to activate the next actor.`,choices:g.kind==='decision'?['Approve','Reject']:['Complete','Needs changes'],state:'pending',createdAt:Date.now(),visited:0,resolvedAt:null,resolvedBy:null,answer:null,parentObjectId:parent,childObjectId:null};state.objects[oid]=o;return o;}
function publicObject(o){return {...o,url:`/o/${o.id}?actor=${encodeURIComponent(o.actorId)}` ,inviteUrl:o.inviteToken?inviteUrl(o.inviteToken):null}}
function activate(gid){const g=state.graphs[gid];if(!g)return null;if(g.rootObjectId&&state.objects[g.rootObjectId])return state.objects[g.rootObjectId];const o=graphObject(gid,g,0);o.inviteToken=inviteFor(o.id,o.actorId);g.rootObjectId=o.id;g.status='active';g.startedAt=Date.now();event('activate',{graphId:gid,objectId:o.id,actorId:o.actorId});queueDelivery(g,'object.activated',{graphId:gid,objectId:o.id,actorId:o.actorId,publicUrl:`${baseUrl()}${inviteUrl(o.inviteToken)}`});return o;}
function advance(o){const g=state.graphs[o.graphId];const nextIndex=o.actorIndex+1;if(!g||nextIndex>=g.actors.length)return null;const next=graphObject(o.graphId,g,nextIndex,o.id);next.inviteToken=inviteFor(next.id,next.actorId);o.childObjectId=next.id;event('propagate',{graphId:o.graphId,parentObjectId:o.id,objectId:next.id,actorId:next.actorId,chainIndex:next.actorIndex});queueDelivery(g,'object.propagated',{graphId:o.graphId,parentObjectId:o.id,objectId:next.id,actorId:next.actorId,publicUrl:`${baseUrl()}${inviteUrl(next.inviteToken)}`});return next;}
function stats(){const ev=state.events;const visits=ev.filter(e=>e.type==='visit');const resolves=ev.filter(e=>e.type==='resolve');const propagated=ev.filter(e=>e.type==='propagate');const uniqueActors=new Set(visits.map(e=>e.actorId).filter(Boolean));const completedGraphs=Object.values(state.graphs).filter(g=>g.status==='completed').length;return {version:VERSION,objects:Object.keys(state.objects).length,graphs:Object.keys(state.graphs).length,activeGraphs:Object.values(state.graphs).filter(g=>g.status==='active').length,completedGraphs,visits:visits.length,uniqueActors:uniqueActors.size,resolutions:resolves.length,propagations:propagated.length,conversion:visits.length?Number((resolves.length/visits.length).toFixed(3)):0};}
const pages={index:fs.readFileSync(path.join(process.cwd(),'public/index.html'),'utf8'),object:fs.readFileSync(path.join(process.cwd(),'public/object.html'),'utf8'),graph:fs.readFileSync(path.join(process.cwd(),'public/graph.html'),'utf8'),dashboard:fs.readFileSync(path.join(process.cwd(),'public/dashboard.html'),'utf8'),protocol:fs.readFileSync(path.join(process.cwd(),'public/protocol.html'),'utf8')};
function queueDelivery(graph,eventType,payload){if(!graph?.webhookUrl)return null;const qid=id();const q={id:qid,kind:'webhook',graphId:graph.id||payload.graphId||null,eventType,payload,attempts:0,status:'pending',nextAttemptAt:Date.now()};state.outbox[qid]=q;save();if(repo.kind==='postgres') repo.transaction(tx=>repo.putOutbox(tx,q)).catch(console.error);return qid;}
function federationDeliveriesFor(event){const allowed=new Set(['activate','resolve','propagate','complete']);if(!allowed.has(event.type))return [];return Object.values(state.peers).map(peer=>({id:id(),kind:'federation',graphId:event.graphId||null,eventType:event.type,payload:{peerId:peer.id,envelope:federationEvent(event)},status:'pending',attempts:0,nextAttemptAt:Date.now()}));}
async function processOutbox(){
 if(repo.kind==='postgres'){
  while(true){const q=await repo.claimOutbox();if(!q)break;try{if(q.kind==='federation'){const peer=state.peers[q.payload.peerId];if(!peer)throw new Error('peer not found');const r=await fetch(`${peer.url}/api/federation/ingest`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(q.payload.envelope)});if(!r.ok)throw new Error(`HTTP ${r.status}`);}else{const g=q.graphId?state.graphs[q.graphId]:null;if(!g?.webhookUrl)throw new Error('webhook target missing');const bodyText=JSON.stringify({id:q.id,type:q.eventType,createdAt:Date.now(),data:q.payload});const headers={'content-type':'application/json','x-internet-object-event':q.eventType,'x-internet-object-delivery':q.id};if(g.webhookSecret)headers['x-internet-object-signature']=signPayload(g.webhookSecret,bodyText);const r=await fetch(g.webhookUrl,{method:'POST',headers,body:bodyText});if(!r.ok)throw new Error(`HTTP ${r.status}`)}await repo.finishOutbox(q,{ok:true});}catch(e){await repo.finishOutbox(q,{ok:false,error:e.message})}}
 }else{
  for(const q of Object.values(state.outbox)){if(q.status!=='pending'||q.nextAttemptAt>Date.now())continue;q.attempts++;try{let r;if(q.kind==='federation'){const peer=state.peers[q.payload.peerId];if(!peer)throw new Error('peer not found');r=await fetch(`${peer.url}/api/federation/ingest`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(q.payload.envelope)});}else{const g=q.graphId?state.graphs[q.graphId]:null;if(!g?.webhookUrl)throw new Error('webhook target missing');const bodyText=JSON.stringify({id:q.id,type:q.eventType,createdAt:Date.now(),data:q.payload});const headers={'content-type':'application/json','x-internet-object-event':q.eventType,'x-internet-object-delivery':q.id};if(g.webhookSecret)headers['x-internet-object-signature']=signPayload(g.webhookSecret,bodyText);r=await fetch(g.webhookUrl,{method:'POST',headers,body:bodyText})}if(r.ok){q.status='delivered';q.completedAt=Date.now()}else throw new Error(`HTTP ${r.status}`)}catch(e){q.error=e.message;if(q.attempts>=5){q.status='failed';q.completedAt=Date.now()}else q.nextAttemptAt=Date.now()+Math.min(300000,1000*2**(q.attempts-1))}save()}
 }
}
setInterval(()=>processOutbox().catch(()=>{}),1000).unref();
function federationManifest(){return {...protocolManifest(),issuer:federationIssuer(),publicKey:federationPublicKeyPem(),signatureAlgorithm:'Ed25519',publishedAt:Date.now()};}
function federationEvent(event){const safeEvent={...event};delete safeEvent.token;delete safeEvent.inviteToken;delete safeEvent.signature;const payload={protocol:'internet-objects',version:VERSION,issuer:federationIssuer(),event:safeEvent};return {...payload,signature:signFederationEnvelope(payload),signatureAlgorithm:'Ed25519',publicKey:federationPublicKeyPem()};}

const handler=async(req,res)=>{try{const u=new URL(req.url,'http://localhost');
 if(!rateOk(req))return json(res,429,{error:'rate_limited',retryAfterMs:RATE_WINDOW});
 if(req.method==='OPTIONS'){res.writeHead(204,{'access-control-allow-origin':'*','access-control-allow-methods':'GET,POST,OPTIONS','access-control-allow-headers':'content-type'});return res.end()}
 if(req.method==='GET'&&u.pathname==='/')return res.writeHead(200,{'content-type':'text/html'}),res.end(pages.index);
 if(req.method==='GET'&&u.pathname==='/protocol')return res.writeHead(200,{'content-type':'text/html'}),res.end(pages.protocol);
 if(req.method==='GET'&&u.pathname==='/dashboard')return res.writeHead(200,{'content-type':'text/html'}),res.end(pages.dashboard);
 if(req.method==='GET'&&u.pathname.startsWith('/r/')){const raw=u.pathname.split('/')[2],inv=parseCapability(raw);if(!inv)return res.writeHead(404),res.end('Invite not found');const o=state.objects[inv.objectId];if(!o)return res.writeHead(410),res.end('State no longer exists');inv.uses++;inv.usedAt=Date.now();save();event('invite_open',{token:inv.token,objectId:o.id,actorId:inv.actorId,uses:inv.uses});return res.writeHead(302,{location:`/o/${o.id}?actor=${encodeURIComponent(inv.actorId)}&invite=${encodeURIComponent(inv.token)}`}),res.end()}
 if(req.method==='GET'&&u.pathname.startsWith('/o/')){const oid=u.pathname.split('/')[2],o=state.objects[oid];if(!o)return res.writeHead(404),res.end('Object not found');o.visited++;event('visit',{objectId:oid,graphId:o.graphId,actorId:u.searchParams.get('actor')||o.actorId,ref:req.headers.referer||null});return res.writeHead(200,{'content-type':'text/html'}),res.end(pages.object)}
 if(req.method==='GET'&&u.pathname.startsWith('/g/'))return res.writeHead(200,{'content-type':'text/html'}),res.end(pages.graph);
 if(req.method==='GET'&&u.pathname==='/api/health')return json(res,200,{ok:true,...stats(),ai:!!(process.env.AI_API_KEY||process.env.OPENAI_API_KEY)});
 if(req.method==='GET'&&u.pathname==='/api/ready'){try{await repo.ready();return json(res,200,{ok:true,version:VERSION,storage:repo.kind})}catch(e){return json(res,503,{ok:false,storage:repo.kind,error:e.message})}}
 if(req.method==='GET'&&u.pathname==='/api/stats')return json(res,200,{...stats(),pendingObjects:Object.values(state.objects).filter(o=>o.state==='pending').length,inviteConversion:(Object.keys(state.invites).length?Number((state.events.filter(e=>e.type==='resolve').length/Math.max(1,state.events.filter(e=>e.type==='invite_open').length)).toFixed(3)):0),invites:Object.keys(state.invites).length,inviteOpens:state.events.filter(e=>e.type==='invite_open').length,webhookDeliveries:Object.keys(state.deliveries).length,outbox:Object.values(state.outbox).filter(x=>x.status==='pending').length,receipts:Object.keys(state.receipts).length});
 if(req.method==='GET'&&u.pathname==='/api/events')return json(res,200,{events:state.events.slice(-200)});
 if(req.method==='GET'&&u.pathname==='/api/discover')return json(res,200,{protocol:protocolManifest(),objects:discovery()});
 if(req.method==='GET'&&u.pathname==='/api/protocol')return json(res,200,protocolManifest());
 if(req.method==='GET'&&u.pathname==='/.well-known/internet-objects')return json(res,200,federationManifest());
 if(req.method==='GET'&&u.pathname==='/.well-known/internet-objects/keys')return json(res,200,{issuer:federationIssuer(),algorithm:'Ed25519',publicKey:federationPublicKeyPem()});
 if(req.method==='GET'&&u.pathname==='/api/outbox')return json(res,200,{items:Object.values(state.outbox).slice(-100).reverse()});
 if(req.method==='GET'&&u.pathname==='/api/peers')return json(res,200,{peers:Object.values(state.peers)});
 if(req.method==='POST'&&u.pathname==='/api/federation/register'){const b=await body(req);if(!b.url||!String(b.url).startsWith('http'))return json(res,400,{error:'url is required'});const url=String(b.url).replace(/\/$/,'');let remote;try{const r=await fetch(`${url}/.well-known/internet-objects`);if(!r.ok)throw Error(`manifest HTTP ${r.status}`);remote=await r.json();}catch(e){return json(res,400,{error:`peer manifest unavailable: ${e.message}`});}if(!remote.publicKey||!remote.issuer)return json(res,400,{error:'peer manifest must contain issuer and publicKey'});if(b.publicKey&&String(b.publicKey).trim()!==String(remote.publicKey).trim())return json(res,400,{error:'supplied publicKey does not match peer manifest'});const pid=id();state.peers[pid]={id:pid,url,name:b.name||null,issuer:remote.issuer,publicKey:remote.publicKey,createdAt:Date.now(),lastSeenAt:Date.now(),payload:{id:pid,url,name:b.name||null,issuer:remote.issuer,publicKey:remote.publicKey}};if(repo.kind==='postgres')await repo.transaction(tx=>repo.putPeer(tx,state.peers[pid]));else save();return json(res,201,{peer:state.peers[pid]});}
 if(req.method==='POST'&&u.pathname==='/api/federation/ingest'){const b=await body(req);if(!b.event||b.protocol!=='internet-objects'||b.signatureAlgorithm!=='Ed25519'||!b.signature||!b.issuer)return json(res,400,{error:'invalid signed federation event'});const peer=Object.values(state.peers).find(p=>p.issuer===b.issuer);if(!peer)return json(res,403,{error:'unknown federation issuer'});const unsigned={protocol:b.protocol,version:b.version,issuer:b.issuer,event:b.event};if(!verifyFederationEnvelope(unsigned,b.signature,peer.publicKey))return json(res,401,{error:'invalid federation signature'});if(!b.event.id||!Number.isFinite(Number(b.event.at)))return json(res,400,{error:'federation event requires id and at'});if(Math.abs(Date.now()-Number(b.event.at))>FEDERATION_MAX_SKEW_MS)return json(res,409,{error:'federation event outside replay window'});if(repo.kind==='postgres'){const result=await repo.transaction(async tx=>{const prior=await tx.query('select issuer,event_id from io_federation_events where issuer=$1 and event_id=$2',[b.issuer,b.event.id]);if(prior.rows[0])return {duplicate:true};await tx.query('insert into io_federation_events(issuer,event_id,received_at) values($1,$2,now())',[b.issuer,b.event.id]);const eid=id();const e={id:eid,type:'federated_event',at:Date.now(),issuer:b.issuer,remoteEvent:b.event,verified:true};await tx.insertEvent(e);await repo.putPeer(tx,{...peer,lastSeenAt:Date.now()});return {duplicate:false,eventId:eid}});if(result.duplicate)return json(res,202,{accepted:true,verified:true,duplicate:true,eventId:b.event.id});return json(res,202,{accepted:true,verified:true,eventId:result.eventId});}const key=`${b.issuer}:${b.event.id}`;if(state.federationEvents[key])return json(res,202,{accepted:true,verified:true,duplicate:true,eventId:b.event.id});state.federationEvents[key]={issuer:b.issuer,eventId:b.event.id,receivedAt:Date.now()};const eid=id();state.events.push({id:eid,type:'federated_event',at:Date.now(),issuer:b.issuer,remoteEvent:b.event,verified:true});peer.lastSeenAt=Date.now();save();return json(res,202,{accepted:true,verified:true,eventId:eid});}
 if(req.method==='GET'&&u.pathname==='/api/receipts'){return json(res,200,{receipts:Object.values(state.receipts).slice(-100).reverse()})}
 if(req.method==='GET'&&u.pathname.startsWith('/api/receipts/')){const rid=u.pathname.split('/')[3],r=state.receipts[rid];return r?json(res,200,{receipt:r}):json(res,404,{error:'receipt not found'})}
 if(req.method==='GET'&&u.pathname==='/api/objects'){return json(res,200,{objects:Object.values(state.objects).sort((a,b)=>b.createdAt-a.createdAt).slice(0,100).map(publicObject)})}
 let m=u.pathname.match(/^\/api\/objects\/([^/]+)$/);if(req.method==='GET'&&m){const o=state.objects[m[1]];return o?json(res,200,{object:publicObject(o)}):json(res,404,{error:'object not found'})}
 if(req.method==='POST'&&u.pathname==='/api/compile'){const b=await body(req),prompt=String(b.prompt||'').trim();if(!prompt)return json(res,400,{error:'prompt is required'});const idem=req.headers['idempotency-key'];if(idem&&repo.kind==='postgres'){const prior=await repo.idempotency('compile',String(idem),null);if(prior)return json(res,200,prior);}let g=null;try{g=await compileWithAI(prompt)}catch{}if(!g)g=compileDeterministic(prompt);g.discoverable=b.discoverable!==false;g.webhookUrl=typeof b.webhookUrl==='string'&&b.webhookUrl.startsWith('http')?b.webhookUrl:null;g.webhookSecret=typeof b.webhookSecret==='string'?b.webhookSecret:null;const gid=id();g.id=gid;g.status='draft';g.createdAt=Date.now();state.graphs[gid]=g;save();event('compile',{graphId:gid,ai:g.version.endsWith('-ai')});await mirror();const response={graph:g,graphId:gid,url:`/g/${gid}`,compiler:g.version.endsWith('-ai')?'ai':'deterministic'};if(idem&&repo.kind==='postgres')await repo.idempotency('compile',String(idem),response);return json(res,201,response);}
 m=u.pathname.match(/^\/api\/graphs\/([^/]+)$/);if(req.method==='GET'&&m){const g=state.graphs[m[1]];return g?json(res,200,{graph:g,rootObject:g.rootObjectId?publicObject(state.objects[g.rootObjectId]):null}):json(res,404,{error:'graph not found'})}
 m=u.pathname.match(/^\/api\/graphs\/([^/]+)\/activate$/);if(req.method==='POST'&&m){const o=activate(m[1]);return o?json(res,201,{object:publicObject(o),publicUrl:`${baseUrl()}${o.inviteToken?inviteUrl(o.inviteToken):`/o/${o.id}`}`}):json(res,404,{error:'graph not found'})}
 if(req.method==='POST'&&u.pathname==='/api/objects'){const b=await body(req),choices=Array.isArray(b.choices)?b.choices.filter(x=>typeof x==='string'&&x.trim()).slice(0,8):[];if(!b.title||!b.description||choices.length<2)return json(res,400,{error:'title, description and at least 2 choices are required'});const oid=id();const o={id:oid,title:String(b.title).trim(),description:String(b.description).trim(),choices,state:'pending',createdAt:Date.now(),visited:0,resolvedAt:null,resolvedBy:null,answer:null,parentObjectId:b.parentObjectId||null,childObjectId:null};state.objects[oid]=o;event('create',{objectId:oid});await mirror();return json(res,201,{object:publicObject(o)});}
 m=u.pathname.match(/^\/api\/objects\/([^/]+)$/);if(req.method==='POST'&&m){
  const o=state.objects[m[1]];if(!o)return json(res,404,{error:'object not found'});
  if(o.state==='resolved')return json(res,200,{object:publicObject(o),nextObject:o.childObjectId?publicObject(state.objects[o.childObjectId]):null});
  const b=await body(req);if(!o.choices.includes(b.answer))return json(res,400,{error:'invalid choice'});
  const actorId=b.actorId||o.actorId;
  const nextIndex=o.actorIndex==null?null:o.actorIndex+1;
  let g=o.graphId?state.graphs[o.graphId]:null;
  const idem=req.headers['idempotency-key'];
  const updated={...o,state:'resolved',answer:b.answer,resolvedAt:Date.now(),resolvedBy:actorId};
  let next=null,invite=null,receipt=null;
  const events=[];
  const federationDeliveries=[];
  if(g&&nextIndex!==null&&nextIndex<g.actors.length){
    next=graphObject(o.graphId,g,nextIndex,o.id); // detached until commit
    delete state.objects[next.id];
    const token=inviteToken();invite={token,objectId:next.id,actorId:next.actorId,createdAt:Date.now(),expiresAt:Date.now()+INVITE_TTL_MS,usedAt:null,uses:0,maxUses:INVITE_MAX_USES,signature:capabilitySignature(token)};
    next.inviteToken=token;updated.childObjectId=next.id;
    events.push({id:id(),type:'resolve',at:Date.now(),objectId:o.id,graphId:o.graphId,actorId,answer:b.answer});
    events.push({id:id(),type:'invite_created',at:Date.now(),token,objectId:next.id,actorId:next.actorId});
    events.push({id:id(),type:'propagate',at:Date.now(),graphId:o.graphId,parentObjectId:o.id,objectId:next.id,actorId:next.actorId,chainIndex:next.actorIndex,delivery:g.webhookUrl?{id:id(),kind:'webhook',graphId:g.id,eventType:'object.propagated',payload:{graphId:g.id,parentObjectId:o.id,objectId:next.id,actorId:next.actorId,publicUrl:`${baseUrl()}${inviteUrl(token)}`},attempts:0,status:'pending',nextAttemptAt:Date.now()}:null});
    if(g.webhookUrl)events.push({id:id(),type:'object.resolved',at:Date.now(),objectId:o.id,graphId:g.id,actorId,delivery:{id:id(),kind:'webhook',graphId:g.id,eventType:'object.resolved',payload:{graphId:g.id,objectId:o.id,actorId,answer:b.answer},attempts:0,status:'pending',nextAttemptAt:Date.now()}});
  } else if(g){
    const completedGraph={...g,status:'completed',completedAt:Date.now()};
    const all=Object.values(state.objects).filter(x=>x.graphId===g.id&&x.id!==o.id).map(x=>({id:x.id,actorId:x.actorId,answer:x.answer,state:x.state}));
    all.push({id:o.id,actorId:o.actorId,answer:b.answer,state:'resolved'});
    receipt={id:id(),graphId:g.id,status:'completed',issuedAt:Date.now(),objects:all};
    events.push({id:id(),type:'resolve',at:Date.now(),objectId:o.id,graphId:g.id,actorId,answer:b.answer});
    events.push({id:id(),type:'complete',at:Date.now(),graphId:g.id,delivery:g.webhookUrl?{id:id(),kind:'webhook',graphId:g.id,eventType:'graph.completed',payload:{graphId:g.id,receiptId:receipt.id,receiptUrl:`${baseUrl()}/api/receipts/${receipt.id}`},attempts:0,status:'pending',nextAttemptAt:Date.now()}:null});
    g={...completedGraph};
  } else {
    events.push({id:id(),type:'resolve',at:Date.now(),objectId:o.id,graphId:null,actorId,answer:b.answer});
  }
  for(const e of events) federationDeliveries.push(...federationDeliveriesFor(e));
  if(repo.kind==='postgres'){
    const response={object:publicObject(updated),nextObject:next?publicObject(next):null,completed:!!(g&&g.status==='completed'),receipt};
    const result=await repo.atomicResolve({object:updated,graph:g,child:next,invite,events,completedReceipt:receipt,federationDeliveries,idempotencyKey:idem?{scope:`resolve:${o.id}`,key:String(idem)}:null,idempotencyResponse:response});
    if(result.duplicate&&result.payload)return json(res,200,result.payload);
    state.objects[o.id]=updated;if(next)state.objects[next.id]=next;if(g)state.graphs[g.id]=g;if(invite)state.invites[invite.token]=invite;if(receipt)state.receipts[receipt.id]=receipt;
    for(const e of events){state.events.push({...e});if(e.delivery){state.outbox[e.delivery.id]=e.delivery;}}
    return json(res,200,response);
  }
  Object.assign(o,updated);if(next){state.objects[next.id]=next;state.invites[invite.token]=invite;o.childObjectId=next.id;}if(g&&g.status==='completed')state.graphs[g.id]=g;if(receipt)state.receipts[receipt.id]=receipt;for(const e of events){const delivery=e.delivery;delete e.delivery;state.events.push(e);if(delivery)state.outbox[delivery.id]=delivery;if(e.type==='propagate'&&!delivery)queueDelivery(g,'object.propagated',{graphId:g.id,parentObjectId:o.id,objectId:next.id,actorId:next.actorId,publicUrl:`${baseUrl()}${inviteUrl(invite.token)}`});if(e.type==='complete'&&!delivery)queueDelivery(g,'graph.completed',{graphId:g.id,receiptId:receipt.id,receiptUrl:`${baseUrl()}/api/receipts/${receipt.id}`});}for(const d of federationDeliveries)state.outbox[d.id]=d;save();return json(res,200,{object:publicObject(o),nextObject:next?publicObject(next):null,completed:!!(g&&g.status==='completed'),receipt});
 }
 res.writeHead(404);res.end('Not found');}catch(e){json(res,500,{error:e.message||'internal error'})}};

export default handler;

if (process.env.VERCEL !== '1') {
  http.createServer(handler).listen(process.env.PORT||3000,()=>console.log(`Internet Objects ${VERSION} listening on http://localhost:${process.env.PORT||3000}`));
}
