import 'dotenv/config';
import {chromium} from 'playwright';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import './src/core.js';
import './src/availability.js';
import './src/presence_transport.js';
const C=globalThis.AmazonDetectorCore,A=globalThis.AmazonDetectorAvailability,P=globalThis.AmazonDetectorPresenceTransport;
const dir=path.dirname(fileURLToPath(import.meta.url));
const env=process.env, port=Number(env.PORT||5002),host=env.BIND_HOST||'127.0.0.1';
const keyDir=path.resolve(dir,env.KEYS_DIR||'../AmazonJobsKeys');
const id=env.DETECTOR_ID||'detector-01';
if(!C.detectorIdValid(id))throw Error('Invalid detector ID');
const credential=fs.readFileSync(path.join(keyDir,`${id}.key`),'utf8').trim();
if(credential.length<12)throw Error('Invalid detector credential');
const base=(env.ORACLE_BASE||'https://145-241-233-33.sslip.io').replace(/\/$/,'');
const stateDir=path.join(dir,'.runtime');fs.mkdirSync(stateDir,{recursive:true,mode:0o700});
const stateFile=path.join(stateDir,`${id}.json`);
let stored={seq:1,cache:{},queue:[],presence:{},audit:[],scans:[]};
try{stored={...stored,...JSON.parse(fs.readFileSync(stateFile,'utf8'))};}catch{}
const save=()=>{const tmp=stateFile+'.tmp';fs.writeFileSync(tmp,JSON.stringify(stored),{mode:0o600});fs.renameSync(tmp,stateFile);};
const status={id,startedAt:new Date().toISOString(),browser:'starting',profile:'pending',lastScan:null,visible:0,reportable:0,security:null,lastError:null,lastHTTP:null};
let context,page,profile,profileAt=0,flushBusy=false,paused=false,refreshTimer;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function post(route,body){const c=new AbortController(),t=setTimeout(()=>c.abort(),9000);try{return await fetch(base+route,{method:'POST',headers:{Authorization:`Bearer ${credential}`,'Content-Type':'application/json'},body:JSON.stringify(body),signal:c.signal});}finally{clearTimeout(t);}}
async function getProfile(){const c=new AbortController(),t=setTimeout(()=>c.abort(),8000);try{const res=await fetch(base+'/v1/detectors/profile',{headers:{Authorization:`Bearer ${credential}`},signal:c.signal});if(!res.ok)throw Error(`Oracle profile HTTP ${res.status}`);profile=C.validateProfile(await res.json());if(profile.detector_id!==id)throw Error('Oracle profile detector mismatch');profileAt=Date.now();status.profile='validated';}finally{clearTimeout(t);}}
async function heartbeat(){try{const state=status.security==='captcha'||status.security==='access_restricted'?status.security:'ok';let res=await post('/v1/detectors/heartbeat',state==='ok'?{state}:{state,recent_error:state});status.heartbeatHTTP=res.status;if(res.status===401||res.status===403)paused=true;}catch(e){status.lastError='heartbeat: '+e.message;}}
async function flush(){if(flushBusy||paused)return;flushBusy=true;try{while(stored.queue.length){const batch=stored.queue.slice(0,8);const response=await post('/v1/detectors/observations',{events:batch});status.lastHTTP=response.status;if(response.status===401||response.status===403){paused=true;break;}if(response.status!==202)break;let json;try{json=await response.json();}catch{break;}const results=[json?.results,json?.events,json?.statuses,json?.outcomes].find(Array.isArray);if(!results||results.length!==batch.length)break;const byId=new Map(results.filter(v=>v&&typeof v==='object'&&v.event_id).map(v=>[v.event_id,v]));const statuses=batch.map((e,i)=>{const v=byId.get(e.event_id)||results[i];return typeof v==='string'?v:(v?.status||v?.result||v?.outcome||'unknown');});const outcome=C.observationOutcomes(statuses);if(!outcome)break; // conservative: no ambiguous ACKs
// Keep retriable events; drop only accepted/known duplicate/rejected events according to validated Oracle outcomes.
const resolved= new Set([...outcome.accepted,...outcome.rejected].filter(Number.isInteger));
if(resolved.size===0)break;
stored.queue=stored.queue.filter((_,i)=>i>=batch.length||!resolved.has(i));save();
if(resolved.size!==batch.length)break;
}}catch(e){status.lastError='observation delivery: '+e.message;}finally{flushBusy=false;}}
async function handle(msg){if(msg?.type==='DETECTOR_PAGE_READY')return {active:!!profile&&!paused,profile};if(msg?.type==='DETECTOR_BLOCKED'){status.security=msg.reason;paused=true;await heartbeat();return {ok:true};}if(msg?.type==='DETECTOR_DIAGNOSTIC'){status.lastError=`scan: ${msg.reason}`;return {ok:true};}if(msg?.type!=='DETECTOR_SNAPSHOT')return {ok:true};status.lastScan=msg.observed_at;status.visible=msg.visible_count;status.reportable=msg.jobs?.length||0;for(const raw of msg.jobs||[]){const job=C.canonicalJob(raw);if(!job)continue;const stable=C.stableKey(job),hash=JSON.stringify([stable,C.versionText(job)]);if(stored.cache[stable]===hash)continue;if(stored.queue.length>=350){paused=true;status.lastError='Queue overflow';break;}const seq=C.nextObservationSequence(stored.seq);stored.seq=seq+1;stored.queue.push({event_type:'job_card_observed',event_id:crypto.randomUUID(),detector_id:id,sequence:seq,observed_at:msg.observed_at,render_complete:true,job});stored.cache[stable]=hash;}
// Match original Stage 5E local presence evidence.
try{const scope=await A.scopeFingerprint(msg.page_url);const ctx={detectorId:id,observedAt:msg.observed_at,scope,refreshCycleId:msg.scan?.cycle_id||'',scanComplete:msg.scan?.complete===true,source:msg.scan?.source||'page_scan'};const jobs=(msg.jobs||[]).map(C.canonicalJob).filter(Boolean);const evidence=A.transitions(stored.presence,jobs,ctx,{...msg.scan,scope});stored.presence=evidence.presence;for(const e of evidence.events){e.content_fingerprint=e.job?await A.hexDigest(C.versionText(e.job)):'';e.previous_content_fingerprint=e.previous_content?await A.hexDigest(C.versionText(e.previous_content)):'';}stored.audit.push(...evidence.events);stored.scans.push({...A.scanSummary(ctx,{...msg.scan,scope},{visible:msg.visible_count,reportable:jobs.length,identities:jobs.map(C.stableKey)},evidence.events),scan_id:crypto.randomUUID()});stored.audit=stored.audit.slice(-600);stored.scans=stored.scans.slice(-120);}catch(e){status.lastError='presence: '+e.message;}save();void flush();return {ok:true};}
async function flushPresence(){if(!stored.audit.length&&!stored.scans.length)return;try{const b=P.batch(id,stored.audit,stored.scans);if(!b)return;const res=await post('/v1/detectors/presence',b.payload);status.presenceHTTP=res.status;if(res.ok){stored.audit=stored.audit.filter(e=>!b.eventIds.includes(e.event_id));stored.scans=stored.scans.filter(s=>!b.scanIds.includes(s.scan_id));save();}}catch(e){status.lastError='presence: '+e.message;}}
async function loadPage(){await page.goto(env.JOB_URL||'https://www.jobsatamazon.co.uk/app#/jobSearch',{waitUntil:'domcontentloaded',timeout:Number(env.NAVIGATION_TIMEOUT_MS||30000)});}
async function installScripts(){for(const src of ['core.js','safety.js','availability.js','content.js'])await page.addScriptTag({path:path.join(dir,'src',src)});}
function scheduleRefresh(){clearTimeout(refreshTimer);if(!profile||paused)return;const next=C.nextScheduledRefreshMs(Date.now(),profile);if(!next)return;refreshTimer=setTimeout(async()=>{try{if(paused)return;await loadPage();await installScripts();status.browser='monitoring';}catch(e){status.lastError='navigation: '+e.message;}finally{scheduleRefresh();}},Math.max(100,next-Date.now()));}
const server=http.createServer((req,res)=>{if(req.url!=='/health'&&req.url!=='/status'){res.writeHead(404).end();return;}res.writeHead(200,{'content-type':'application/json','cache-control':'no-store'}).end(JSON.stringify({...status,paused,queue:stored.queue.length,profileAgeMs:Date.now()-profileAt}));});
async function main(){await getProfile();context=await chromium.launchPersistentContext(path.resolve(dir,env.PROFILE_DIR||'.browser-profile'),{headless:env.HEADLESS!=='false',args:['--no-sandbox']});page=context.pages()[0]||await context.newPage();await page.exposeBinding('__nodeDetectorMessage',(_,msg)=>handle(msg));await context.addInitScript(()=>{globalThis.chrome={runtime:{id:'node-detector',sendMessage:(msg)=>globalThis.__nodeDetectorMessage(msg),onMessage:{addListener(cb){globalThis.__detectorListener=cb;}}}};});await loadPage();await installScripts();status.browser='monitoring';scheduleRefresh();setInterval(()=>{if(Date.now()-profileAt>15*60*1000)getProfile().catch(e=>{status.profile='error';status.lastError=e.message;});},60000).unref();setInterval(()=>{void flush();void flushPresence();void heartbeat();},30000).unref();await heartbeat();}
server.listen(port,host,()=>console.log(`Detector health http://${host}:${port}/health`));main().catch(e=>{status.browser='error';status.lastError=e.message;console.error(e);process.exitCode=1;});
for(const sig of ['SIGTERM','SIGINT'])process.on(sig,async()=>{clearTimeout(refreshTimer);save();server.close();await context?.close();process.exit(0);});
