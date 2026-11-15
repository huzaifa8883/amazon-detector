/* Amazon Jobs Signal Detector v0.5 — trusted Oracle client; no credential exposure to pages. */
importScripts('core.js');
importScripts('availability.js');
importScripts('presence_transport.js');
const C=globalThis.AmazonDetectorCore,A=globalThis.AmazonDetectorAvailability,P=globalThis.AmazonDetectorPresenceTransport;
const STORE='detectorV2Store',TOKEN='detectorV2Token';
const SEND_ALARM='detector-v2-send',HEARTBEAT_ALARM='detector-v2-heartbeat',PROFILE_ALARM='detector-v2-profile',LEGACY_REFRESH_ALARM='detector-v2-refresh';
const MAX_QUEUE=350,MAX_CACHE=1800,BATCH_LIMIT=8,BODY_LIMIT=131072;
const DEFAULT={
  config:{profile:null,profileFetchedAt:null,previousDetectorId:''},
  run:{enabled:false,paused:false,targetTabId:null,status:'stopped',sessionId:'',lastError:'',pageState:'not_open',
    lastScanAt:null,lastCardCount:0,lastReportableCount:0,lastUnidentifiedCount:0,lastAmbiguousCount:0,lastRawCandidateCount:0,lastParsedCount:0,
    lastRejectedReasons:{},lastAdvertisedCount:null,lastDeliveryAt:null,lastHeartbeatAt:null,lastHttpStatus:null,
    lastResult:'',submitted:0,changed:0,duplicateObservations:0,observationsSubmitted:0,observationsAccepted:0,observationsRejected:0,lastRetryAt:null,
    nextRefreshAt:null,lastRefreshAt:null,lastRefreshAttemptAt:null,lastRefreshTargetAt:null,
    lastRefreshDelayMs:null,refreshCount:0,refreshPending:null,profileStatus:'not_configured',
    lastMutationAt:null,lastScanComplete:false,lastScanReason:'',lastScanScope:'',lastRefreshCycleId:null,
    cardAppearances:0,cardChanges:0,cardProvisionalMissing:0,cardConfirmedMissing:0,cardReappearances:0,
    mutationEvidenceCount:0,auditOverflowCount:0,presenceScanOverflowCount:0,
    presenceLastUploadAt:null,presenceLastHttpStatus:null,presenceLastError:'',
    presenceEventsUploaded:0,presenceScansUploaded:0,presenceNextRetryAt:0,
    safetySignalStatus:'none',safetySignalReason:'',safetySignalAt:null,safetySignalAckAt:null,
    safetySignalHttpStatus:null,safetySignalLastError:'',lastHeartbeatState:null,lastHeartbeatHttpStatus:null},
  db:{nextSequence:1,nextAuditSequence:1,queue:[],rejectedObservations:[],cache:{},presence:{},auditQueue:[],scanSummaries:[]}
};
let chain=Promise.resolve(),sending=false,heartbeating=false,profileRefreshing=false,presenceSending=false;
let pendingHeartbeat=null;
function ordered(fn){const p=chain.then(fn,fn);chain=p.catch(()=>{});return p;}
function copy(v){return JSON.parse(JSON.stringify(v));}
async function readStore(){
  const obj=(await chrome.storage.local.get(STORE))[STORE];
  if(!obj){
    const legacy=await chrome.storage.local.get(['detectorState','detectorOutbox']);
    const s=copy(DEFAULT);
    if(legacy.detectorState){
      s.config.previousDetectorId=C.detectorIdValid(legacy.detectorState.detectorId)?legacy.detectorState.detectorId:'';
      s.run.status='needs_oracle_profile';s.run.lastError='Previous local schedule removed. Enter your assigned API key to obtain the Oracle profile.';
    }
    await chrome.storage.local.set({[STORE]:s});return s;
  }
  const s={config:{...DEFAULT.config},run:{...DEFAULT.run,...(obj.run||{})},
    db:{...DEFAULT.db,...(obj.db||{}),queue:Array.isArray(obj.db?.queue)?obj.db.queue:[],
      rejectedObservations:Array.isArray(obj.db?.rejectedObservations)?obj.db.rejectedObservations:[],
      cache:obj.db?.cache||{},presence:obj.db?.presence||{},
      auditQueue:Array.isArray(obj.db?.auditQueue)?obj.db.auditQueue:[],
      scanSummaries:Array.isArray(obj.db?.scanSummaries)?obj.db.scanSummaries:[]}};
  if(obj.config?.profile){
    try{s.config.profile=C.validateProfile(obj.config.profile);s.config.profileFetchedAt=obj.config.profileFetchedAt||null;}
    catch(_){s.config.profile=null;}
  }else if(obj.config?.detectorId&&C.detectorIdValid(obj.config.detectorId)){
    s.config.previousDetectorId=obj.config.detectorId;
  }
  if(!s.config.profile && obj.config && !Object.prototype.hasOwnProperty.call(obj.config,'profile')){
    // One-time migration: reject former local schedules, retain event history.
    s.run.enabled=false;s.run.paused=false;s.run.nextRefreshAt=null;
    s.run.status='needs_oracle_profile';s.run.profileStatus='not_configured';
  }
  return s;
}
async function writeStore(s){await chrome.storage.local.set({[STORE]:s});}
async function token(){return (await chrome.storage.local.get(TOKEN))[TOKEN]||'';}
function hash(job){return JSON.stringify([C.stableKey(job),C.versionText(job)]);}
function bodySize(events){return new TextEncoder().encode(JSON.stringify({events})).length;}
function retryDelay(attempt,header){
  if(header){const secs=Number(header);if(Number.isFinite(secs)&&secs>=0)return Math.max(1000,Math.min(3600000,secs*1000));
    const date=Date.parse(header);if(Number.isFinite(date))return Math.max(1000,Math.min(3600000,date-Date.now()));}
  return Math.min(3600000,2000*2**Math.min(9,attempt))*(0.85+Math.random()*0.3);
}
function currentProfile(s){return s.config.profile;}
function profileValid(s){return !!currentProfile(s)&&C.profileFresh(currentProfile(s),s.config.profileFetchedAt);}

async function syncAlarms(s){
  await Promise.all([chrome.alarms.clear(SEND_ALARM),chrome.alarms.clear(HEARTBEAT_ALARM),chrome.alarms.clear(PROFILE_ALARM),chrome.alarms.clear(LEGACY_REFRESH_ALARM)]);
  if(s.run.enabled){
    await chrome.alarms.create(SEND_ALARM,{periodInMinutes:0.5});
    await chrome.alarms.create(HEARTBEAT_ALARM,{periodInMinutes:0.5});
    await chrome.alarms.create(PROFILE_ALARM,{periodInMinutes:15});
  }
  s.run.nextRefreshAt=null;
  await writeStore(s);
}
async function publicStatus(){
  const s=await readStore(),p=currentProfile(s),credential=!!await token();
  const age=p&&Number.isFinite(s.config.profileFetchedAt)?Date.now()-s.config.profileFetchedAt:null;
  return {...s.run,detectorId:p?.detector_id||'',assignedSeconds:p?.schedule.seconds_of_minute||null,
    profileRevision:p?.revision||null,configured:credential&&profileValid(s),hasToken:credential,
    profileStatus:p?(age>C.PROFILE_MAX_AGE_MS?'expired':age>C.PROFILE_REFRESH_MS?'refresh_due':s.run.profileStatus||'validated'):'not_configured',
    queueLength:s.db.queue.length,nextSequence:s.db.nextSequence,rejectedObservations:s.db.rejectedObservations.length,
    auditPending:s.db.auditQueue.length,scanHistoryCount:s.db.scanSummaries.length,
    auditMode:'oracle_presence_evidence',appearanceTracking:'oracle_evidence_not_automatically_signalled'};
}
class ProfileError extends Error{constructor(message,status=0){super(message);this.status=status;}}
async function requestProfile(key){
  const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),8000);
  try{
    let response;
    try{response=await fetch(C.PROFILE,{method:'GET',headers:{Authorization:`Bearer ${key}`},
      cache:'no-store',credentials:'omit',signal:controller.signal});}
    catch(_){throw new ProfileError('Oracle profile service cannot be reached.');}
    if(response.status===401||response.status===403)throw new ProfileError('Detector API key was rejected or revoked.',response.status);
    if(response.status===404)throw new ProfileError('Oracle profile endpoint is not activated on the server (HTTP 404).',404);
    if(!response.ok)throw new ProfileError(`Oracle profile HTTP ${response.status}.`,response.status);
    let data;try{data=await response.json();}catch(_){throw new ProfileError('Oracle returned invalid profile JSON.');}
    return C.validateProfile(data);
  }finally{clearTimeout(timeout);}
}
function checkIdentity(s,profile){
  const old=currentProfile(s)?.detector_id||s.config.previousDetectorId;
  if(old&&old!==profile.detector_id&&(s.db.queue.length||s.db.nextSequence>1))
    throw new Error('This Chrome profile contains another detector\'s event history. Use a separate Chrome profile to avoid sequence conflicts.');
}
async function configure(msg){
  const s=await readStore();if(s.run.enabled)throw Error('Stop monitoring before changing the API key.');
  const provided=String(msg.deviceToken||'').trim(),key=provided||await token();
  if(!/^[^\s]{16,512}$/.test(key))throw Error('Enter your individual Oracle detector API key (16–512 characters).');
  const p=await requestProfile(key);checkIdentity(s,p);
  await chrome.storage.local.setAccessLevel?.({accessLevel:'TRUSTED_CONTEXTS'});
  if(provided)await chrome.storage.local.set({[TOKEN]:provided});
  s.config={profile:p,profileFetchedAt:Date.now(),previousDetectorId:p.detector_id};
  s.run.profileStatus='validated';s.run.lastError='';s.run.status='connected';s.run.paused=false;
  await writeStore(s);
  return {ok:true,detectorId:p.detector_id,seconds:p.schedule.seconds_of_minute};
}
async function syncProfile(){
  if(profileRefreshing)return;
  profileRefreshing=true;
  try{
    const key=await token();if(!key)return;
    try{
      const p=await requestProfile(key);
      await ordered(async()=>{
        const s=await readStore();checkIdentity(s,p);
        if(s.config.profile&&s.config.profile.revision>p.revision)throw Error('Older Oracle profile revision rejected.');
        s.config.profile=p;s.config.previousDetectorId=p.detector_id;s.config.profileFetchedAt=Date.now();
        s.run.profileStatus='validated';s.run.lastError='';
        await writeStore(s);
        if(s.run.enabled&&!s.run.paused)void chrome.tabs.sendMessage(s.run.targetTabId,{type:'DETECTOR_SCHEDULE_UPDATE',profile:p}).catch(()=>{});
      });
    }catch(e){
      await ordered(async()=>{
        const s=await readStore();
        if(e?.status===401||e?.status===403){
          s.config.profile=null;s.run.profileStatus='revoked';s.run.paused=true;s.run.status='paused_invalid_key';
          s.run.lastError='Detector API key rejected or revoked; monitoring stopped.';
        }else if(!profileValid(s)){
          s.run.paused=true;s.run.status='paused_profile_expired';s.run.profileStatus='expired';
          s.run.lastError='Oracle profile cannot be verified and the cached profile has expired; monitoring stopped.';
        }else{
          s.run.profileStatus='using_cached_profile';s.run.lastError='Oracle profile refresh temporarily unavailable; using recently verified cached profile.';
        }
        await writeStore(s);
        if(s.run.paused){await syncAlarms(s);if(s.run.targetTabId!=null)void chrome.tabs.sendMessage(s.run.targetTabId,{type:'DETECTOR_STOP_OBSERVE'}).catch(()=>{});}
      });
    }
  }finally{profileRefreshing=false;}
}
async function start(msg){
  let s=await readStore();if(!await token()||!currentProfile(s))throw Error('Paste the API key and click Save & Connect first.');
  if(!profileValid(s))throw Error('Oracle profile expired. Click Save & Connect to revalidate before starting.');
  if(Date.now()-s.config.profileFetchedAt>=C.PROFILE_REFRESH_MS){
    try{
      const fresh=await requestProfile(await token());checkIdentity(s,fresh);
      if(fresh.revision<currentProfile(s).revision)throw Error('Oracle returned an older profile revision.');
      s.config.profile=fresh;s.config.profileFetchedAt=Date.now();s.run.profileStatus='validated';
    }catch(e){
      if(e?.status===401||e?.status===403){
        s.config.profile=null;s.run.profileStatus='revoked';s.run.status='paused_invalid_key';s.run.lastError='Oracle rejected this detector key.';
        await writeStore(s);throw Error('Detector key is revoked or invalid. Save & Connect with a valid key.');
      }
      // Temporarily unavailable profiles may be used only for a bounded period.
      s.run.profileStatus='using_cached_profile';
    }
  }
  if(!Number.isInteger(msg.tabId))throw Error('Open a public Amazon Jobs listing tab.');
  const tab=await chrome.tabs.get(msg.tabId);
  if(!C.isAmazonPageUrl(tab.url||''))throw Error('Choose a supported public Amazon Jobs page.');
  if(C.isApplicationUrl(tab.url||''))throw Error('Do not start monitoring on an application page. Open the public job listings.');
  if(tab.status!=='complete')throw Error('Wait until the Amazon Jobs listings page finishes loading, then start monitoring.');
  // Passive mode: never reload the target tab. Verify the existing content
  // script, then begin observing the document already displayed to the user.
  let safety;
  try { safety=await chrome.tabs.sendMessage(tab.id,{type:'DETECTOR_SAFETY_CHECK'}); }
  catch (_) { throw Error('Content observer unavailable. Manually open/reload the Amazon Jobs page once, then start again.'); }
  if(safety?.safe!==true)throw Error(safety?.safe===false?
    'Amazon security verification or access restriction detected. Monitoring did not start.':
    'Cannot verify page safety. Monitoring did not start.');
  s.run={...s.run,enabled:true,paused:false,targetTabId:tab.id,status:'monitoring',sessionId:crypto.randomUUID(),
    lastError:'',pageState:'observing',refreshPending:null,nextRefreshAt:null};
  await syncAlarms(s);
  try {
    const started=await chrome.tabs.sendMessage(tab.id,{type:'DETECTOR_BEGIN',profile:currentProfile(s)});
    if(started?.ok!==true)throw Error(started?.error||'Observer did not start');
  } catch (e) {
    s.run.enabled=false;s.run.status='stopped';s.run.pageState='error';
    s.run.lastError='Could not activate the page observer: '+String(e?.message||e);
    await syncAlarms(s);
    throw Error(s.run.lastError);
  }
  kickSend();kickHeartbeat('ok');return {ok:true,mode:'passive_dom_observation'};
}
async function stop(){
  const s=await readStore(),tabId=s.run.targetTabId;
  s.run={...s.run,enabled:false,paused:false,targetTabId:null,status:'stopped',pageState:'stopped',refreshPending:null,nextRefreshAt:null};
  await syncAlarms(s);
  if(tabId!=null)void chrome.tabs.sendMessage(tabId,{type:'DETECTOR_STOP_OBSERVE'}).catch(()=>{});
  kickHeartbeat('paused',true);return {ok:true};
}
async function pause(reason,error=''){
  const s=await readStore();if(!s.run.enabled)return;
  s.run.paused=true;s.run.status=reason;s.run.lastError=String(error||reason).slice(0,160);
  s.run.pageState=reason.includes('captcha')?'captcha':reason.includes('restriction')?'access_restricted':reason==='paused_application'?'application':'paused';
  s.run.refreshPending=null;
  const safetyState=reason.includes('captcha')?'captcha':reason.includes('restriction')?'access_restricted':null;
  if(safetyState){
    s.run.safetySignalStatus='pending';s.run.safetySignalReason=safetyState;
    s.run.safetySignalAt=new Date().toISOString();s.run.safetySignalAckAt=null;
    s.run.safetySignalHttpStatus=null;s.run.safetySignalLastError='';
  }
  await syncAlarms(s);
  if(s.run.targetTabId!=null)void chrome.tabs.sendMessage(s.run.targetTabId,{type:'DETECTOR_STOP_OBSERVE'}).catch(()=>{});
  kickHeartbeat(safetyState||'error',true);
}
async function pageReady(sender){
  const s=await readStore();
  if(!s.run.enabled||s.run.paused||sender.tab?.id!==s.run.targetTabId||!C.isAmazonPageUrl(sender.tab.url||''))return {active:false};
  s.run.pageState='observing';s.run.nextRefreshAt=null;
  await writeStore(s);
  return {active:true,profile:currentProfile(s)};
}
// Future audit events are retained locally only. Do NOT POST to the strict production
// /observations API, and do not pretend the server has accepted them.
function retainAudit(s,records){
  for(const raw of records){
    const record={...raw,audit_sequence:s.db.nextAuditSequence++,queued_at:new Date().toISOString(),
      delivery_state:'local_pending_oracle_audit_endpoint'};
    s.db.auditQueue.push(record);
  }
  // Deliberately bounded; never block production job observations because audit
  // transport has not yet been deployed. Surface any loss explicitly in the UI.
  while(s.db.auditQueue.length>A.AUDIT_LIMIT ||
    new TextEncoder().encode(JSON.stringify(s.db.auditQueue)).length>2500000){
    s.db.auditQueue.shift();s.run.auditOverflowCount++;
  }
}
function trimPresence(s){
  const list=Object.entries(s.db.presence);
  if(list.length<=MAX_CACHE)return;
  list.sort((a,b)=>String(a[1].lastSeenAt||'').localeCompare(String(b[1].lastSeenAt||'')));
  for(const [key] of list.slice(0,list.length-MAX_CACHE))delete s.db.presence[key];
}
async function mutationEvidence(msg,sender){
  const s=await readStore();
  if(!s.run.enabled||s.run.paused||sender.tab?.id!==s.run.targetTabId)return {ignored:true};
  if(!C.isAmazonPageUrl(sender.tab.url||''))return {ignored:true};
  if(!['added','removed'].includes(msg.action)||!Array.isArray(msg.candidates))return {error:'Invalid mutation evidence'};
  const scope=await A.scopeFingerprint(sender.tab.url||'');
  const observedAt=typeof msg.observed_at==='string'&&Number.isFinite(Date.parse(msg.observed_at))?msg.observed_at:new Date().toISOString();
  const records=[];
  for(const item of msg.candidates.slice(0,12)){
    const title=C.compact(item?.title||'').slice(0,500),location=C.compact(item?.location||'').slice(0,500);
    const raw=C.compact(item?.visible_card_text||'').slice(0,5000);
    if(!title||!location||raw.length<25)continue;
    const fallback=await C.syntheticJobId(title,location);
    const job=C.canonicalJob({job_id:fallback,title,location,visible_card_text:raw});
    if(!job)continue;
    const ctx={detectorId:currentProfile(s)?.detector_id,observedAt,scope,
      refreshCycleId:s.run.lastRefreshCycleId,scanComplete:false,source:'mutation_record'};
    records.push(A.recordEvent(msg.action==='added'?'dom_insertion_candidate':'dom_removal_candidate',ctx,job,
      {visibility_confirmation:'not_confirmed',content_fingerprint:await A.hexDigest(raw)}));
  }
  if(records.length){retainAudit(s,records);s.run.mutationEvidenceCount+=records.length;}
  s.run.lastMutationAt=observedAt;
  await writeStore(s);
  if(records.length)kickPresence();
  return {stored:records.length,transmission:'oracle_presence_queue'};
}
async function snapshot(msg,sender){
  const s=await readStore();
  if(!s.run.enabled||s.run.paused||sender.tab?.id!==s.run.targetTabId)return {ignored:true};
  if(!C.isAmazonPageUrl(sender.tab.url||msg.page_url||''))return {ignored:true};
  if(!Array.isArray(msg.jobs)||msg.jobs.length>120)return {error:'Invalid card snapshot'};
  const now=new Date().toISOString();let queued=0,unchanged=0,invalid=0;
  const visibleCount=Number.isInteger(msg.visible_count)&&msg.visible_count>=0?Math.min(120,msg.visible_count):msg.jobs.length;
  const unidentifiedCount=Number.isInteger(msg.unidentified_count)&&msg.unidentified_count>=0?Math.min(120,msg.unidentified_count):0;
  const ambiguousCount=Number.isInteger(msg.ambiguous_count)&&msg.ambiguous_count>=0?Math.min(120,msg.ambiguous_count):0;
  const rawCandidateCount=Number.isInteger(msg.raw_candidate_count)&&msg.raw_candidate_count>=0?Math.min(20000,msg.raw_candidate_count):0;
  const parsedCount=Number.isInteger(msg.complete_count)&&msg.complete_count>=0?Math.min(120,msg.complete_count):msg.jobs.length;
  const rejectedReasons={};
  for(const key of ['incomplete_or_hidden','nested_duplicate','parse_failed','ambiguous_identity']){
    const value=msg.rejected_reasons?.[key];
    rejectedReasons[key]=Number.isInteger(value)&&value>=0?Math.min(20000,value):0;
  }
  const advertisedCount=Number.isInteger(msg.advertised_count)&&msg.advertised_count>=0?msg.advertised_count:null;
  for(const raw of msg.jobs){
    const job=C.canonicalJob(raw);if(!job){invalid++;continue;}
    const key=C.stableKey(job),fingerprint=hash(job),previous=s.db.cache[key];
    if(previous?.hash===fingerprint&&previous.sessionId===s.run.sessionId){previous.lastSeen=now;unchanged++;continue;}
    if(s.db.queue.length>=MAX_QUEUE){s.run.paused=true;s.run.status='paused_queue_full';s.run.lastError='Local queue full; observations not silently discarded.';break;}
    const seq=C.nextObservationSequence(s.db.nextSequence);
    s.db.nextSequence=seq+1;
    const event={event_type:'job_card_observed',event_id:crypto.randomUUID(),detector_id:currentProfile(s).detector_id,
      sequence:seq,observed_at:msg.observed_at||now,render_complete:true,job};
    if(bodySize([event])>BODY_LIMIT){invalid++;continue;}
    s.db.queue.push({payload:event,attempts:0,nextAt:0});
    s.db.cache[key]={hash:fingerprint,lastSeen:now,sessionId:s.run.sessionId};queued++;
  }
  // Separate identity, content version and presence evidence. Only a verified,
  // comparable complete scan can advance missing -> confirmed absent.
  const pageScope=await A.scopeFingerprint(sender.tab.url||msg.page_url||'');
  const context={detectorId:currentProfile(s)?.detector_id||'',observedAt:now,scope:pageScope,
    refreshCycleId:typeof msg.scan?.cycle_id==='string'?msg.scan.cycle_id:(s.run.lastRefreshCycleId||''),
    scanComplete:msg.scan?.complete===true,source:msg.scan?.source||'page_scan'};
  const validJobs=msg.jobs.map(C.canonicalJob).filter(Boolean);
  const scan={...msg.scan,scope:pageScope};
  const evidence=A.transitions(s.db.presence,validJobs,context,scan);
  s.db.presence=evidence.presence;
  for(const entry of evidence.events){
    entry.content_fingerprint=entry.job?await A.hexDigest(C.versionText(entry.job)):'';
    entry.previous_content_fingerprint=entry.previous_content?await A.hexDigest(C.versionText(entry.previous_content)):'';
    entry.production_event_id=s.db.queue.find(x=>C.stableKey(x.payload.job)===entry.job_key)?.payload.event_id||null;
  }
  retainAudit(s,evidence.events);
  trimPresence(s);
  if(msg.scan){
    s.db.scanSummaries.push({...A.scanSummary(context,scan,{visible:visibleCount,reportable:validJobs.length,
      identities:validJobs.map(C.stableKey)},evidence.events),scan_id:crypto.randomUUID()});
    if(s.db.scanSummaries.length>A.SCAN_HISTORY_LIMIT){
      const old=s.db.scanSummaries.splice(0,s.db.scanSummaries.length-A.SCAN_HISTORY_LIMIT);
      s.run.presenceScanOverflowCount+=old.filter(x=>x.scan_id).length;
    }
  }
  const countBy=t=>evidence.events.filter(e=>e.event_type===t).length;
  s.run.cardAppearances+=countBy('card_appeared');s.run.cardChanges+=countBy('card_changed');
  s.run.cardProvisionalMissing+=countBy('card_provisionally_missing');
  s.run.cardConfirmedMissing+=countBy('card_absence_evidence');
  s.run.cardReappearances+=countBy('card_reappeared');
  s.run.lastScanComplete=evidence.comparable;s.run.lastScanReason=msg.scan?.reason||'';
  s.run.lastScanScope=A.safeScope(sender.tab.url||msg.page_url||'');
  s.run.lastRefreshCycleId=context.refreshCycleId;
  const keys=Object.keys(s.db.cache);
  if(keys.length>MAX_CACHE){const retained=new Set(s.db.queue.map(e=>C.stableKey(e.payload.job)));
    for(const k of keys.sort((a,b)=>s.db.cache[a].lastSeen.localeCompare(s.db.cache[b].lastSeen))){
      if(Object.keys(s.db.cache).length<=MAX_CACHE)break;if(!retained.has(k))delete s.db.cache[k];}}
  s.run.lastScanAt=now;s.run.lastCardCount=visibleCount;s.run.lastReportableCount=msg.jobs.length;
  s.run.lastUnidentifiedCount=unidentifiedCount;s.run.lastAmbiguousCount=ambiguousCount;
  s.run.lastRawCandidateCount=rawCandidateCount;s.run.lastParsedCount=parsedCount;s.run.lastRejectedReasons=rejectedReasons;
  s.run.lastAdvertisedCount=advertisedCount;s.run.submitted+=queued;s.run.duplicateObservations+=unchanged;
  if(invalid)s.run.lastError=`Skipped ${invalid} invalid job record(s).`;
  else if(ambiguousCount)s.run.lastError=`Ambiguous identity: ${ambiguousCount} distinct card(s) share title and location without individual job identifiers; not merged or submitted.`;
  else if(unidentifiedCount)s.run.lastError=`Found ${visibleCount} visible job card(s); ${unidentifiedCount} could not be parsed or identified for Oracle.`;
  else if(advertisedCount>0&&visibleCount===0)s.run.lastError=`Page advertises ${advertisedCount} job(s), but no complete card structures were recognised. A redacted card DOM sample is needed.`;
  else if(/^(?:Found |Page advertises |Ambiguous identity:)/.test(s.run.lastError))s.run.lastError='';
  if(!s.run.paused)s.run.status='monitoring';
  await writeStore(s);if(s.run.paused)await syncAlarms(s);
  return {queued,unchanged,invalid};
}
function resultStatuses(data,events){
  const list=[data?.results,data?.events,data?.statuses,data?.outcomes].find(Array.isArray);
  if(!list)return null;
  const byId=new Map(list.filter(x=>x&&typeof x==='object'&&x.event_id).map(x=>[x.event_id,x]));
  return events.map((e,i)=>{const val=byId.get(e.event_id)||list[i];return typeof val==='string'?val:(val?.status||val?.result||val?.outcome||'unknown');});
}
function kickSend(){if(!sending)void flushOutbox().catch(()=>{});}
function scheduleNearRetry(at){const wait=Math.max(0,at-Date.now());if(wait<=25000)setTimeout(kickSend,wait+30);}
async function flushOutbox(){
  if(sending)return;sending=true;
  try{while(true){
    const setup=await ordered(async()=>{
      const s=await readStore();if(!s.run.enabled||s.run.paused||!await token()||!s.db.queue.length)return null;
      if(s.db.queue[0].nextAt>Date.now())return null;
      const events=[];for(const item of s.db.queue){if(item.nextAt>Date.now()||events.length>=BATCH_LIMIT)break;
        if(bodySize([...events,item.payload])>BODY_LIMIT)break;events.push(item.payload);}
      return events.length?{events,credential:await token()}:null;
    });
    if(!setup)break;
    const ctrl=new AbortController(),timeout=setTimeout(()=>ctrl.abort(),9000);let response=null,raw=null;
    try{response=await fetch(C.OBSERVATIONS,{method:'POST',headers:{Authorization:`Bearer ${setup.credential}`,'Content-Type':'application/json'},
      body:JSON.stringify({events:setup.events}),credentials:'omit',cache:'no-store',signal:ctrl.signal});
      if(response.status===202){try{raw=await response.json();}catch(_){raw=null;}}
    }catch(_){/* no secret logging */}finally{clearTimeout(timeout);}
    let retryAt=null,pauseError=false;
    const statuses=response?.status===202?resultStatuses(raw,setup.events):null;
    // HTTP 202 without valid per-event outcomes is not proof that every event
    // was accepted. Keep original IDs and payloads for idempotent safe retry.
    const outcome=response?.status===202&&statuses?.length===setup.events.length?
      C.observationOutcomes(statuses):null;
    const worked=!!outcome;
    let needsRescan=false,rescanTabId=null;
    await ordered(async()=>{
      const s=await readStore(),count=setup.events.length;
      const prefix=s.db.queue.slice(0,count).map(e=>e.payload.event_id);
      if(prefix.some((id,i)=>id!==setup.events[i].event_id))return;
      s.run.lastHttpStatus=response?.status||null;s.run.observationsSubmitted+=count;
      if(worked){
        // 202 means a response was processed, not that each observation was
        // accepted. Oracle's out_of_order is a REJECTION and must never be
        // counted as an accepted delivery or silently discarded.
        const rejected=new Set(outcome.rejected);
        for(const i of outcome.rejected){
          const failed=setup.events[i];
          s.db.rejectedObservations.push({payload:failed,rejected_at:new Date().toISOString(),
            reason:'out_of_order',recovery:'new_live_scan_required'});
          delete s.db.cache[C.stableKey(failed.job)];
          for(const record of s.db.auditQueue){if(record.production_event_id===failed.event_id){
            record.production_delivery_status='out_of_order';
            record.production_delivered_at=null;
          }}
        }
        // Explicit bounded rejection archive: surface overflow rather than
        // silently growing Chrome storage until all other queues fail.
        if(s.db.rejectedObservations.length>250){
          const n=s.db.rejectedObservations.length-250;
          s.db.rejectedObservations.splice(0,n);
          s.run.lastError=`Sequence-rejection archive exceeded 250 records; ${n} oldest local records were dropped. Review Oracle audit history.`;
        }
        for(let i=0;i<count;i++){
          if(rejected.has(i))continue;
          for(const record of s.db.auditQueue){if(record.production_event_id===setup.events[i].event_id){
            record.production_delivery_status=statuses[i];
            record.production_delivered_at=new Date().toISOString();
          }}
        }
        s.db.queue.splice(0,count);
        s.run.observationsAccepted+=outcome.accepted.length;
        s.run.observationsRejected+=outcome.rejected.length;
        if(outcome.accepted.length)s.run.lastDeliveryAt=new Date().toISOString();
        s.run.lastResult=statuses.join(', ').slice(0,240);
        s.run.changed+=statuses.filter(x=>x==='job_changed').length;
        if(outcome.rejected.length){
          needsRescan=true;rescanTabId=s.run.targetTabId;
          s.run.lastError=`Oracle rejected ${outcome.rejected.length} stale detector sequence(s). Rechecking current visible cards with a fresh monotonic sequence.`;
        }else if(!s.run.lastError.startsWith('Sequence-rejection archive exceeded'))s.run.lastError='';
        s.run.lastRetryAt=null;
      }else{
        const status=response?.status;
        for(const item of s.db.queue.slice(0,count)){item.attempts=(item.attempts||0)+1;item.nextAt=Date.now()+retryDelay(item.attempts,response?.headers?.get('Retry-After'));}
        retryAt=s.db.queue[0].nextAt;s.run.lastRetryAt=new Date(retryAt).toISOString();
        s.run.lastError=status===202?'Oracle HTTP 202 did not contain recognised outcomes for every event; original events retained for retry.':
          status?`Oracle HTTP ${status}. Queue retained for retry or review.`:'Oracle observation network/timeout error; retrying.';
        if([400,401,403,404,409,413,415,422].includes(status)){s.run.paused=true;s.run.status='paused_api_error';pauseError=true;}
      }
      await writeStore(s);if(pauseError)await syncAlarms(s);
    });
    if(needsRescan&&rescanTabId!=null){
      // Request fresh data from the live page, never alter/retry a rejected
      // payload under its original event ID or invent a new observation.
      void chrome.tabs.sendMessage(rescanTabId,{type:'DETECTOR_RESCAN_CURRENT_CARDS'}).catch(()=>{});
    }
    if(!worked||pauseError){if(retryAt&&!pauseError)scheduleNearRetry(retryAt);break;}
    if(setup.events.length>=BATCH_LIMIT)await new Promise(r=>setTimeout(r,1100));
  }}finally{sending=false;}
}
// Never discard a CAPTCHA heartbeat just because an ordinary 'ok' heartbeat
// is already in flight. Urgent state always takes precedence over 'ok'.
// Evidence uploader runs separately from the production observation queue.
// Never hold the serialized extension store lock while awaiting Oracle network.
function kickPresence(){if(!presenceSending)void flushPresence().catch(()=>{});}
async function flushPresence(){
  if(presenceSending)return;presenceSending=true;
  try{
    const setup=await ordered(async()=>{
      const s=await readStore();
      if(!currentProfile(s)||!profileValid(s)||s.run.paused||Date.now()<(s.run.presenceNextRetryAt||0))return null;
      const key=await token();if(!key)return null;
      // Pre-v0.5.6 scan summaries have no UUID: do not fabricate old scans.
      const prepared=P.batch(currentProfile(s).detector_id,s.db.auditQueue,
          s.db.scanSummaries.filter(x=>x.scan_id));
      return prepared?{...prepared,key,detectorId:currentProfile(s).detector_id}:null;
    });
    if(!setup)return;
    let response=null,body=null;
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),7000);
    try{
      response=await fetch(C.PRESENCE,{method:'POST',credentials:'omit',cache:'no-store',
        headers:{'Authorization':'Bearer '+setup.key,'Content-Type':'application/json'},
        body:JSON.stringify(setup.payload),signal:controller.signal});
      if(response.status===202)body=await response.json();
    }catch(_){/* network problem; production observations remain independent */}
    finally{clearTimeout(timer);}
    const ok=response?.status===202&&body?.status==='recorded_evidence_only'&&
       body?.detector_id===setup.detectorId&&
       (body.events_inserted||0)+(body.duplicate_events||0)===setup.eventIds.length&&
       (body.scans_inserted||0)+(body.duplicate_scans||0)===setup.scanIds.length;
    await ordered(async()=>{
      const s=await readStore();
      s.run.presenceLastHttpStatus=response?.status||null;
      if(ok){
        const ei=new Set(setup.eventIds),si=new Set(setup.scanIds);
        s.db.auditQueue=s.db.auditQueue.filter(e=>!ei.has(e.event_id));
        s.db.scanSummaries=s.db.scanSummaries.filter(e=>!si.has(e.scan_id));
        s.run.presenceEventsUploaded+=setup.eventIds.length;
        s.run.presenceScansUploaded+=setup.scanIds.length;
        s.run.presenceLastUploadAt=new Date().toISOString();
        s.run.presenceNextRetryAt=0;s.run.presenceLastError='';
      }else{
        const status=response?.status||0;
        s.run.presenceLastError=status===404?'Presence endpoint not activated (404).':
          status===422?'Oracle rejected an evidence record; inspect local evidence data.':
          status?`Presence HTTP ${status}. Evidence retained.`:'Presence network error; evidence retained.';
        // No busy-loop if Oracle is not yet upgraded or receives an invalid batch.
        s.run.presenceNextRetryAt=Date.now()+([401,403,404,413,422].includes(status)?15*60000:30000);
      }
      await writeStore(s);
    });
  }finally{presenceSending=false;}
}
function kickHeartbeat(state='ok',force=false){
  if(heartbeating){
    if(state==='captcha'||state==='access_restricted'||(state!=='ok'&&!pendingHeartbeat))
      pendingHeartbeat={state,force:true};
    return;
  }
  void sendHeartbeat(state,force).catch(()=>{}).finally(()=>{
    if(pendingHeartbeat){const next=pendingHeartbeat;pendingHeartbeat=null;kickHeartbeat(next.state,next.force);}
  });
}
async function sendHeartbeat(state='ok',force=false){
  if(heartbeating)return;heartbeating=true;
  try{
    const s=await readStore(),key=await token();
    if(!key||!currentProfile(s)||(!force&&!s.run.enabled))return;
    // If a stale normal heartbeat was queued before a security pause, do not
    // send 'ok' after the page has been classified as blocked.
    if(state==='ok'&&s.run.paused)return;
    const ctrl=new AbortController(),timeout=setTimeout(()=>ctrl.abort(),7000);let response=null;
    const error=state==='captcha'?'captcha':state==='access_restricted'?'access_restricted':null;
    try{response=await fetch(C.HEARTBEAT,{method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},
      body:JSON.stringify(error?{state,recent_error:error}:{state}),cache:'no-store',credentials:'omit',signal:ctrl.signal});}
    catch(_){/* no secret logging */}finally{clearTimeout(timeout);}
    await ordered(async()=>{
      const cur=await readStore();cur.run.lastHeartbeatHttpStatus=response?.status||null;
      if(response?.ok){cur.run.lastHeartbeatAt=new Date().toISOString();cur.run.lastHeartbeatState=state;}
      else if([401,403].includes(response?.status)){
        cur.run.lastError=`Heartbeat authentication error (HTTP ${response.status}).`;
        if(cur.run.enabled){cur.run.paused=true;cur.run.status='paused_invalid_key';await syncAlarms(cur);}
      }else if(cur.run.enabled&&!cur.run.paused){cur.run.lastError=response?`Heartbeat HTTP ${response.status}`:'Heartbeat network error';}
      if(error&&cur.run.safetySignalReason===error){
        cur.run.safetySignalHttpStatus=response?.status||null;
        if(response?.ok){
          cur.run.safetySignalStatus='accepted_by_oracle';
          cur.run.safetySignalAckAt=new Date().toISOString();cur.run.safetySignalLastError='';
        }else{
          cur.run.safetySignalStatus='retry_pending';
          cur.run.safetySignalLastError=response?`HTTP ${response.status}`:'Network error or timeout';
        }
      }
      await writeStore(cur);
    });
    return {ok:!!response?.ok,status:response?.status||null};
  }finally{heartbeating=false;}
}
chrome.runtime.onMessage.addListener((msg,sender,respond)=>{
  if(!msg||typeof msg.type!=='string')return false;
  const fromPage=!!sender.tab;
  const pageTypes=['DETECTOR_PAGE_READY','DETECTOR_SNAPSHOT','DETECTOR_MUTATION_EVIDENCE','DETECTOR_BLOCKED','DETECTOR_DIAGNOSTIC'];
  if(fromPage&&!pageTypes.includes(msg.type))return false;
  if(!fromPage&&pageTypes.includes(msg.type))return false;
  ordered(async()=>{
    switch(msg.type){
      case 'DETECTOR_STATUS':return publicStatus();
      case 'DETECTOR_CONFIGURE':return configure(msg);
      case 'DETECTOR_START':return start(msg);
      case 'DETECTOR_STOP':return stop();
      case 'DETECTOR_PAGE_READY':return pageReady(sender);
      case 'DETECTOR_SNAPSHOT':{const out=await snapshot(msg,sender);if(out.queued)kickSend();kickPresence();return out;}
      case 'DETECTOR_MUTATION_EVIDENCE':return mutationEvidence(msg,sender);
      case 'DETECTOR_DIAGNOSTIC':{
        const s=await readStore();if(sender.tab?.id!==s.run.targetTabId)return {ignored:true};
        s.run.lastError=msg.reason==='card_scan_error'?'Card scanner failed; check extension version and supported page DOM.':'Card scanner diagnostic.';
        await writeStore(s);return {ok:true};
      }
      case 'DETECTOR_BLOCKED':{const s=await readStore();if(sender.tab?.id!==s.run.targetTabId)return {ignored:true};
        await pause(msg.reason==='captcha'?'paused_captcha':msg.reason==='application_in_progress'?'paused_application':'paused_access_restriction',msg.reason);return {ok:true};}
      default:throw Error('Unknown detector action.');
    }
  }).then(r=>respond(r)).catch(e=>respond({error:e?.message||'Action failed'}));
  return true;
});
chrome.alarms.onAlarm.addListener(a=>{
  if(a.name===SEND_ALARM){kickSend();kickPresence();return;}
  if(a.name===HEARTBEAT_ALARM){
    void ordered(async()=>{
      const s=await readStore();
      return s.run.enabled?(s.run.status==='paused_captcha'?'captcha':s.run.status==='paused_access_restriction'?'access_restricted':s.run.paused?'paused':'ok'):null;
    }).then(state=>{if(state)kickHeartbeat(state);});return;
  }
  if(a.name===PROFILE_ALARM){void syncProfile();}
});
chrome.tabs.onRemoved.addListener(id=>{void ordered(async()=>{const s=await readStore();if(s.run.enabled&&id===s.run.targetTabId)await stop();});});
// A full-page challenge can redirect outside the extension's Amazon host
// permissions, preventing content-script injection. Never keep refreshing a
// monitored tab after it leaves the expected site; no claim of CAPTCHA is made.
chrome.tabs.onUpdated.addListener((id,change)=>{
  if(!change?.url)return;
  void ordered(async()=>{
    const s=await readStore();
    if(s.run.enabled&&!s.run.paused&&id===s.run.targetTabId&&!C.isAmazonPageUrl(change.url))
      await pause('paused_navigation','Monitored tab left the supported Amazon Jobs website. Check for a security challenge before restarting.');
  });
});
chrome.runtime.onInstalled.addListener(()=>{
  void chrome.storage.local.setAccessLevel?.({accessLevel:'TRUSTED_CONTEXTS'}).catch(()=>{});
  void ordered(async()=>{const s=await readStore();await syncAlarms(s);});
});
chrome.runtime.onStartup.addListener(()=>{
  void chrome.storage.local.setAccessLevel?.({accessLevel:'TRUSTED_CONTEXTS'}).catch(()=>{});
  void ordered(async()=>{
    const s=await readStore();if(s.run.enabled){
      try{const tab=await chrome.tabs.get(s.run.targetTabId);if(!C.isAmazonPageUrl(tab.url||''))throw Error('tab not restored');
        if(profileValid(s)&&!s.run.paused)void chrome.tabs.sendMessage(tab.id,{type:'DETECTOR_BEGIN',profile:currentProfile(s)}).catch(()=>{});
      }catch(_){s.run.paused=true;s.run.status='paused_restart_reselect_tab';s.run.lastError='Select Amazon Jobs tab and restart monitoring.';}
    }await syncAlarms(s);
    if(s.run.enabled&&s.run.paused&&['captcha','access_restricted'].includes(s.run.safetySignalReason)
       &&s.run.safetySignalStatus!=='accepted_by_oracle')kickHeartbeat(s.run.safetySignalReason,true);
  });
});
