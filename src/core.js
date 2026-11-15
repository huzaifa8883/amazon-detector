/* Amazon Jobs Signal Detector — strict Oracle profile and public job validation. */
(function(scope){
  'use strict';
  const BASE='https://145-241-233-33.sslip.io';
  const PROFILE=BASE+'/v1/detectors/profile';
  const OBSERVATIONS=BASE+'/v1/detectors/observations';
  const HEARTBEAT=BASE+'/v1/detectors/heartbeat';
  const PRESENCE=BASE+'/v1/detectors/presence';
  const ROOTS=['amazon.jobs','hiring.amazon.com'];
  const PAGE_ROOTS=[...ROOTS,'jobsatamazon.co.uk','jobs.amazon.co.uk'];
  const PROFILE_REFRESH_MS=15*60*1000;
  const PROFILE_MAX_AGE_MS=60*60*1000;
  function compact(s){return String(s??'').normalize('NFC').replace(/[\u00a0\u200b\u200c\u200d]/g,' ').replace(/\s+/g,' ').trim();}
  function hostAllowed(host,roots){return roots.some(r=>host===r||host.endsWith('.'+r));}
  function isAmazonPageUrl(value){try{const u=new URL(value);return u.protocol==='https:'&&hostAllowed(u.hostname.toLowerCase(),PAGE_ROOTS);}catch(_){return false;}}
  function officialUrl(value){if(!value)return '';try{const u=new URL(value);if(u.protocol!=='https:'||u.username||u.password||!hostAllowed(u.hostname.toLowerCase(),ROOTS))return '';u.hash='';return u.href.length<=1500?u.href:'';}catch(_){return '';}}
  function isApplicationUrl(value){
    try{const u=new URL(value);return /\/(?:apply|application|candidate|prehire)(?:\/|$|[?#])/i.test(u.pathname+u.search+u.hash);}
    catch(_){return false;}
  }
  function detectorIdValid(id){return /^detector-(0[1-9]|10)$/.test(String(id||''));}
  function jobIdValid(v){return !!v&&String(v).length<=150&&/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/.test(String(v));}
  function cleanField(v,max=500){const c=compact(v);return c.length<=max?c:'';}
  function canonicalJob(raw){
    if(!raw||typeof raw!=='object')return null;
    const title=cleanField(raw.title),location=cleanField(raw.location),visible_card_text=compact(raw.visible_card_text);
    const job_id=cleanField(raw.job_id,150),official_url=officialUrl(raw.official_url);
    if(!title||!location||visible_card_text.length<8||visible_card_text.length>12000||(!official_url&&!jobIdValid(job_id)))return null;
    if(raw.job_id&&!jobIdValid(job_id))return null;
    const job={title,location,visible_card_text};if(job_id)job.job_id=job_id;if(official_url)job.official_url=official_url;
    for(const field of ['employment_type','pay','weekly_hours','schedule','duration']){
      if(raw[field]!=null&&String(raw[field]).trim()){const value=cleanField(raw[field]);if(!value)return null;job[field]=value;}
    }
    return job;
  }
  // Identity fallback is SHA-256(title + location) ONLY; mutable card fields must
  // never enter a synthetic identity. All detectors receive the same UTF-8 input.
  function normalizedIdentityPart(s){return compact(s).toLocaleLowerCase('en-GB');}
  async function syntheticJobId(title,location){
    const t=normalizedIdentityPart(title),l=normalizedIdentityPart(location);
    if(!t||!l)throw Error('A complete title and location are needed for text identity.');
    const input=JSON.stringify(['cardtext-v1',t,l]);
    const digest=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(input));
    return 'cardtext-v1-'+Array.from(new Uint8Array(digest),b=>b.toString(16).padStart(2,'0')).join('');
  }
  // Only real individual job URLs can be treated as official job links. A generic
  // search/results page is never an individual listing and never an ID source.
  function individualJobUrl(value){
    const safe=officialUrl(value);if(!safe)return '';
    const u=new URL(safe);
    const q=u.searchParams.get('jobId')||u.searchParams.get('job_id')||u.searchParams.get('requisitionId');
    if(jobIdValid(q)&&!/^(?:search|results|recommended|all|browse)$/i.test(q))return safe;
    const match=u.pathname.match(/\/(?:jobs?|positions|vacancies|requisitions)\/([A-Za-z0-9_.:-]+)(?:\/)?$/i);
    if(!match||/^(?:search|results|browse|filter|recommended|apply|index|all|category)$/i.test(match[1]))return '';
    return safe;
  }
  function stableKey(job){return job.job_id?'id:'+job.job_id:job.official_url?'url:'+job.official_url:'';}
  function versionText(job){return compact(job.visible_card_text);}
  // Oracle requires a globally monotonic sequence per authenticated detector.
  // Millisecond epoch is intentionally well above legacy local counters that
  // may have restarted at 1 after an extension folder/profile replacement.
  // Max with persisted state also handles multiple observations in one ms.
  function nextObservationSequence(nextValue,now=Date.now()){
    const previous=Number.isSafeInteger(nextValue)&&nextValue>=1?nextValue:1;
    const epoch=Number.isSafeInteger(now)&&now>=1?now:Date.now();
    return Math.max(previous,epoch);
  }
  function observationOutcomes(statuses){
    if(!Array.isArray(statuses))return null;
    const success=new Set(['new_job','job_changed','unchanged','duplicate_event','stale','stale_known_version','reversion_pending']);
    if(statuses.some(x=>!success.has(x)&&x!=='out_of_order'))return null;
    return {accepted:statuses.map((x,i)=>x!=='out_of_order'?i:-1).filter(x=>x>=0),
      rejected:statuses.map((x,i)=>x==='out_of_order'?i:-1).filter(x=>x>=0)};
  }

  function expectedSeconds(id){
    if(!detectorIdValid(id))return null;
    const n=Number(id.slice(-2))%10;
    return Array.from({length:6},(_,i)=>n+i*10);
  }
  function exactKeys(o,allowed){return Object.keys(o).every(k=>allowed.includes(k));}
  function validateProfile(raw){
    if(!raw||typeof raw!=='object'||Array.isArray(raw))throw Error('Oracle did not return a valid profile object.');
    if(!exactKeys(raw,['schema_version','detector_id','source','revision','server_time_utc','schedule']))throw Error('Unexpected Oracle profile fields.');
    if(raw.schema_version!==2||raw.source!=='oracle_key_profile'||!detectorIdValid(raw.detector_id))throw Error('Unsupported Oracle detector profile.');
    if(!Number.isSafeInteger(raw.revision)||raw.revision<1)throw Error('Invalid Oracle profile revision.');
    if(typeof raw.server_time_utc!=='string'||!/(?:Z|[+-]\d\d:\d\d)$/.test(raw.server_time_utc)||!Number.isFinite(Date.parse(raw.server_time_utc)))throw Error('Invalid Oracle server time.');
    const sc=raw.schedule;
    if(!sc||typeof sc!=='object'||Array.isArray(sc)||!exactKeys(sc,['mode','timezone','interval_seconds','seconds_of_minute','missed_slot_policy']))throw Error('Unexpected Oracle schedule fields.');
    if(sc.mode!=='fixed_10_seconds'||sc.timezone!=='Europe/London'||sc.interval_seconds!==10||sc.missed_slot_policy!=='skip')throw Error('Unsupported Oracle schedule.');
    const expected=expectedSeconds(raw.detector_id);
    if(!Array.isArray(sc.seconds_of_minute)||sc.seconds_of_minute.length!==6||sc.seconds_of_minute.some((v,i)=>!Number.isInteger(v)||v!==expected[i]))throw Error('Oracle schedule does not match the authenticated detector identity.');
    return {schema_version:2,detector_id:raw.detector_id,source:'oracle_key_profile',revision:raw.revision,
      server_time_utc:raw.server_time_utc,schedule:{mode:sc.mode,timezone:sc.timezone,interval_seconds:10,
        seconds_of_minute:[...expected],missed_slot_policy:'skip'}};
  }
  // UK UTC offsets are always whole hours, so wall-clock seconds match UTC seconds
  // through both GMT and BST. Always derive the next slot from the current clock;
  // do not advance an accumulating setInterval (which would drift).
  function nextScheduledRefreshMs(afterMs,profile){
    const seconds=profile?.schedule?.seconds_of_minute;
    if(!Array.isArray(seconds)||seconds.length!==6||!Number.isFinite(afterMs))return null;
    const min=Math.floor(afterMs/60000)*60000;
    for(let m=0;m<3;m++)for(const sec of seconds){const t=min+m*60000+sec*1000;if(t>afterMs)return t;}
    return null;
  }
  function profileFresh(profile,obtainedAt,now=Date.now(),maxAge=PROFILE_MAX_AGE_MS){
    return !!profile&&Number.isFinite(obtainedAt)&&obtainedAt<=now&&now-obtainedAt<=maxAge;
  }
  function formatUK(ms){if(!Number.isFinite(ms))return '—';return new Intl.DateTimeFormat('en-GB',{
    timeZone:'Europe/London',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false
  }).format(new Date(ms));}
  const api={PROFILE,OBSERVATIONS,HEARTBEAT,PRESENCE,PROFILE_REFRESH_MS,PROFILE_MAX_AGE_MS,compact,isAmazonPageUrl,isApplicationUrl,officialUrl,individualJobUrl,normalizedIdentityPart,syntheticJobId,
    detectorIdValid,jobIdValid,canonicalJob,stableKey,versionText,nextObservationSequence,observationOutcomes,expectedSeconds,validateProfile,nextScheduledRefreshMs,profileFresh,formatUK};
  scope.AmazonDetectorCore=api;
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof globalThis!=='undefined'?globalThis:this);
