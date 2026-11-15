/* Local availability evidence. No network calls; Oracle remains authoritative. */
(function(scope){
  'use strict';
  const C=scope.AmazonDetectorCore;
  const CONFIRM_GAP_MS=900;
  const SCAN_HISTORY_LIMIT=120;
  const AUDIT_LIMIT=600;
  function safeScope(pageUrl){
    try{
      const u=new URL(pageUrl);
      if(!C.isAmazonPageUrl(u.href))return '';
      // Never persist potentially private search query values, tokens or fragments.
      return u.origin+u.pathname;
    }catch(_){return '';}
  }
  async function scopeFingerprint(pageUrl){
    try{
      const u=new URL(pageUrl);
      if(!C.isAmazonPageUrl(u.href))return '';
      // The digest keeps distinct URL query/filter scopes comparable without exposing their values.
      return safeScope(pageUrl)+'#'+(await hexDigest(u.search));
    }catch(_){return '';}
  }
  async function hexDigest(raw){
    const bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(String(raw)));
    return Array.from(new Uint8Array(bytes),b=>b.toString(16).padStart(2,'0')).join('');
  }
  function badged(text){
    const matches=String(text||'').match(/\b(?:Flexible Shifts|Shifts available soon|Shifts available|Full Time|Part Time|Reduced)\b/gi)||[];
    return [...new Set(matches.map(C.compact))];
  }
  function jobDetails(job){
    if(!job)return null;
    const out={};
    for(const k of ['job_id','official_url','title','location','employment_type','pay','duration','weekly_hours','schedule','visible_card_text']){
      if(typeof job[k]==='string')out[k]=job[k];
    }
    out.availability_badges=badged(job.visible_card_text);
    return out;
  }
  function changes(before,after){
    if(!before)return [];
    const result=[];
    for(const key of ['title','location','employment_type','pay','duration','weekly_hours','schedule','visible_card_text']){
      const a=before[key]||'',b=after[key]||'';
      if(a!==b)result.push({field:key,before:a,after:b});
    }
    const a=badged(before.visible_card_text).sort(),b=badged(after.visible_card_text).sort();
    if(JSON.stringify(a)!==JSON.stringify(b))result.push({field:'availability_badges',before:a,after:b});
    return result;
  }
  function comparableScan(scan,validCount){
    // A page must expressly meet the content-script's conservative coverage test.
    // One incomplete reload or skeleton is never disappearance evidence.
    return scan?.complete===true && scan?.scope && Number.isInteger(validCount)&&validCount>=0;
  }
  function recordEvent(type,context,job,extra={}){
    return {event_id:crypto.randomUUID(),detector_id:context.detectorId||'',event_type:type,
      observed_at:context.observedAt,scope:context.scope,refresh_cycle_id:context.refreshCycleId||'',
      scan_complete:context.scanComplete===true,source:context.source||'page_scan',
      job_key:job?C.stableKey(job):'',job:jobDetails(job),...extra};
  }
  function transitions(priorPresence, jobs, context, scan){
    const presence={...priorPresence},events=[];
    const seen=new Set(),at=Date.parse(context.observedAt)||Date.now();
    const isComparable=comparableScan(scan,jobs.length);
    for(const job of jobs){
      const key=C.stableKey(job);if(!key)continue;
      seen.add(key);
      const previous=presence[key];
      const prevJob=previous?.job||null;
      const version=C.versionText(job);
      if(!previous){events.push(recordEvent('card_appeared',context,job));}
      else if(previous.state==='confirmed_absent'){
        events.push(recordEvent('card_reappeared',context,job,{previous_content:jobDetails(prevJob),same_content:previous.version===version}));
      }else if(previous.version!==version||changes(prevJob,job).some(x=>x.field!=='visible_card_text')){
        events.push(recordEvent('card_changed',context,job,{previous_content:jobDetails(prevJob),changed_fields:changes(prevJob,job)}));
      }
      presence[key]={job,version,state:'visible',lastSeenAt:context.observedAt,firstMissingAt:null,missingScans:0,scope:context.scope};
    }
    if(isComparable){
      for(const [key,prev] of Object.entries(presence)){
        if(prev.scope!==context.scope||seen.has(key)||prev.state==='confirmed_absent')continue;
        if(prev.state!=='provisional_missing'){
          presence[key]={...prev,state:'provisional_missing',missingScans:1,firstMissingAt:context.observedAt};
          events.push(recordEvent('card_provisionally_missing',context,prev.job,{last_seen_at:prev.lastSeenAt}));
        }else if(at-Date.parse(prev.firstMissingAt)>=CONFIRM_GAP_MS){
          presence[key]={...prev,state:'confirmed_absent',missingScans:prev.missingScans+1};
          events.push(recordEvent('card_absence_evidence',context,prev.job,{last_seen_at:prev.lastSeenAt,first_missing_at:prev.firstMissingAt,
            scope_limited:true,detector_confirmation_only:true}));
        }
      }
    }
    return {presence,events,comparable:isComparable};
  }
  function scanSummary(context,scan,counts,events){
    return {detector_id:context.detectorId,refresh_cycle_id:context.refreshCycleId,scope:context.scope,
      started_at:scan?.started_at||context.observedAt,completed_at:scan?.finished_at||context.observedAt,
      observed_at:context.observedAt,complete:scan?.complete===true,reason:scan?.reason||'',
      advertised_count:scan?.advertised_count??null,visible_count:counts.visible,reportable_count:counts.reportable,
      identities:counts.identities.slice(0,120),source:context.source,
      extraction_ms:scan?.extraction_ms??null,transition_count:events.length};
  }
  const api={CONFIRM_GAP_MS,SCAN_HISTORY_LIMIT,AUDIT_LIMIT,safeScope,scopeFingerprint,hexDigest,badged,jobDetails,changes,comparableScan,recordEvent,transitions,scanSummary};
  scope.AmazonDetectorAvailability=api;
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof globalThis!=='undefined'?globalThis:this);
