/* Public-page DOM observer. No secret access, network requests or click automation. */
(() => {
  'use strict';
  if (globalThis.__amazonDetectorV2Loaded) return;
  globalThis.__amazonDetectorV2Loaded = true;
  const C = globalThis.AmazonDetectorCore, S = globalThis.AmazonDetectorSafety;
  const SELECTORS = [
    '[data-testid*="job-card" i]', '[data-testid*="jobcard" i]', '[data-test*="job-card" i]',
    '[data-job-id]', '[data-jobid]', '[class*="job-card" i]', '[class*="jobcard" i]',
    '[class*="job-result" i]', '[data-testid*="job" i]', '[class*="job-listing" i]', '[class*="jobListing" i]', 'article', '[role="listitem"]', 'li'
  ];
  const LOCATION_LINE = /(?:^|\n)\s*([^\n]{2,90},\s*(?:England|Wales|Scotland|Northern Ireland|United Kingdom|UK))\s*(?=\n|$)/i;
  const LOCATION_INLINE = /\b([A-Za-z][A-Za-z'. -]{1,75},\s*(?:England|Wales|Scotland|Northern Ireland|United Kingdom|UK))\b/i;
  const JOB_LINK = 'a[href*="/jobs/"],a[href*="/job/"],a[href*="jobId="],a[href*="job_id="],a[href*="requisitionId="]';
  const CARD_FIELDS = /\b(?:Type|Duration|Pay\s*rate|Pay|Salary|Working\s*hours|Hours)\s*:/gi;
  let enabled = false, observer = null, timer = null, scheduledFor = 0;
  let oracleProfile = null, pendingProfile = null;
  let priorCandidate = '', priorSignature = '', lastMutationAt = 0;
  let lastDOMMutationAt=0,forcedNext=false,scanCycleId=(crypto.randomUUID?.()||('local-'+Date.now()+'-'+Math.random().toString(36).slice(2)));
  let lastCompleteSignature='',scopeDirtyUntil=0;
  let inspecting = false, rerun = false;
  const SETTLE_MS = 135;

  function visible(el) {
    if (!(el instanceof HTMLElement) || el.closest('[hidden],[aria-hidden="true"],[aria-busy="true"]')) return false;
    const style = getComputedStyle(el);
    return style.display !== 'none' && style.visibility !== 'hidden';
  }
  function text(el) { return C.compact(el.innerText || el.textContent || ''); }
  function lines(el) { return (el.innerText || el.textContent || '').split(/\r?\n/).map(C.compact).filter(Boolean); }
  function fieldCount(raw) { return new Set(Array.from(raw.matchAll(CARD_FIELDS), m => m[0].replace(/\s/g,'').toLowerCase())).size; }
  function locationIn(el) {
    const full=el.innerText || el.textContent || '';
    const loc=full.match(LOCATION_LINE)?.[1]
      || full.match(/\b(?:Location|City|Work location|Job location)\s*[:：]\s*([^\n]{3,95})/i)?.[1]
      || full.match(LOCATION_INLINE)?.[1]
      || el.querySelector('[data-testid*="location" i],[class*="location" i],[data-test*="location" i]')?.textContent
      || '';
    return C.compact(loc).slice(0,500);
  }
  function titleIn(el) {
    const heading=el.querySelector('h1,h2,h3,h4,h5,[data-testid*="title" i],[class*="job-title" i],[class*="jobTitle" i]');
    const marked=C.compact(heading?.innerText || heading?.textContent || '');
    if(marked && marked.length<=500 && !/\bjobs? found\b/i.test(marked)) return marked;
    const link=Array.from(el.querySelectorAll(JOB_LINK)).find(a=>C.individualJobUrl(a.href));
    const linkText=C.compact(link?.innerText || link?.textContent || '');
    if(linkText.length>=4&&linkText.length<=160&&!/^(apply|view|learn more|details)$/i.test(linkText))return linkText;
    const ls=lines(el);
    for(const rawLine of ls.slice(0,12)) {
      const v=C.compact(rawLine.split(/\b(?:Type|Duration|Pay\s*rate|Pay|Salary|Hours)\s*:/i)[0]);
      if(v.length<4 || v.length>160 || /\bjobs? found\b|job and alert preferences|your pay just got an upgrade/i.test(v))continue;
      if(/^(?:Type|Duration|Pay|From £|Hours|Schedule|Location|Within|Apply|View job)\b/i.test(v))continue;
      if(LOCATION_INLINE.test(v)||/^£\s*\d/.test(v))continue;
      return v;
    }
    return '';
  }
  function isOneJob(el) {
    if(!visible(el))return false;
    const raw=text(el);
    if(raw.length<12||raw.length>5000||/^(?:loading|please wait|search jobs)$/i.test(raw))return false;
    if(!locationIn(el)||!titleIn(el))return false;
    // Prevent parent result panels being mistaken for a single listing.
    const distinctLinks=new Set(Array.from(el.querySelectorAll(JOB_LINK))
      .map(a=>C.individualJobUrl(a.href)).filter(Boolean));
    if(distinctLinks.size>1)return false;
    for(const label of ['Type','Duration','Pay\\s*rate']) {
      const hits=raw.match(new RegExp('\\b'+label+'\\s*:','gi'))||[];
      if(hits.length>1)return false;
    }
    // Modern site cards may omit all pay/type labels. Require a genuine
    // individual job link OR the original stronger multi-field structure.
    const semanticCard=el.matches('[data-testid*="job-card" i],[data-testid*="jobcard" i],[class*="job-card" i],[class*="jobcard" i],[class*="job-listing" i],article');
    return distinctLinks.size===1 || (semanticCard && raw.length>=25 && !!el.querySelector('h2,h3,h4,h5,[data-testid*="title" i]')) || (fieldCount(raw)>=2 &&
      (/(?:\bPay\s*rate|\bPay|\bSalary)\s*:/i.test(raw)||(/\bDuration\s*:/i.test(raw)&&/\bType\s*:/i.test(raw))));
  }
  function identityScope(el) {
    // Generic React cards often put the job ID/link on the outer wrapper while
    // the title and type/pay text live in a smaller inner details DIV.
    let current=el;
    for(let steps=0;steps<5&&current.parentElement&&current.parentElement!==document.body;steps++){
      const parent=current.parentElement;
      if(!isOneJob(parent,true))break; // never cross into a multi-card list
      current=parent;
      if(current.matches('[data-job-id],[data-jobid],[data-job_id]'))break;
    }
    return current;
  }
  function findOfficialLink(el) {
    el=identityScope(el);
    const links=Array.from(el.querySelectorAll('a[href]')).slice(0,36);
    if(el.matches('a[href]'))links.unshift(el);
    for(const a of links) {
      const url=C.individualJobUrl(a.href);
      if(!url)continue;
      const u=new URL(url);
      if(/\/jobs?\/|jobId=|job_id=|position|vacanc|apply|hiring/i.test(u.pathname+u.search))return url;
    }
    return '';
  }
  function findJobId(el,raw,url) {
    el=identityScope(el);
    const named=[el,...Array.from(el.querySelectorAll('[data-job-id],[data-jobid],[data-job_id],[data-vacancy-id],[data-requisition-id]')).slice(0,30)];
    for(const node of named)for(const attr of ['data-job-id','data-jobid','data-job_id','data-vacancy-id','data-requisition-id']){
      const v=node.getAttribute(attr);if(C.jobIdValid(v))return v;
    }
    const m=raw.match(/\bJOB-UK-[A-Z0-9-]{3,}\b/i)||raw.match(/\bJob\s*ID\s*[:#]\s*([A-Z0-9_.:-]{3,})\b/i);
    if(m&&C.jobIdValid(m[1]||m[0]))return m[1]||m[0];
    // A job ID exposed in an actual same-site listing link is also genuine,
    // even when that website URL cannot be submitted as official_url to Oracle.
    const hrefs=[url,...Array.from(el.querySelectorAll('a[href]')).slice(0,30).map(a=>a.href)].filter(Boolean);
    for(const href of hrefs){
      if(!C.isAmazonPageUrl(href))continue;
      const u=new URL(href),q=u.searchParams.get('jobId')||u.searchParams.get('job_id')||u.searchParams.get('requisitionId');
      if(C.jobIdValid(q))return q;
      const hash=u.hash.match(/(?:^|[?&])job(?:_?id)?=([A-Za-z0-9_.:-]+)/i);
      if(hash&&C.jobIdValid(hash[1]))return hash[1];
      const path=(u.pathname+u.hash).match(/\/(?:jobs?|vacancies|requisitions)\/([A-Za-z0-9_.:-]+)(?:\/?$|[?#])/i);
      if(path&&C.jobIdValid(path[1])&&!/^(?:search|results|browse|filter|recommended|apply|all|category)$/i.test(path[1]))return path[1];
    }
    return '';
  }
  async function parseCard(el) {
    if(!isOneJob(el,true))return null;
    const raw=text(el),title=titleIn(el),location=locationIn(el);
    const url=findOfficialLink(el);
    const genuineId=findJobId(el,raw,url);
    // No real ID or job-specific URL? Use deterministic text identity, never a
    // random ID, search-page URL or mutable employment/pay/duration field.
    const jobId=genuineId||(!url?await C.syntheticJobId(title,location):'');
    const type=raw.match(/\bType\s*:\s*(.*?)(?=\s+(?:Duration|Pay\s*rate|Pay|Salary|Hours|Schedule|Location)\s*:|$)/i)?.[1]
      ||raw.match(/\b(?:Full[- ]Time|Part[- ]Time|Reduced|Seasonal|Temporary|Permanent|Flexible)\b(?:\s*[,/]\s*(?:Full[- ]Time|Part[- ]Time|Reduced|Seasonal|Temporary|Permanent|Flexible))*/i)?.[0]||'';
    const pay=raw.match(/\b(?:Pay\s*rate|Pay|Salary)\s*:\s*(?:From\s*)?((?:£\s*\d+(?:\.\d+)?|GBP\s*\d+(?:\.\d+)?)(?:\s*(?:\/|per)\s*(?:hour|hr))?)/i)?.[1]
      ||raw.match(/(?:£\s*\d+(?:\.\d+)?|GBP\s*\d+(?:\.\d+)?)(?:\s*(?:\/|per)\s*(?:hour|hr))?/i)?.[0]||'';
    const hours=raw.match(/\b(?:Hours\s*:\s*)?(\d+(?:\.\d+)?)\s*hours\s*(?:\/|per\s*)?\s*(?:week|weekly)\b/i)?.[1]||'';
    const schedule=raw.match(/\b(?:Shift|Schedule)\s*:\s*(.*?)(?=\s+(?:Type|Duration|Pay|Salary|Hours|Location)\s*:|$)/i)?.[1]||'';
    const duration=raw.match(/\bDuration\s*:\s*(.*?)(?=\s+(?:Type|Pay\s*rate|Pay|Salary|Hours|Shift|Schedule|Location)\s*:|$)/i)?.[1]||'';
    const job=C.canonicalJob({job_id:jobId,official_url:url,title,location,employment_type:C.compact(type),pay:C.compact(pay),
      weekly_hours:hours,schedule:C.compact(schedule),duration:C.compact(duration),visible_card_text:raw});
    return {job,synthetic:!!jobId&&!genuineId&&!url,title,location,raw,element:el};
  }
  function collectCandidates() {
    const candidates=new Set();
    for(const n of document.querySelectorAll(SELECTORS.join(',')))candidates.add(n);
    // Start from job-specific links. Their ancestors often carry the location
    // while React-generated card wrappers have no semantic class or role.
    for(const link of document.querySelectorAll(JOB_LINK)){
      if(!C.individualJobUrl(link.href))continue;
      let el=link;
      for(let depth=0;el&&el!==document.body&&depth<9;depth++,el=el.parentElement){
        if(isOneJob(el)){candidates.add(el);break;}
      }
    }
    // Amazon UK also renders cards as hashed DIV structures with neither <article>
    // nor <li>. Walk from leaf field labels to the *smallest* full card ancestor.
    const walker=document.createTreeWalker(document.body,NodeFilter.SHOW_TEXT);
    let node,seen=0;
    while((node=walker.nextNode())&&seen++<15000){
      if(!/\b(?:Type|Duration|Pay\s*rate|Pay|Salary|Location|City)\s*:/i.test(node.nodeValue||''))continue;
      let el=node.parentElement;
      for(let depth=0;el&&el!==document.body&&depth<12;depth++,el=el.parentElement){
        if(isOneJob(el)) {candidates.add(el);break;}
      }
    }
    return [...candidates];
  }
  async function listCards() {
    const raw=collectCandidates();
    const rejects={incomplete_or_hidden:0,nested_duplicate:0,parse_failed:0,ambiguous_identity:0};
    const candidates=[];
    for(const el of raw){
      if(isOneJob(el,true))candidates.push(el);
      else rejects.incomplete_or_hidden++;
    }
    // Keep the smallest complete single-card container; discard its ancestors.
    candidates.sort((a,b)=>a.querySelectorAll('*').length-b.querySelectorAll('*').length);
    const chosen=[];
    for(const el of candidates){
      if(chosen.some(prev=>el.contains(prev)||prev.contains(el))){rejects.nested_duplicate++;continue;}
      chosen.push(el);
      if(chosen.length>=120)break;
    }
    const parsed=await Promise.all(chosen.map(parseCard));
    const byKey=new Map();let withoutIdentity=0,complete=0;
    for(const item of parsed){
      if(!item?.job){rejects.parse_failed++;withoutIdentity++;continue;}
      complete++;
      const k=C.stableKey(item.job);
      const bucket=byKey.get(k)||[];
      bucket.push(item);byKey.set(k,bucket);
    }
    const jobs=[];let ambiguousCount=0;
    for(const bucket of byKey.values()){
      // Title+location may describe two genuinely distinct jobs. Do not
      // silently merge text-only cards that cannot be distinguished.
      if(bucket.length>1&&bucket.some(item=>item.synthetic)){
        ambiguousCount+=bucket.length;rejects.ambiguous_identity+=bucket.length;
        continue;
      }
      const best=bucket.reduce((a,b)=>b.job.visible_card_text.length>a.job.visible_card_text.length?b:a);
      jobs.push(best.job);
    }
    const headline=document.body?.innerText?.match(/\b(\d{1,4})\s+jobs?\s+found\b/i);
    const noResults=/\b(?:no jobs (?:found|available)|no results (?:found|available)|0 jobs? (?:found|available))\b/i.test(document.body?.innerText||'');
    return {jobs,rawCount:raw.length,completeCount:complete,visibleCount:chosen.length,
      unidentifiedCount:withoutIdentity,ambiguousCount,rejected:rejects,
      advertisedCount:headline?Number(headline[1]):(noResults?0:null)};
  }
  function pageComparable(result){
    // Conservative: never infer absence from an incomplete render, loading
    // skeleton, pagination, a filter transition or an obscured/blocked screen.
    const body=(document.body?.innerText||'').slice(0,20000);
    if(document.readyState!=='complete')return {complete:false,reason:'document_loading'};
    if(Date.now()<scopeDirtyUntil)return {complete:false,reason:'filter_or_navigation_changed'};
    if(/\b(?:loading jobs|loading results|please wait|fetching jobs|verifying|captcha|access denied)\b/i.test(body))return {complete:false,reason:'loading_or_blocked'};
    if(document.querySelector('[aria-busy="true"],[role="progressbar"],[class*="skeleton" i]'))return {complete:false,reason:'skeleton_or_busy'};
    if(/\b(?:next page|previous page|load more|show more jobs)\b/i.test(body))return {complete:false,reason:'pagination_possible'};
    if(result.ambiguousCount||result.unidentifiedCount||result.completeCount!==result.visibleCount)return {complete:false,reason:'incomplete_card_parsing'};
    // A stable single-page rendered list may omit a numeric result header.
    // Absence still requires two separate complete scans at least 8s apart on Oracle.
    if(result.advertisedCount===null)return {complete:result.visibleCount>0,reason:result.visibleCount>0?'stable_render_no_result_header':'unknown_empty_result_coverage'};
    if(result.visibleCount!==result.advertisedCount)return {complete:false,reason:'visible_count_mismatch'};
    return {complete:true,reason:'full_comparable_result_set'};
  }
  // Mutation records survive a synchronous insert->remove; the detached node's
  // textContent can remain readable. Keep evidence but never claim the removed
  // node proves a real listing vanished from Amazon.
  function nodeWords(el){const parts=[];const walker=document.createTreeWalker(el,NodeFilter.SHOW_TEXT);let node,count=0;
    while((node=walker.nextNode())&&count++<300)parts.push(node.nodeValue||'');
    return C.compact(parts.join(' '));}
  function extractMutationCandidates(records,action){
    const results=[],seen=new Set(),nodes=[];
    for(const record of records.slice(0,35)){
      if(record.type!=='childList')continue;
      for(const node of (action==='added'?record.addedNodes:record.removedNodes)){
        if(node.nodeType===1)nodes.push(node);
      }
    }
    for(const root of nodes.slice(0,24)){
      const elements=[root,...Array.from(root.querySelectorAll?.('article,li,[role="listitem"],div')||[]).slice(0,36)];
      for(const el of elements){
        const raw=nodeWords(el);
        if(raw.length<25||raw.length>5000||fieldCount(raw)<2||!locationIn(el))continue;
        if((raw.match(/\bType\s*:/gi)||[]).length>1)continue;
        const title=titleIn(el),location=(nodeWords(el).match(LOCATION_INLINE)?.[1]||locationIn(el));
        if(!title||!location)continue;
        const k=JSON.stringify([title,location,raw]);
        if(seen.has(k))continue;
        seen.add(k);
        results.push({title,location,visible_card_text:raw});
        if(results.length>=8)return results;
      }
    }
    return results;
  }
  function observeMutations(records){
    // CAPTCHA may be inserted while a scan is running. Do not wait for debounce.
    const security=securityState();
    if(security){stopForSecurity(security);return;}
    lastDOMMutationAt=Date.now();
    const added=extractMutationCandidates(records,'added');
    const removed=extractMutationCandidates(records,'removed');
    if(added.length)emit({type:'DETECTOR_MUTATION_EVIDENCE',action:'added',candidates:added,observed_at:new Date().toISOString()});
    if(removed.length)emit({type:'DETECTOR_MUTATION_EVIDENCE',action:'removed',candidates:removed,observed_at:new Date().toISOString()});
    // React in the next task to a DOM change already received from the site.
    // Do not generate an extra request or attempt to impersonate user input.
    schedule(0);
  }
  // Filter controls can change the result set without changing the URL.
  // Such a transition must not be interpreted as job disappearance evidence.
  function filtersChanged(e){
    if(e.target?.closest?.('input,select,[role="combobox"],[role="searchbox"],[class*="filter" i]'))scopeDirtyUntil=Date.now()+3000;
  }
  function emit(msg){try{Promise.resolve(chrome.runtime.sendMessage(msg)).catch(()=>{});}catch(_) {}}
  // Security state is checked before scanning, on mutations, on page load and
  // immediately before every refresh. Stop locally even if Oracle is offline.
  let securityNotified = false;
  function securityState(){return S?.inspect(document,location.href)||null;}
  function stopForSecurity(reason){
    if(securityNotified)return;
    securityNotified=true;
    halt();
    // Retrying this *local* Chrome message guards against a service-worker
    // startup race. No requests to Amazon or Oracle originate from this code.
    (async()=>{for(let attempt=0;attempt<3;attempt++){
      try{const r=await chrome.runtime.sendMessage({type:'DETECTOR_BLOCKED',reason});if(r?.ok||r?.ignored)return;}catch(_){/* worker not yet ready */}
      await new Promise(resolve=>setTimeout(resolve,250*(attempt+1)));
    }})();
  }
  async function inspect(force=false){
    if(!enabled)return;
    if(inspecting){rerun=true;return;}
    inspecting=true;
    try{
      const security=securityState();
      if(security){stopForSecurity(security);return;}
      const body=C.compact(document.body?.innerText||'').slice(0,2400);
      if(C.isApplicationUrl(location.href)||/\bStep\s+[1-9]\s+of\s+[1-9]|Schedule pre-hire appointment|\bStart Application\b|\bAccept Offer\b/i.test(body)){
        stopForSecurity('application_in_progress');return;
      }
      const scanStart=Date.now(),result=await listCards(),scanEnd=Date.now();
      if(!enabled)return;
      const signature=JSON.stringify({jobs:result.jobs.map(j=>[C.stableKey(j),C.versionText(j)]).sort((a,b)=>a[0].localeCompare(b[0])),
        visibleCount:result.visibleCount,unidentifiedCount:result.unidentifiedCount,ambiguousCount:result.ambiguousCount,
        advertisedCount:result.advertisedCount,rawCount:result.rawCount});
      if(signature!==priorCandidate){priorCandidate=signature;lastMutationAt=Date.now();schedule(SETTLE_MS);return;}
      if(Date.now()-lastMutationAt<SETTLE_MS){schedule(SETTLE_MS);return;}
      if(signature===priorSignature&&!force)return;
      priorSignature=signature;
      const comparison=pageComparable(result);
      const source=(Date.now()-lastDOMMutationAt<1200)?'dom_mutation':'scheduled_page_scan';
      // First comparable disappearance evidence needs a distinct later scan.
      if(comparison.complete&&signature!==lastCompleteSignature){lastCompleteSignature=signature;schedule(1400,true);}
      emit({type:'DETECTOR_SNAPSHOT',jobs:result.jobs,visible_count:result.visibleCount,
        raw_candidate_count:result.rawCount,complete_count:result.completeCount,rejected_reasons:result.rejected,
        ambiguous_count:result.ambiguousCount,unidentified_count:result.unidentifiedCount,advertised_count:result.advertisedCount,
        observed_at:new Date().toISOString(),page_url:location.href,
        scan:{cycle_id:scanCycleId,started_at:new Date(scanStart).toISOString(),finished_at:new Date(scanEnd).toISOString(),
          extraction_ms:scanEnd-scanStart,complete:comparison.complete,reason:comparison.reason,
          advertised_count:result.advertisedCount,source}});
    }catch(err){
      // Diagnostic category only; never emit page text, URLs or credentials.
      emit({type:'DETECTOR_DIAGNOSTIC',reason:'card_scan_error',error_type:err instanceof TypeError?'TypeError':'scan_exception'});
    }finally{
      inspecting=false;
      if(rerun&&enabled){rerun=false;schedule(0);}
    }
  }
  function schedule(ms=SETTLE_MS,force=false){if(!enabled)return;if(force)forcedNext=true;
    const target=Date.now()+ms;
    if(timer!==null){if(target>=scheduledFor)return;clearTimeout(timer);}
    scheduledFor=target;timer=setTimeout(()=>{timer=null;scheduledFor=0;const useForce=forcedNext;forcedNext=false;void inspect(useForce);},ms);}
  function halt(){
    enabled=false;document.removeEventListener('change',filtersChanged,true);
    document.removeEventListener('input',filtersChanged,true);
    oracleProfile=null;observer?.disconnect();observer=null;
    if(timer!==null){clearTimeout(timer);timer=null;scheduledFor=0;}
  }
  function arm(){
    halt();enabled=true;priorCandidate='';priorSignature='';lastMutationAt=0;
    if(!document.body)return;
    scanCycleId=(crypto.randomUUID?.()||('local-'+Date.now()+'-'+Math.random().toString(36).slice(2)));lastCompleteSignature='';scopeDirtyUntil=0;lastDOMMutationAt=0;forcedNext=false;
    document.addEventListener('change',filtersChanged,true);document.addEventListener('input',filtersChanged,true);
    observer=new MutationObserver(observeMutations);
    observer.observe(document.body,{subtree:true,childList:true,characterData:true,attributes:true,
      attributeFilter:['class','aria-hidden','style','hidden','src','title','id','name','data-job-id','data-jobid']});
    void inspect(true);
  }
  function begin(profile){
    let validated;
    try { validated=C.validateProfile(profile); }
    catch (_) { halt();return false; }
    securityNotified=false;
    const issue=securityState();
    if(issue){stopForSecurity(issue);return false;}
    oracleProfile=validated;
    arm(); // initial snapshot and one MutationObserver; no network or page timer
    return true;
  }
  function beginAfterLoad(profile){if(document.readyState==='complete')begin(profile);
    else{pendingProfile=profile;window.addEventListener('load',()=>{const p=pendingProfile;pendingProfile=null;if(p)begin(p);},{once:true});}}
  chrome.runtime.onMessage.addListener((msg,_sender,respond)=>{
    if(msg?.type==='DETECTOR_SAFETY_CHECK'){const issue=securityState();if(issue&&enabled)stopForSecurity(issue);respond({safe:!issue,reason:issue});}
    else if(msg?.type==='DETECTOR_BEGIN'){beginAfterLoad(msg.profile);respond({ok:true});}
    else if(msg?.type==='DETECTOR_SCHEDULE_UPDATE'){try{oracleProfile=C.validateProfile(msg.profile);respond({ok:true});}
      catch(_){halt();respond({error:'Invalid Oracle profile update'});}}
    else if(msg?.type==='DETECTOR_RESCAN_CURRENT_CARDS'){if(enabled&&!securityState()){schedule(0,true);respond({ok:true});}else respond({ok:false});}
    else if(msg?.type==='DETECTOR_STOP_OBSERVE'){pendingProfile=null;halt();respond({ok:true});}
  });
  async function boot(){try{const issue=securityState();if(issue){stopForSecurity(issue);return;}
    const cfg=await chrome.runtime.sendMessage({type:'DETECTOR_PAGE_READY'});
    if(cfg?.active)begin(cfg.profile);}catch(_){/* Service worker unavailable or no active detector. */}}
  if(document.readyState==='complete')boot();else window.addEventListener('load',boot,{once:true});
})();
