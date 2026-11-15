(()=>{
  'use strict';
  const $=id=>document.getElementById(id),C=globalThis.AmazonDetectorCore;
  const send=async body=>{const r=await chrome.runtime.sendMessage(body);if(r?.error)throw Error(r.error);return r;};
  const fmt=iso=>{const v=iso?Date.parse(iso):NaN;return Number.isFinite(v)?C.formatUK(v)+' UK':'—';};
  function notice(s,bad=false){$('feedback').textContent=s;$('feedback').className=bad?'error':'';}
  async function refresh(){
    const s=await send({type:'DETECTOR_STATUS'});
    $('detectorId').textContent=s.detectorId||'Not connected';
    $('seconds').textContent=s.assignedSeconds?.map(x=>String(x).padStart(2,'0')).join(', ')||'—';
    $('schedule').textContent=s.assignedSeconds?.length?'Every 10 seconds · UK clock':'—';
    $('profileStatus').textContent=s.profileStatus||'Not configured';
    $('runStatus').textContent=(s.status||'stopped').replaceAll('_',' ').toUpperCase();
    $('statusDetail').textContent=s.lastError||(s.enabled?'Monitoring public Amazon Jobs listings.':'Enter your assigned key and click Save & Connect.');
    $('dot').className='dot'+(s.paused?' warn':s.enabled?' ok':'');
    $('cards').textContent=s.lastCardCount||0;
    $('cardDiagnostic').textContent=s.lastScanAt?`${s.lastReportableCount||0} reportable · ${s.lastAmbiguousCount||0} ambiguous · ${s.lastUnidentifiedCount||0} unparsed${s.lastAdvertisedCount!=null?` · page shows ${s.lastAdvertisedCount} jobs found`:''}`:'No card scan completed yet.';$('changes').textContent=s.submitted||0;$('pending').textContent=s.queueLength||0;
    $('rawCandidates').textContent=s.lastRawCandidateCount||0;
    $('parsedCards').textContent=s.lastParsedCount||0;
    $('ambiguousCards').textContent=s.lastAmbiguousCount||0;
    const rejected=s.lastRejectedReasons||{};
    $('rejectedCards').textContent=(rejected.incomplete_or_hidden||0)+(rejected.nested_duplicate||0)+(rejected.parse_failed||0)+(rejected.ambiguous_identity||0);
    $('rejectionDetails').textContent=Object.entries(rejected).filter(([,v])=>v).map(([k,v])=>`${k.replaceAll('_',' ')}: ${v}`).join(' · ');
    $('queuedObservations').textContent=s.submitted||0;
    $('sentObservations').textContent=s.observationsSubmitted||0;
    $('acceptedObservations').textContent=s.observationsAccepted||0;
    $('rejectedObservations').textContent=s.rejectedObservations||0;
    $('pageState').textContent=s.pageState||'—';$('lastScan').textContent=fmt(s.lastScanAt);
    $('nextRefresh').textContent='Disabled';$('lastRefresh').textContent=fmt(s.lastRefreshAt);
    $('refreshCount').textContent=s.refreshCount||0;$('heartbeatAt').textContent=fmt(s.lastHeartbeatAt);
    $('heartbeatState').textContent=s.lastHeartbeatState||'—';
    $('safetySignal').textContent=s.safetySignalStatus==='none'?'—':
      `${s.safetySignalReason||'security'}: ${(s.safetySignalStatus||'pending').replaceAll('_',' ')}${s.safetySignalLastError?' ('+s.safetySignalLastError+')':''}`;
    $('safetyAt').textContent=fmt(s.safetySignalAt);$('safetyAckAt').textContent=fmt(s.safetySignalAckAt);
    $('heartbeatHttp').textContent=s.safetySignalHttpStatus||s.lastHeartbeatHttpStatus||'—';
    $('lastDelivery').textContent=fmt(s.lastDeliveryAt);$('http').textContent=s.lastHttpStatus??'—';
    $('appearances').textContent=s.cardAppearances||0;
    $('cardChanges').textContent=s.cardChanges||0;
    $('provisional').textContent=s.cardProvisionalMissing||0;
    $('confirmed').textContent=s.cardConfirmedMissing||0;
    $('reappearances').textContent=s.cardReappearances||0;
    $('mutationAt').textContent=fmt(s.lastMutationAt);
    $('scanComplete').textContent=s.lastScanComplete?'Yes':'No — '+(s.lastScanReason||'coverage unknown');
    $('auditPending').textContent=s.auditPending||0;
    $('auditOverflow').textContent=s.auditOverflowCount||0;
    $('result').textContent=s.lastResult||'';
    $('start').disabled=!!(s.enabled&&!s.paused)||!s.configured;
    $('stop').disabled=!s.enabled;
  }
  async function act(fn){try{await fn();await refresh();}catch(e){notice(e?.message||'Action failed',true);await refresh().catch(()=>{});}}
  $('save').addEventListener('click',()=>act(async()=>{
    const key=$('deviceToken').value.trim();
    const r=await send({type:'DETECTOR_CONFIGURE',deviceToken:key});
    $('deviceToken').value='';notice(`Connected as ${r.detectorId}; Oracle assigned its UK schedule.`);
  }));
  $('start').addEventListener('click',()=>act(async()=>{
    const tabs=await chrome.tabs.query({active:true,currentWindow:true});
    if(!tabs[0])throw Error('Open the public Amazon Jobs listings tab first.');
    await send({type:'DETECTOR_START',tabId:tabs[0].id});notice('Passive observation started. No scheduled webpage refreshes.');
  }));
  $('stop').addEventListener('click',()=>act(async()=>{await send({type:'DETECTOR_STOP'});notice('Monitoring stopped.');}));
  $('extensionVersion').textContent='v'+chrome.runtime.getManifest().version;
  refresh().catch(e=>notice('Cannot read detector status: '+e.message,true));
})();
