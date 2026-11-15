/* Stage5E pure evidence encoder. It never changes job IDs or normal observations. */
(function(scope){
  'use strict';
  function batch(detectorId, audit=[], scans=[], maxBytes=110000){
    if(!/^detector-(?:0[1-9]|10)$/.test(detectorId))throw Error('Missing authenticated detector');
    const events=audit.slice(0,8).map(e=>{
      const allowed=['event_id','detector_id','event_type','observed_at','scope','job_key','same_content','job','source','content_fingerprint','previous_content_fingerprint','changed_fields'];
      const out={};for(const k of allowed)if(e[k]!==undefined)out[k]=e[k];
      if(out.detector_id!==detectorId||!out.event_id||!out.scope||!out.job_key)throw Error('Invalid evidence event');
      return out;
    });
    const selected=scans.filter(s=>s.scan_id).slice(0,3);
    const batchScans=selected.map(s=>({
      detector_id:detectorId,scan_id:s.scan_id,scope:s.scope,observed_at:s.observed_at,
      complete:s.complete===true,reason:String(s.reason||'').slice(0,120),
      identities:s.identities||[]
    }));
    let payload={schema_version:1,events,scans:batchScans};
    // Stable, bounded batches; do not truncate the contents of a record.
    while(new TextEncoder().encode(JSON.stringify(payload)).length>maxBytes){
      if(payload.events.length>=payload.scans.length&&payload.events.length>1)payload.events.pop();
      else if(payload.scans.length>1)payload.scans.pop();
      else throw Error('Single evidence record too large');
    }
    if(!payload.events.length&&!payload.scans.length)return null;
    return {payload, eventIds:payload.events.map(e=>e.event_id),scanIds:payload.scans.map(s=>s.scan_id)};
  }
  const api={batch};scope.AmazonDetectorPresenceTransport=api;
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof globalThis!=='undefined'?globalThis:this);
