import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import {webcrypto} from 'node:crypto';
const require=createRequire(import.meta.url);
const C=require('./src/core.js');

async function fixture(opts={}){
  let listener;
  const tracker={reloads:[],messages:[],cleared:[],created:[]};
  const profile={schema_version:2,detector_id:'detector-01',source:'oracle_key_profile',revision:1,
    server_time_utc:new Date().toISOString(),schedule:{mode:'fixed_10_seconds',timezone:'Europe/London',
    interval_seconds:10,seconds_of_minute:[1,11,21,31,41,51],missed_slot_policy:'skip'}};
  const obj={
    config:{profile,profileFetchedAt:Date.now(),previousDetectorId:'detector-01'},
    run:{enabled:false,paused:false,targetTabId:null,status:'stopped',refreshCount:0},
    db:{nextSequence:1793805505994,queue:[],cache:{},auditQueue:[],scanSummaries:[],presence:{}}
  };
  const store={detectorV2Store:obj,detectorV2Token:'detector-test-token-123456'};
  const tab={id:17,url:opts.url||'https://www.amazon.jobs/en/search?city=London',status:opts.loading?'loading':'complete'};
  const chrome={
    storage:{local:{
      async get(keys){if(Array.isArray(keys))return Object.fromEntries(keys.map(k=>[k,store[k]]));
        return {[keys]:structuredClone(store[keys])};},
      async set(v){for(const [k,val] of Object.entries(v))store[k]=structuredClone(val);},
      async setAccessLevel(){},
    }},
    alarms:{async clear(s){tracker.cleared.push(s);return true;},async create(s,o){tracker.created.push([s,o]);},
      onAlarm:{addListener(){}}},
    tabs:{
      async get(id){assert.equal(id,17);return {...tab};},
      async sendMessage(id,payload){assert.equal(id,17);tracker.messages.push(payload.type);
        if(payload.type==='DETECTOR_SAFETY_CHECK'){
          if(opts.missingContent)throw Error('No receiving end');
          if(opts.safe===false)return {safe:false,reason:'captcha'};
          return opts.safetyUnknown?{}:{safe:true,reason:null};
        }
        return {ok:true};},
      async reload(id){tracker.reloads.push(id);if(opts.reloadError)throw Error('reload failed');},
      onRemoved:{addListener(){}},onUpdated:{addListener(){}}
    },
    runtime:{onMessage:{addListener(fn){listener=fn;}},onInstalled:{addListener(){}},onStartup:{addListener(){}}}
  };
  const globals={console,chrome,crypto:webcrypto,URL,TextEncoder,AbortController,Date,Math,JSON,Set,
    setTimeout,clearTimeout,fetch:async()=>({ok:true,status:200}),
    importScripts(name){if(name==='core.js')globals.AmazonDetectorCore=C;
      else if(name==='availability.js')globals.AmazonDetectorAvailability={};
      else if(name==='presence_transport.js')globals.AmazonDetectorPresenceTransport={};}}
  globals.globalThis=globals;
  vm.runInNewContext(fs.readFileSync('./src/background.js','utf8'),globals,{filename:'background.js'});
  assert.equal(typeof listener,'function');
  async function dispatch(message,sender={}){
    return await new Promise((resolve,reject)=>{
      const returned=listener(message,sender,r=>resolve(r));
      if(!returned)reject(Error('Listener rejected message'));
    });
  }
  const status=()=>structuredClone(store.detectorV2Store.run);
  return {dispatch,status,tracker,tab};
}

// Passive start must never invoke a tab reload or schedule refreshes.
{
  const f=await fixture();
  const result=await f.dispatch({type:'DETECTOR_START',tabId:17});
  assert.equal(result.ok,true);
  assert.equal(result.mode,'passive_dom_observation');
  assert.deepEqual(f.tracker.reloads,[]);
  assert.deepEqual(f.tracker.messages.slice(0,2),['DETECTOR_SAFETY_CHECK','DETECTOR_BEGIN']);
  assert.equal(f.status().pageState,'observing');
  assert.equal(f.status().refreshPending,null);
  assert.equal(f.status().nextRefreshAt,null);
  const ready=await f.dispatch({type:'DETECTOR_PAGE_READY'}, {tab:{id:17,url:f.tab.url}});
  assert.equal(ready.active,true);
  assert.equal(f.status().pageState,'observing');
  assert.equal(f.status().refreshCount,0);
  // Legacy refresh messages are not accepted by the background listener.
  assert.deepEqual(f.tracker.reloads,[]);
}
for(const [opt,explanation] of [
  [{safe:false},/security verification/],
  [{missingContent:true},/Content observer unavailable/],
  [{safetyUnknown:true},/Cannot verify/],
  [{loading:true},/finishes loading/],
  [{url:'https://www.amazon.jobs/en/apply/'},/application page/]
]){
  const f=await fixture(opt);
  const response=await f.dispatch({type:'DETECTOR_START',tabId:17});
  assert.match(response.error,explanation);
  assert.equal(f.tracker.reloads.length,0);
  assert.equal(f.status().enabled,false);
}
const source=fs.readFileSync('./src/background.js','utf8');
const content=fs.readFileSync('./src/content.js','utf8');
assert.doesNotMatch(source,/chrome\.tabs\.reload\(/);
assert.doesNotMatch(content,/DETECTOR_REFRESH_DUE|nextRefreshSlot|refreshTimer/);
assert.match(content,/MutationObserver\(observeMutations\)/);
assert.equal(JSON.parse(fs.readFileSync('./manifest.json')).version,'0.6.1');
console.log('PASS: Passive startup, absent refresh machinery, safety checks and legacy message rejection');
