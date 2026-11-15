/* Read-only public-page security challenge detection. No network, clicking, or solving. */
(function(scope){
  'use strict';
  const CAPTCHA_TEXT=[
    /verify\s+(?:that\s+)?you\s+are\s+(?:a\s+)?human/i,
    /(?:i\s*['’]?m|i\s+am)\s+not\s+a\s+robot/i,
    /(?:captcha|recaptcha|hcaptcha)\s*(?:challenge|verification|validation|is\s+required)/i,
    /(?:complete|solve|enter|type)\s+(?:the\s+)?(?:captcha|security\s+challenge|characters\s+you\s+see)/i,
    /(?:type|enter)\s+the\s+(?:letters|characters)\s+(?:shown|in\s+the\s+(?:image|picture))/i,
    /robot\s+check|human\s+verification|security\s+verification/i,
    /press\s+and\s+hold\s+(?:to\s+)?(?:verify|confirm)/i,
    /please\s+(?:verify|confirm)\s+you(?:'re|\s+are)\s+(?:not\s+a\s+robot|human)/i,
    /we\s+(?:just\s+)?need\s+to\s+(?:verify|make\s+sure)\s+(?:you\s+are|you're)\s+(?:not\s+a\s+robot|human)/i,
    /sorry[,\s]+we\s+just\s+need\s+to\s+make\s+sure\s+you/i
  ];
  const ACCESS_TEXT=[
    /access\s+denied|your\s+(?:access|request)\s+(?:has\s+been\s+)?blocked/i,
    /too\s+many\s+requests|rate\s+limit\s+exceeded|unusual\s+(?:traffic|activity)/i,
    /temporarily\s+blocked|request\s+blocked|you\s+have\s+been\s+blocked/i,
    /(?:http\s*)?429\s*[:—-]?\s*too\s+many\s+requests/i,
    /(?:http\s*)?403\s*[:—-]?\s*forbidden/i
  ];
  const CHALLENGE_NODES='awswaf-captcha, h-captcha, iframe[src*="captcha" i], iframe[src*="recaptcha" i], iframe[src*="hcaptcha" i], iframe[src*="challenge" i], iframe[title*="captcha" i], iframe[title*="security challenge" i], iframe[title*="verification challenge" i], [data-testid*="captcha" i], [id="captcha" i], [id*="captcha-challenge" i], [id*="captcha-container" i], input[name="captcha" i], input[id="captcha" i]';
  function visible(node,doc){
    if(!node || node.hidden || node.getAttribute?.('aria-hidden')==='true' || node.closest?.('[hidden],[aria-hidden="true"]'))return false;
    const win=doc?.defaultView;
    const style=typeof win?.getComputedStyle==='function'?win.getComputedStyle(node):null;
    if(style && (style.display==='none'||style.visibility==='hidden'||style.opacity==='0'))return false;
    if(typeof node.getBoundingClientRect==='function'){
      const r=node.getBoundingClientRect();
      if(r.width<2 || r.height<2)return false;
    }
    return true;
  }
  function inspect(doc,href=''){
    if(!doc)return null;
    let path='';
    try{const url=new URL(href);path=url.pathname+url.hash;}catch(_){/* invalid URL */}
    if(/(?:^|[\/#])(?:captcha|robot-check|verify-human|human-verification)(?:[\/?#-]|$)/i.test(path))return 'captcha';
    if(/(?:^|[\/#])(?:access-denied|rate-limited)(?:[\/?#-]|$)/i.test(path))return 'access_restricted';
    const title=String(doc.title||'');
    if(/captcha|robot\s*check|verify\s+you(?:\s+are|'re)\s+human/i.test(title))return 'captcha';
    if(/access\s+denied|too\s+many\s+requests/i.test(title))return 'access_restricted';
    for(const node of (doc.querySelectorAll?.(CHALLENGE_NODES)||[])){
      if(visible(node,doc))return 'captcha';
    }
    // Using the full rendered text fixes missed challenges following lengthy job lists.
    // textContent is intentionally not used: it would include hidden templates/scripts.
    const body=String(doc.body?.innerText||'');
    if(CAPTCHA_TEXT.some(pattern=>pattern.test(body)))return 'captcha';
    if(ACCESS_TEXT.some(pattern=>pattern.test(body)))return 'access_restricted';
    return null;
  }
  const api={inspect};
  scope.AmazonDetectorSafety=api;
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
})(typeof globalThis!=='undefined'?globalThis:this);
